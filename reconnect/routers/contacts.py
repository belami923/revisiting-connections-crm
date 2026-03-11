"""Routes for contact browsing and detail views."""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from reconnect.database import get_connection
from reconnect.resolution.normalizer import normalize_email, normalize_phone

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/contacts")
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
def contacts_list(request: Request, q: str = ""):
    """List all contacts with search."""
    conn = get_connection()
    try:
        if q:
            rows = conn.execute(
                """
                SELECT c.*, cs.total_interactions, cs.last_interaction_at,
                       cs.days_since_last, cs.suggestion_score, cs.decay_score
                FROM contacts c
                LEFT JOIN contact_scores cs ON c.id = cs.contact_id
                WHERE c.display_name LIKE ?
                ORDER BY cs.suggestion_score DESC NULLS LAST
                LIMIT 200
                """,
                (f"%{q}%",),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT c.*, cs.total_interactions, cs.last_interaction_at,
                       cs.days_since_last, cs.suggestion_score, cs.decay_score
                FROM contacts c
                LEFT JOIN contact_scores cs ON c.id = cs.contact_id
                ORDER BY cs.total_interactions DESC NULLS LAST
                LIMIT 200
                """
            ).fetchall()

        contacts = [dict(row) for row in rows]

        return templates.TemplateResponse(
            request,
            "contacts_list.html",
            {
                "contacts": contacts,
                "query": q,
                "total": len(contacts),
            },
        )
    finally:
        conn.close()


@router.post("/create")
def create_contact(
    display_name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
):
    """Create a new contact manually."""
    conn = get_connection()
    try:
        # Split display_name into first/last
        parts = display_name.strip().split(None, 1)
        first_name = parts[0] if parts else display_name.strip()
        last_name = parts[1] if len(parts) > 1 else ""

        cursor = conn.execute(
            "INSERT INTO contacts (display_name, first_name, last_name) VALUES (?, ?, ?)",
            (display_name.strip(), first_name, last_name),
        )
        contact_id = cursor.lastrowid

        # Add phone identifier if provided
        if phone.strip():
            normalized = normalize_phone(phone.strip())
            if normalized:
                conn.execute(
                    "INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) VALUES (?, 'phone', ?, 'manual')",
                    (contact_id, normalized),
                )

        # Add email identifier if provided
        if email.strip():
            normalized = normalize_email(email.strip())
            if normalized:
                conn.execute(
                    "INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) VALUES (?, 'email', ?, 'manual')",
                    (contact_id, normalized),
                )

        conn.commit()
        logger.info("Created contact %d: %s", contact_id, display_name)
        return RedirectResponse(url=f"/contacts/{contact_id}", status_code=303)
    finally:
        conn.close()


@router.post("/{contact_id}/socials")
def update_socials(
    contact_id: int,
    linkedin_url: str = Form(""),
    twitter_url: str = Form(""),
):
    """Update social profile URLs for a contact."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE contacts SET linkedin_url = ?, twitter_url = ?, updated_at = datetime('now') WHERE id = ?",
            (linkedin_url.strip() or None, twitter_url.strip() or None, contact_id),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/contacts/{contact_id}", status_code=303)


@router.get("/never-show", response_class=HTMLResponse)
def never_show_list(request: Request):
    """List all contacts marked as never show."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT c.*, cs.total_interactions, cs.last_interaction_at
            FROM contacts c
            LEFT JOIN contact_scores cs ON c.id = cs.contact_id
            WHERE c.is_excluded = 1
            ORDER BY c.display_name
            """
        ).fetchall()
        contacts = [dict(row) for row in rows]
        return templates.TemplateResponse(
            request,
            "never_show.html",
            {"contacts": contacts, "total": len(contacts)},
        )
    finally:
        conn.close()


@router.post("/{contact_id}/restore")
def restore_contact(contact_id: int):
    """Remove a contact from the never-show list."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE contacts SET is_excluded = 0, skip_until = NULL, updated_at = datetime('now') WHERE id = ?",
            (contact_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/contacts/never-show", status_code=303)


@router.get("/{contact_id}", response_class=HTMLResponse)
def contact_detail(request: Request, contact_id: int):
    """Show detail view for a single contact."""
    conn = get_connection()
    try:
        # Get contact info
        contact = conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (contact_id,)
        ).fetchone()
        if not contact:
            return HTMLResponse("Contact not found", status_code=404)

        contact = dict(contact)

        # Get identifiers
        identifiers = conn.execute(
            "SELECT * FROM contact_identifiers WHERE contact_id = ?",
            (contact_id,),
        ).fetchall()
        contact["identifiers"] = [dict(i) for i in identifiers]

        # Get scores
        scores = conn.execute(
            "SELECT * FROM contact_scores WHERE contact_id = ?",
            (contact_id,),
        ).fetchone()
        contact["scores"] = dict(scores) if scores else None

        # Get pattern matches
        patterns = conn.execute(
            "SELECT * FROM pattern_matches WHERE contact_id = ? ORDER BY score_contribution DESC",
            (contact_id,),
        ).fetchall()
        contact["patterns"] = [dict(p) for p in patterns]

        # Get interaction timeline (last 100)
        interactions = conn.execute(
            """
            SELECT source, interaction_type, occurred_at, metadata_json
            FROM interactions
            WHERE contact_id = ?
            ORDER BY occurred_at DESC
            LIMIT 100
            """,
            (contact_id,),
        ).fetchall()
        contact["interactions"] = [dict(i) for i in interactions]

        # Get interaction stats by year
        yearly_stats = conn.execute(
            """
            SELECT strftime('%Y', occurred_at) as year,
                   COUNT(*) as count,
                   interaction_type
            FROM interactions
            WHERE contact_id = ?
            GROUP BY year, interaction_type
            ORDER BY year DESC
            """,
            (contact_id,),
        ).fetchall()
        contact["yearly_stats"] = [dict(s) for s in yearly_stats]

        # Get suggestion history
        suggestion_history = conn.execute(
            """
            SELECT s.*, sb.month_label
            FROM suggestions s
            JOIN suggestion_batches sb ON s.batch_id = sb.id
            WHERE s.contact_id = ?
            ORDER BY sb.month_label DESC
            """,
            (contact_id,),
        ).fetchall()
        contact["suggestion_history"] = [dict(s) for s in suggestion_history]

        # Get lists this contact belongs to
        contact_lists = conn.execute(
            """
            SELECT cl.id, cl.name
            FROM custom_lists cl
            JOIN list_memberships lm ON cl.id = lm.list_id
            WHERE lm.contact_id = ?
            ORDER BY cl.name
            """,
            (contact_id,),
        ).fetchall()
        contact["lists"] = [dict(l) for l in contact_lists]

        # Get lists this contact is NOT in (for the add dropdown)
        member_list_ids = [l["id"] for l in contact["lists"]]
        if member_list_ids:
            placeholders = ",".join("?" * len(member_list_ids))
            available = conn.execute(
                f"SELECT id, name FROM custom_lists WHERE id NOT IN ({placeholders}) ORDER BY name",
                member_list_ids,
            ).fetchall()
        else:
            available = conn.execute(
                "SELECT id, name FROM custom_lists ORDER BY name"
            ).fetchall()
        contact["available_lists"] = [dict(l) for l in available]

        return templates.TemplateResponse(
            request,
            "contact_detail.html",
            {"contact": contact},
        )
    finally:
        conn.close()
