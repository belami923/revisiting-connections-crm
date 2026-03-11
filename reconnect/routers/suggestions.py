"""Routes for the dashboard (suggestions) and feedback."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from reconnect.database import get_connection
from reconnect.scoring.feedback import submit_feedback
from reconnect.scoring.scorer import recalculate_all_scores
from reconnect.scoring.suggester import generate_suggestions, get_replacement_candidate

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# List names for the reach-out workflow
_REACHING_OUT_LIST = "Reaching Out"
_REACHED_OUT_LIST = "Reached Out"


def _ensure_list(conn: sqlite3.Connection, name: str) -> int:
    """Get or create a system list by name, return its id."""
    row = conn.execute(
        "SELECT id FROM custom_lists WHERE name = ?", (name,)
    ).fetchone()
    if row:
        return row["id"]
    cursor = conn.execute(
        "INSERT INTO custom_lists (name, description) VALUES (?, ?)",
        (name, f"Auto-managed by the dashboard reach-out workflow."),
    )
    conn.commit()
    return cursor.lastrowid


def _add_to_list(conn: sqlite3.Connection, list_name: str, contact_id: int):
    """Add a contact to a named list (idempotent)."""
    list_id = _ensure_list(conn, list_name)
    conn.execute(
        "INSERT OR IGNORE INTO list_memberships (list_id, contact_id) VALUES (?, ?)",
        (list_id, contact_id),
    )


def _remove_from_list(conn: sqlite3.Connection, list_name: str, contact_id: int):
    """Remove a contact from a named list (safe if not present)."""
    row = conn.execute(
        "SELECT id FROM custom_lists WHERE name = ?", (list_name,)
    ).fetchone()
    if row:
        conn.execute(
            "DELETE FROM list_memberships WHERE list_id = ? AND contact_id = ?",
            (row["id"], contact_id),
        )


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    """Main dashboard showing current month's suggestions."""
    conn = get_connection()
    try:
        month_label = date.today().strftime("%Y-%m")

        # Get current batch
        batch = conn.execute(
            "SELECT * FROM suggestion_batches WHERE month_label = ? ORDER BY id DESC LIMIT 1",
            (month_label,),
        ).fetchone()

        suggestions = []
        batch_info = None
        if batch:
            batch_info = dict(batch)
            rows = conn.execute(
                """
                SELECT s.*, c.display_name, c.first_name, c.last_name,
                       c.linkedin_url, c.twitter_url,
                       cs.total_interactions, cs.last_interaction_at, cs.days_since_last
                FROM suggestions s
                JOIN contacts c ON s.contact_id = c.id
                LEFT JOIN contact_scores cs ON s.contact_id = cs.contact_id
                WHERE s.batch_id = ?
                  AND (s.feedback IS NULL OR s.feedback != 'no')
                ORDER BY s.rank
                """,
                (batch["id"],),
            ).fetchall()

            for row in rows:
                s = dict(row)
                # Parse all narratives
                if s.get("all_narratives_json"):
                    s["all_narratives"] = json.loads(s["all_narratives_json"])
                else:
                    s["all_narratives"] = []

                # Parse enrichment data (conversation context)
                if s.get("enrichment_json"):
                    try:
                        s["enrichment"] = json.loads(s["enrichment_json"])
                    except (json.JSONDecodeError, TypeError):
                        s["enrichment"] = {}
                else:
                    s["enrichment"] = {}

                # Get contact identifiers for display
                identifiers = conn.execute(
                    "SELECT identifier_type, identifier_value FROM contact_identifiers WHERE contact_id = ?",
                    (s["contact_id"],),
                ).fetchall()
                s["identifiers"] = [dict(i) for i in identifiers]

                suggestions.append(s)

        # Count feedback stats
        total = len(suggestions)
        responded = sum(1 for s in suggestions if s.get("feedback"))
        yes_count = sum(1 for s in suggestions if s.get("feedback") == "yes" and not s.get("reached_out_at"))
        done_count = sum(1 for s in suggestions if s.get("reached_out_at"))
        no_count = sum(1 for s in suggestions if s.get("feedback") == "no")

        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "month_label": month_label,
                "batch": batch_info,
                "suggestions": suggestions,
                "total": total,
                "responded": responded,
                "yes_count": yes_count,
                "done_count": done_count,
                "no_count": no_count,
            },
        )
    finally:
        conn.close()


@router.post("/suggestions/{suggestion_id}/feedback")
def post_feedback(suggestion_id: int, feedback: str = Form(...)):
    """Submit feedback on a suggestion."""
    result = submit_feedback(suggestion_id, feedback)
    # Add to "Reaching Out" list when user says yes
    if feedback == "yes" and result.get("contact_id"):
        conn = get_connection()
        try:
            _add_to_list(conn, _REACHING_OUT_LIST, result["contact_id"])
            conn.commit()
        finally:
            conn.close()
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/{suggestion_id}/undo")
def undo_feedback(suggestion_id: int):
    """Clear feedback and reached_out_at on a suggestion, remove from lists."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT contact_id FROM suggestions WHERE id = ?", (suggestion_id,)
        ).fetchone()
        conn.execute(
            "UPDATE suggestions SET feedback = NULL, reached_out_at = NULL WHERE id = ?",
            (suggestion_id,),
        )
        if row:
            _remove_from_list(conn, _REACHING_OUT_LIST, row["contact_id"])
            _remove_from_list(conn, _REACHED_OUT_LIST, row["contact_id"])
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/{suggestion_id}/skip")
def skip_suggestion(
    suggestion_id: int,
    skip_type: str = Form(...),
    skip_months: int = Form(0),
):
    """Skip a suggestion: remove from dashboard and replace with someone new."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, batch_id, contact_id FROM suggestions WHERE id = ?",
            (suggestion_id,),
        ).fetchone()
        if not row:
            return RedirectResponse(url="/", status_code=303)

        contact_id = row["contact_id"]
        batch_id = row["batch_id"]

        # Apply skip/exclusion to the contact
        if skip_type == "never":
            conn.execute(
                "UPDATE contacts SET is_excluded = 1, updated_at = datetime('now') WHERE id = ?",
                (contact_id,),
            )
        elif skip_type in ("3", "6", "12"):
            months = int(skip_type)
            skip_until = (date.today() + timedelta(days=months * 30)).isoformat()
            conn.execute(
                "UPDATE contacts SET skip_until = ?, updated_at = datetime('now') WHERE id = ?",
                (skip_until, contact_id),
            )
        elif skip_type == "custom" and skip_months > 0:
            skip_until = (date.today() + timedelta(days=skip_months * 30)).isoformat()
            conn.execute(
                "UPDATE contacts SET skip_until = ?, updated_at = datetime('now') WHERE id = ?",
                (skip_until, contact_id),
            )

        # Delete the skipped suggestion from the dashboard
        conn.execute("DELETE FROM suggestions WHERE id = ?", (suggestion_id,))

        # Gather IDs still in batch (to exclude from replacement search)
        remaining = conn.execute(
            "SELECT contact_id FROM suggestions WHERE batch_id = ?", (batch_id,)
        ).fetchall()
        exclude_ids = [r["contact_id"] for r in remaining] + [contact_id]

        # Get next rank number
        max_rank_row = conn.execute(
            "SELECT COALESCE(MAX(rank), 0) as mr FROM suggestions WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        next_rank = max_rank_row["mr"] + 1

        # Find and add a replacement
        replacement = get_replacement_candidate(conn, exclude_ids)
        if replacement:
            conn.execute(
                """
                INSERT INTO suggestions
                    (batch_id, contact_id, rank, score_at_time,
                     primary_rule_id, primary_narrative, all_narratives_json,
                     enrichment_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    replacement["contact_id"],
                    next_rank,
                    replacement["score"],
                    replacement["primary"]["rule_id"] if replacement["primary"] else None,
                    replacement["primary"]["narrative"] if replacement["primary"] else None,
                    json.dumps(replacement["narratives"]) if replacement["narratives"] else None,
                    json.dumps(replacement["enrichment"]) if replacement["enrichment"] else None,
                ),
            )

        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/{suggestion_id}/reached-out")
def mark_reached_out(suggestion_id: int):
    """Mark that the user actually reached out to this contact."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT contact_id FROM suggestions WHERE id = ?", (suggestion_id,)
        ).fetchone()
        conn.execute(
            "UPDATE suggestions SET reached_out_at = datetime('now') WHERE id = ?",
            (suggestion_id,),
        )
        if row:
            _remove_from_list(conn, _REACHING_OUT_LIST, row["contact_id"])
            _add_to_list(conn, _REACHED_OUT_LIST, row["contact_id"])
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/{suggestion_id}/undo-reached-out")
def undo_reached_out(suggestion_id: int):
    """Undo marking that the user reached out — move back to Reaching Out."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT contact_id FROM suggestions WHERE id = ?", (suggestion_id,)
        ).fetchone()
        conn.execute(
            "UPDATE suggestions SET reached_out_at = NULL WHERE id = ?",
            (suggestion_id,),
        )
        if row:
            _remove_from_list(conn, _REACHED_OUT_LIST, row["contact_id"])
            _add_to_list(conn, _REACHING_OUT_LIST, row["contact_id"])
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/{suggestion_id}/notes")
def save_notes(suggestion_id: int, notes: str = Form("")):
    """Save a note/comment on a suggestion."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE suggestions SET notes = ? WHERE id = ?",
            (notes.strip(), suggestion_id),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/generate")
def trigger_generate():
    """Manually trigger suggestion generation for current month."""
    recalculate_all_scores()
    month_label = date.today().strftime("%Y-%m")
    generate_suggestions(month_label)
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/regenerate")
def trigger_regenerate():
    """Delete ALL batches for this month and regenerate fresh.

    Skips full score recalculation — uses existing scores to stay fast.
    Scores are recalculated during Sync instead.
    """
    conn = get_connection()
    try:
        month_label = date.today().strftime("%Y-%m")
        batches = conn.execute(
            "SELECT id FROM suggestion_batches WHERE month_label = ?",
            (month_label,),
        ).fetchall()
        for batch in batches:
            conn.execute("DELETE FROM suggestions WHERE batch_id = ?", (batch["id"],))
            conn.execute("DELETE FROM suggestion_batches WHERE id = ?", (batch["id"],))
        conn.commit()
    finally:
        conn.close()

    month_label = date.today().strftime("%Y-%m")
    generate_suggestions(month_label)
    return RedirectResponse(url="/", status_code=303)
