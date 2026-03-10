"""Routes for the dashboard (suggestions) and feedback."""

from __future__ import annotations

import json
from datetime import date

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from reconnect.database import get_connection
from reconnect.scoring.feedback import submit_feedback
from reconnect.scoring.scorer import recalculate_all_scores
from reconnect.scoring.suggester import generate_suggestions

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


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
        yes_count = sum(1 for s in suggestions if s.get("feedback") == "yes")
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
                "no_count": no_count,
            },
        )
    finally:
        conn.close()


@router.post("/suggestions/{suggestion_id}/feedback")
def post_feedback(suggestion_id: int, feedback: str = Form(...)):
    """Submit feedback on a suggestion."""
    result = submit_feedback(suggestion_id, feedback)
    return RedirectResponse(url="/", status_code=303)


@router.post("/suggestions/{suggestion_id}/undo")
def undo_feedback(suggestion_id: int):
    """Clear feedback on a suggestion."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE suggestions SET feedback = NULL WHERE id = ?",
            (suggestion_id,),
        )
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
    """Delete current batch and regenerate."""
    conn = get_connection()
    try:
        month_label = date.today().strftime("%Y-%m")
        batch = conn.execute(
            "SELECT id FROM suggestion_batches WHERE month_label = ?",
            (month_label,),
        ).fetchone()
        if batch:
            conn.execute("DELETE FROM suggestions WHERE batch_id = ?", (batch["id"],))
            conn.execute("DELETE FROM suggestion_batches WHERE id = ?", (batch["id"],))
            conn.commit()
    finally:
        conn.close()

    recalculate_all_scores()
    generate_suggestions(month_label)
    return RedirectResponse(url="/", status_code=303)
