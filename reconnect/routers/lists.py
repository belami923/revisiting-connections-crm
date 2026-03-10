"""Routes for custom contact lists."""

from __future__ import annotations

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from reconnect.database import get_connection

router = APIRouter(prefix="/lists")
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
def lists_index(request: Request):
    """Show all custom lists with member counts."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT cl.*,
                   COUNT(lm.id) as member_count
            FROM custom_lists cl
            LEFT JOIN list_memberships lm ON cl.id = lm.list_id
            GROUP BY cl.id
            ORDER BY cl.is_auto ASC, cl.name ASC
            """
        ).fetchall()
        lists = [dict(r) for r in rows]

        return templates.TemplateResponse(
            request,
            "custom_lists.html",
            {"lists": lists, "total": len(lists)},
        )
    finally:
        conn.close()


@router.get("/{list_id}", response_class=HTMLResponse)
def list_detail(request: Request, list_id: int):
    """Show a single list with its members."""
    conn = get_connection()
    try:
        lst = conn.execute(
            "SELECT * FROM custom_lists WHERE id = ?", (list_id,)
        ).fetchone()
        if not lst:
            return HTMLResponse("List not found", status_code=404)
        lst = dict(lst)

        # Get members with scores
        members = conn.execute(
            """
            SELECT c.id, c.display_name,
                   cs.total_interactions, cs.last_interaction_at,
                   cs.days_since_last, cs.suggestion_score,
                   lm.added_at
            FROM list_memberships lm
            JOIN contacts c ON lm.contact_id = c.id
            LEFT JOIN contact_scores cs ON c.id = cs.contact_id
            WHERE lm.list_id = ?
            ORDER BY c.display_name
            """,
            (list_id,),
        ).fetchall()
        lst["members"] = [dict(m) for m in members]

        # Get all contacts for the "add contact" dropdown
        all_contacts = conn.execute(
            """
            SELECT c.id, c.display_name
            FROM contacts c
            WHERE c.id NOT IN (
                SELECT contact_id FROM list_memberships WHERE list_id = ?
            )
            ORDER BY c.display_name
            """,
            (list_id,),
        ).fetchall()
        lst["available_contacts"] = [dict(c) for c in all_contacts]

        return templates.TemplateResponse(
            request,
            "custom_list_detail.html",
            {"list": lst},
        )
    finally:
        conn.close()


@router.post("/create", response_class=HTMLResponse)
def create_list(name: str = Form(...), description: str = Form("")):
    """Create a new manual list."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO custom_lists (name, description) VALUES (?, ?)",
            (name.strip(), description.strip()),
        )
        conn.commit()
        return RedirectResponse(url="/lists/", status_code=303)
    finally:
        conn.close()


@router.post("/{list_id}/add", response_class=HTMLResponse)
def add_to_list(list_id: int, contact_id: int = Form(...)):
    """Add a contact to a list."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO list_memberships (list_id, contact_id) VALUES (?, ?)",
            (list_id, contact_id),
        )
        conn.commit()
        return RedirectResponse(url=f"/lists/{list_id}", status_code=303)
    finally:
        conn.close()


@router.post("/{list_id}/remove", response_class=HTMLResponse)
def remove_from_list(list_id: int, contact_id: int = Form(...)):
    """Remove a contact from a list."""
    conn = get_connection()
    try:
        conn.execute(
            "DELETE FROM list_memberships WHERE list_id = ? AND contact_id = ?",
            (list_id, contact_id),
        )
        conn.commit()
        return RedirectResponse(url=f"/lists/{list_id}", status_code=303)
    finally:
        conn.close()


@router.post("/{list_id}/delete", response_class=HTMLResponse)
def delete_list(list_id: int):
    """Delete a list and its memberships."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM custom_lists WHERE id = ?", (list_id,))
        conn.commit()
        return RedirectResponse(url="/lists/", status_code=303)
    finally:
        conn.close()


@router.post("/{list_id}/keep", response_class=HTMLResponse)
def keep_auto_list(list_id: int):
    """Convert an auto-suggested list to a manual (kept) list."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE custom_lists SET is_auto = 0 WHERE id = ?",
            (list_id,),
        )
        conn.commit()
        return RedirectResponse(url=f"/lists/{list_id}", status_code=303)
    finally:
        conn.close()


@router.post("/auto-generate", response_class=HTMLResponse)
def auto_generate(request: Request):
    """Run the auto-suggest engine to create lists from patterns."""
    from reconnect.scoring.list_suggestions import auto_generate_lists

    conn = get_connection()
    try:
        result = auto_generate_lists(conn)
        return RedirectResponse(url="/lists/", status_code=303)
    finally:
        conn.close()
