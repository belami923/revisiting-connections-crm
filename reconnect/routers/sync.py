"""Routes for data sync status and triggering syncs."""

from __future__ import annotations

import logging
import threading

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from reconnect.database import get_connection
from reconnect.ingestion.apple_contacts import bootstrap_contacts
from reconnect.ingestion.gmail import ingest_gmail, is_gmail_configured, setup_gmail
from reconnect.ingestion.imessage import ingest_imessage, backfill_message_text
from reconnect.resolution.resolver import resolve_duplicates
from reconnect.scoring.scorer import recalculate_all_scores
from reconnect.scoring.suggester import generate_suggestions, _build_enrichment

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sync")
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# Simple sync state tracking
_sync_lock = threading.Lock()
_sync_status = {"running": False, "message": ""}


def _run_sync_pipeline(steps: list[str]):
    """Run sync steps in a background thread."""
    global _sync_status
    try:
        _sync_status = {"running": True, "message": "Starting sync..."}

        if "contacts" in steps:
            _sync_status["message"] = "Importing Apple Contacts..."
            logger.info("Sync: importing Apple Contacts")
            bootstrap_contacts()

        if "imessage" in steps:
            _sync_status["message"] = "Syncing iMessage..."
            logger.info("Sync: importing iMessage")
            ingest_imessage()

        if "backfill_text" in steps:
            _sync_status["message"] = "Backfilling message text..."
            logger.info("Sync: backfilling iMessage text")
            backfill_message_text()

        if "gmail" in steps:
            _sync_status["message"] = "Syncing Gmail..."
            logger.info("Sync: importing Gmail")
            ingest_gmail()

        if "enrich_suggestions" in steps:
            _sync_status["message"] = "Enriching suggestions with social profiles..."
            logger.info("Sync: enriching suggestions")
            _enrich_current_suggestions()
            _sync_status = {"running": False, "message": "Enrichment complete!"}
            logger.info("Sync: enrichment complete")
            return

        _sync_status["message"] = "Resolving duplicates..."
        logger.info("Sync: resolving duplicates")
        resolve_duplicates()

        _sync_status["message"] = "Calculating scores..."
        logger.info("Sync: recalculating scores")
        recalculate_all_scores()

        _sync_status["message"] = "Generating suggestions..."
        logger.info("Sync: generating suggestions")
        generate_suggestions()

        _sync_status = {"running": False, "message": "Sync complete!"}
        logger.info("Sync: complete")
    except Exception as e:
        logger.exception("Sync failed: %s", e)
        _sync_status = {"running": False, "message": f"Sync error: {e}"}
    finally:
        _sync_lock.release()


def _enrich_current_suggestions():
    """Re-build enrichment (incl. social profiles) for all current suggestions."""
    import json
    conn = get_connection()
    try:
        rows = conn.execute("SELECT id, contact_id FROM suggestions").fetchall()
        for i, row in enumerate(rows):
            _sync_status["message"] = (
                f"Enriching suggestion {i + 1}/{len(rows)}..."
            )
            enrichment = _build_enrichment(conn, row["contact_id"])
            conn.execute(
                "UPDATE suggestions SET enrichment_json = ? WHERE id = ?",
                (json.dumps(enrichment), row["id"]),
            )
            conn.commit()
            logger.info(
                "Enriched suggestion %d (contact %d): %s",
                row["id"], row["contact_id"],
                list(enrichment.keys()),
            )
    finally:
        conn.close()


def _start_sync(steps: list[str]):
    """Start a sync in the background if one isn't already running."""
    if not _sync_lock.acquire(blocking=False):
        return False  # already running
    t = threading.Thread(target=_run_sync_pipeline, args=(steps,), daemon=True)
    t.start()
    return True


@router.get("/", response_class=HTMLResponse)
def sync_status(request: Request):
    """Show sync status for all data sources."""
    conn = get_connection()
    try:
        sources = conn.execute(
            "SELECT * FROM ingestion_state ORDER BY source"
        ).fetchall()
        sources = [dict(s) for s in sources]

        # Get counts
        contact_count = conn.execute("SELECT COUNT(*) as cnt FROM contacts").fetchone()["cnt"]
        interaction_count = conn.execute("SELECT COUNT(*) as cnt FROM interactions").fetchone()["cnt"]

        # Interaction counts by source
        source_counts = conn.execute(
            "SELECT source, COUNT(*) as cnt FROM interactions GROUP BY source"
        ).fetchall()
        source_counts = {row["source"]: row["cnt"] for row in source_counts}

        gmail_status = is_gmail_configured()

        return templates.TemplateResponse(
            request,
            "sync_status.html",
            {
                "sources": sources,
                "contact_count": contact_count,
                "interaction_count": interaction_count,
                "source_counts": source_counts,
                "gmail_status": gmail_status,
                "sync_running": _sync_status.get("running", False),
                "sync_message": _sync_status.get("message", ""),
            },
        )
    finally:
        conn.close()


@router.get("/status")
def sync_progress():
    """JSON endpoint for sync progress polling."""
    return JSONResponse(_sync_status)


@router.post("/imessage")
def trigger_imessage_sync():
    """Trigger iMessage sync."""
    _start_sync(["imessage"])
    return RedirectResponse(url="/sync", status_code=303)


@router.post("/contacts")
def trigger_contacts_sync():
    """Trigger Apple Contacts bootstrap."""
    _start_sync(["contacts"])
    return RedirectResponse(url="/sync", status_code=303)


@router.post("/gmail")
def trigger_gmail_sync():
    """Trigger Gmail sync."""
    _start_sync(["gmail"])
    return RedirectResponse(url="/sync", status_code=303)


@router.post("/gmail/setup")
def trigger_gmail_setup():
    """Run Gmail OAuth setup flow."""
    setup_gmail()
    return RedirectResponse(url="/sync", status_code=303)


@router.get("/gmail/status")
def gmail_auth_status():
    """JSON endpoint for Gmail auth status."""
    return JSONResponse(is_gmail_configured())


@router.post("/imessage/backfill")
def trigger_backfill():
    """Backfill message text for existing iMessage interactions."""
    _start_sync(["backfill_text"])
    return RedirectResponse(url="/sync", status_code=303)



@router.post("/enrich-suggestions")
def trigger_enrich_suggestions():
    """Enrich current suggestions with social profiles and updated conversation data."""
    _start_sync(["enrich_suggestions"])
    return RedirectResponse(url="/sync", status_code=303)


@router.post("/all")
def trigger_full_sync():
    """Trigger full sync pipeline."""
    _start_sync(["contacts", "imessage", "backfill_text", "gmail"])
    return RedirectResponse(url="/sync", status_code=303)
