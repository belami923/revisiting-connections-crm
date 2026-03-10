"""Ingest interaction data from the iMessage chat.db SQLite database.

Reads the local iMessage database at ~/Library/Messages/chat.db,
imports message metadata and text snippets as interactions, and
resolves handles to contacts.

Requires Full Disk Access for the running process.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime

from reconnect.config import IMESSAGE_DB_PATH
from reconnect.database import get_connection, get_readonly_connection
from reconnect.resolution.normalizer import classify_identifier

logger = logging.getLogger(__name__)

# Apple's epoch offset: seconds between Unix epoch and Apple epoch (Jan 1, 2001)
APPLE_EPOCH_OFFSET = 978307200
# macOS stores dates in nanoseconds since Apple epoch
NANOSECOND_DIVISOR = 1_000_000_000

# Max characters of message text to store
TEXT_SNIPPET_LENGTH = 150

IMESSAGE_QUERY = """
SELECT
    m.ROWID as message_id,
    m.date as raw_date,
    h.id as handle_identifier,
    h.service as service,
    m.is_from_me,
    m.cache_roomnames,
    m.text
FROM message m
LEFT JOIN handle h ON m.handle_id = h.ROWID
WHERE m.ROWID > ?
  AND h.id IS NOT NULL
ORDER BY m.ROWID ASC
"""

BATCH_SIZE = 5000


def ingest_imessage() -> dict:
    """Import iMessage interactions into the app database.

    Returns a summary dict with counts.
    """
    if not IMESSAGE_DB_PATH.exists():
        logger.warning(
            "iMessage database not found at %s", IMESSAGE_DB_PATH
        )
        return {"status": "skipped", "reason": "database not found"}

    try:
        im_conn = get_readonly_connection(IMESSAGE_DB_PATH)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        logger.error(
            "Cannot open iMessage database. "
            "Grant Full Disk Access to Terminal/Python. Error: %s",
            e,
        )
        return {"status": "error", "reason": str(e)}

    app_conn = get_connection()

    # Get the watermark (last imported ROWID)
    watermark_row = app_conn.execute(
        "SELECT watermark FROM ingestion_state WHERE source = 'imessage'"
    ).fetchone()
    watermark = int(watermark_row["watermark"]) if watermark_row else 0

    imported = 0
    skipped = 0
    group_skipped = 0
    unresolved = 0
    max_rowid = watermark

    try:
        cursor = im_conn.execute(IMESSAGE_QUERY, (watermark,))

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                message_id = row["message_id"]
                max_rowid = max(max_rowid, message_id)

                # Skip group chats in V1
                if row["cache_roomnames"]:
                    group_skipped += 1
                    continue

                handle = row["handle_identifier"]
                if not handle:
                    skipped += 1
                    continue

                # Classify and normalize the handle
                id_type, normalized = classify_identifier(handle)
                if not normalized:
                    skipped += 1
                    continue

                # Convert Apple timestamp to ISO 8601
                raw_date = row["raw_date"]
                if raw_date is None:
                    skipped += 1
                    continue

                try:
                    unix_ts = raw_date / NANOSECOND_DIVISOR + APPLE_EPOCH_OFFSET
                    occurred_at = datetime.fromtimestamp(unix_ts).isoformat()
                except (ValueError, OSError, OverflowError):
                    skipped += 1
                    continue

                # Resolve handle to contact
                contact_id = _resolve_handle(
                    app_conn, id_type, normalized, handle
                )
                if not contact_id:
                    unresolved += 1
                    continue

                is_from_me = row["is_from_me"]
                interaction_type = (
                    "message_sent" if is_from_me else "message_received"
                )

                # Build metadata with text snippet
                text = (row["text"] or "")[:TEXT_SNIPPET_LENGTH]
                metadata = {"service": row["service"]}
                if text.strip():
                    metadata["text"] = text

                # Insert interaction (ignore duplicates)
                try:
                    app_conn.execute(
                        "INSERT OR IGNORE INTO interactions "
                        "(contact_id, source, interaction_type, occurred_at, "
                        "source_id, metadata_json) "
                        "VALUES (?, 'imessage', ?, ?, ?, ?)",
                        (
                            contact_id,
                            interaction_type,
                            occurred_at,
                            f"imessage_{message_id}",
                            json.dumps(metadata),
                        ),
                    )
                    imported += 1
                except sqlite3.IntegrityError:
                    skipped += 1

            app_conn.commit()

        # Update watermark
        if max_rowid > watermark:
            app_conn.execute(
                "INSERT OR REPLACE INTO ingestion_state "
                "(source, last_synced_at, watermark, status) "
                "VALUES ('imessage', datetime('now'), ?, 'idle')",
                (str(max_rowid),),
            )
            app_conn.commit()

    finally:
        im_conn.close()
        app_conn.close()

    logger.info(
        "iMessage ingestion: imported=%d, skipped=%d, "
        "group_skipped=%d, unresolved=%d",
        imported,
        skipped,
        group_skipped,
        unresolved,
    )
    return {
        "status": "ok",
        "imported": imported,
        "skipped": skipped,
        "group_skipped": group_skipped,
        "unresolved": unresolved,
    }


def backfill_message_text() -> dict:
    """Backfill text snippets for existing iMessage interactions.

    Re-reads the iMessage database and updates metadata_json for
    interactions that are missing the 'text' field.
    """
    if not IMESSAGE_DB_PATH.exists():
        return {"status": "skipped", "reason": "database not found"}

    try:
        im_conn = get_readonly_connection(IMESSAGE_DB_PATH)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        return {"status": "error", "reason": str(e)}

    app_conn = get_connection()
    updated = 0

    try:
        # Get all iMessage interactions missing text
        rows = app_conn.execute(
            "SELECT id, source_id, metadata_json FROM interactions "
            "WHERE source = 'imessage' "
            "AND (metadata_json NOT LIKE '%\"text\"%' OR metadata_json IS NULL)"
        ).fetchall()

        if not rows:
            return {"status": "ok", "updated": 0, "message": "All messages already have text"}

        # Build a map of source_id -> app interaction id + metadata
        source_ids = {}
        for row in rows:
            sid = row["source_id"]
            # Extract the ROWID from source_id like "imessage_12345"
            if sid and sid.startswith("imessage_"):
                try:
                    rowid = int(sid.replace("imessage_", ""))
                    source_ids[rowid] = {
                        "app_id": row["id"],
                        "metadata": row["metadata_json"],
                    }
                except ValueError:
                    pass

        if not source_ids:
            return {"status": "ok", "updated": 0}

        # Batch-read texts from iMessage DB
        batch_size = 1000
        rowid_list = sorted(source_ids.keys())

        for i in range(0, len(rowid_list), batch_size):
            batch = rowid_list[i : i + batch_size]
            placeholders = ",".join("?" * len(batch))
            im_rows = im_conn.execute(
                f"SELECT ROWID, text FROM message WHERE ROWID IN ({placeholders})",
                batch,
            ).fetchall()

            for im_row in im_rows:
                rowid = im_row[0]
                text = (im_row[1] or "")[:TEXT_SNIPPET_LENGTH].strip()
                if not text:
                    continue

                info = source_ids[rowid]
                try:
                    metadata = json.loads(info["metadata"] or "{}")
                except (json.JSONDecodeError, TypeError):
                    metadata = {}

                metadata["text"] = text

                app_conn.execute(
                    "UPDATE interactions SET metadata_json = ? WHERE id = ?",
                    (json.dumps(metadata), info["app_id"]),
                )
                updated += 1

            app_conn.commit()

    finally:
        im_conn.close()
        app_conn.close()

    logger.info("Backfilled text for %d iMessage interactions", updated)
    return {"status": "ok", "updated": updated}


def _resolve_handle(
    conn: sqlite3.Connection,
    id_type: str,
    normalized: str,
    raw_handle: str,
) -> int | None:
    """Resolve an iMessage handle to a contact_id.

    If the identifier already exists, returns the existing contact_id.
    If not, creates a new contact with this identifier.
    """
    # Look up existing identifier
    row = conn.execute(
        "SELECT contact_id FROM contact_identifiers "
        "WHERE identifier_type = ? AND identifier_value = ?",
        (id_type, normalized),
    ).fetchone()

    if row:
        return row["contact_id"]

    # Create a new contact for this handle
    # Display name is the raw handle for now (will be enriched later)
    display_name = raw_handle
    cursor = conn.execute(
        "INSERT INTO contacts (display_name) VALUES (?)",
        (display_name,),
    )
    contact_id = cursor.lastrowid

    conn.execute(
        "INSERT OR IGNORE INTO contact_identifiers "
        "(contact_id, identifier_type, identifier_value, source) "
        "VALUES (?, ?, ?, 'imessage')",
        (contact_id, id_type, normalized),
    )

    return contact_id
