"""Ingest interaction data from Gmail via the Google Gmail API.

Uses OAuth 2.0 for authentication. The user needs to:
1. Create a Google Cloud project with Gmail API enabled
2. Download OAuth Desktop credentials as gmail_credentials.json
3. Place it in the data/ directory
4. Click "Connect Gmail" on the Sync page to authorize

Requires: pip install reconnect[gmail]
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from email.utils import parseaddr
from pathlib import Path

from reconnect.config import (
    GMAIL_BATCH_SIZE,
    GMAIL_CREDENTIALS_PATH,
    GMAIL_TOKEN_PATH,
)
from reconnect.database import get_connection
from reconnect.resolution.normalizer import classify_identifier

logger = logging.getLogger(__name__)

# We only need read access to Gmail
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# Courtesy throttle between individual message fetches (seconds)
_THROTTLE_DELAY = 0.05


def _check_deps() -> bool:
    """Check if Gmail API dependencies are installed."""
    try:
        import googleapiclient.discovery  # noqa: F401
        import google.auth  # noqa: F401
        return True
    except ImportError:
        return False


def is_gmail_configured() -> dict:
    """Check Gmail configuration status.

    Returns dict with:
        deps_installed: bool
        has_credentials: bool
        has_token: bool
    """
    return {
        "deps_installed": _check_deps(),
        "has_credentials": GMAIL_CREDENTIALS_PATH.exists(),
        "has_token": GMAIL_TOKEN_PATH.exists(),
    }


def _get_gmail_service():
    """Authenticate and return a Gmail API service object.

    On first run, opens a browser for OAuth authorization.
    On subsequent runs, uses the saved token (refreshing if expired).

    Returns:
        Gmail API service object.

    Raises:
        FileNotFoundError: If credentials.json is missing.
        ImportError: If Gmail dependencies aren't installed.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    if not GMAIL_CREDENTIALS_PATH.exists():
        raise FileNotFoundError(
            f"Gmail credentials not found at {GMAIL_CREDENTIALS_PATH}. "
            "Download OAuth Desktop credentials from Google Cloud Console."
        )

    creds = None

    # Load existing token
    if GMAIL_TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(
            str(GMAIL_TOKEN_PATH), SCOPES
        )

    # Refresh or obtain new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(GMAIL_CREDENTIALS_PATH), SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save token for next run
        GMAIL_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(GMAIL_TOKEN_PATH, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def setup_gmail() -> dict:
    """Run the OAuth setup flow.

    Opens a browser for the user to authorize Gmail access.

    Returns:
        Status dict.
    """
    if not _check_deps():
        return {
            "status": "error",
            "reason": "Gmail dependencies not installed. Run: pip install reconnect[gmail]",
        }

    if not GMAIL_CREDENTIALS_PATH.exists():
        return {
            "status": "error",
            "reason": f"Place gmail_credentials.json in {GMAIL_CREDENTIALS_PATH.parent}",
        }

    try:
        _get_gmail_service()
        return {"status": "ok", "message": "Gmail connected successfully"}
    except Exception as e:
        logger.error("Gmail setup failed: %s", e)
        return {"status": "error", "reason": str(e)}


def ingest_gmail() -> dict:
    """Import Gmail interactions into the app database.

    Returns a summary dict with counts.
    """
    if not _check_deps():
        logger.info("Gmail dependencies not installed, skipping")
        return {"status": "skipped", "reason": "gmail dependencies not installed"}

    status = is_gmail_configured()
    if not status["has_credentials"]:
        return {"status": "skipped", "reason": "credentials not configured"}
    if not status["has_token"]:
        return {"status": "skipped", "reason": "not authenticated — run setup first"}

    try:
        service = _get_gmail_service()
    except Exception as e:
        logger.error("Failed to connect to Gmail: %s", e)
        return {"status": "error", "reason": str(e)}

    app_conn = get_connection()

    # Get watermark (last historyId)
    watermark_row = app_conn.execute(
        "SELECT watermark FROM ingestion_state WHERE source = 'gmail'"
    ).fetchone()
    watermark = watermark_row["watermark"] if watermark_row else None

    try:
        if watermark:
            result = _incremental_sync(service, app_conn, watermark)
        else:
            result = _full_sync(service, app_conn)
    except Exception as e:
        logger.error("Gmail ingestion failed: %s", e)
        # Update status to error
        app_conn.execute(
            "INSERT OR REPLACE INTO ingestion_state "
            "(source, last_synced_at, watermark, status, error_message) "
            "VALUES ('gmail', datetime('now'), ?, 'error', ?)",
            (watermark or "", str(e)),
        )
        app_conn.commit()
        app_conn.close()
        return {"status": "error", "reason": str(e)}
    finally:
        if not app_conn._is_closed if hasattr(app_conn, '_is_closed') else False:
            pass  # connection managed in result handlers

    return result


def _full_sync(service, app_conn) -> dict:
    """Perform a full sync of all Gmail messages."""
    imported = 0
    skipped = 0
    unresolved = 0
    last_history_id = None
    page_token = None

    logger.info("Starting full Gmail sync")

    while True:
        # List message IDs (paginated)
        kwargs = {"userId": "me", "maxResults": 500}
        if page_token:
            kwargs["pageToken"] = page_token

        results = service.users().messages().list(**kwargs).execute()
        messages = results.get("messages", [])

        if not messages:
            break

        # Process each message
        for msg_stub in messages:
            msg_id = msg_stub["id"]

            counts = _process_message(service, app_conn, msg_id)
            imported += counts["imported"]
            skipped += counts["skipped"]
            unresolved += counts["unresolved"]

            # Track highest historyId
            if counts.get("history_id"):
                if not last_history_id or counts["history_id"] > last_history_id:
                    last_history_id = counts["history_id"]

            time.sleep(_THROTTLE_DELAY)

        # Commit after each page
        app_conn.commit()

        page_token = results.get("nextPageToken")
        if not page_token:
            break

        logger.info("Gmail sync progress: %d imported, %d skipped", imported, skipped)

    # Update watermark
    if last_history_id:
        app_conn.execute(
            "INSERT OR REPLACE INTO ingestion_state "
            "(source, last_synced_at, watermark, status) "
            "VALUES ('gmail', datetime('now'), ?, 'idle')",
            (str(last_history_id),),
        )
        app_conn.commit()

    app_conn.close()

    logger.info(
        "Gmail full sync complete: imported=%d, skipped=%d, unresolved=%d",
        imported, skipped, unresolved,
    )
    return {
        "status": "ok",
        "imported": imported,
        "skipped": skipped,
        "unresolved": unresolved,
    }


def _incremental_sync(service, app_conn, watermark: str) -> dict:
    """Perform incremental sync using Gmail history API."""
    imported = 0
    skipped = 0
    unresolved = 0
    last_history_id = watermark

    logger.info("Starting incremental Gmail sync from historyId=%s", watermark)

    try:
        page_token = None
        while True:
            kwargs = {
                "userId": "me",
                "startHistoryId": watermark,
                "historyTypes": ["messageAdded"],
            }
            if page_token:
                kwargs["pageToken"] = page_token

            results = service.users().history().list(**kwargs).execute()

            for record in results.get("history", []):
                for added in record.get("messagesAdded", []):
                    msg = added.get("message", {})
                    msg_id = msg.get("id")
                    if not msg_id:
                        continue

                    counts = _process_message(service, app_conn, msg_id)
                    imported += counts["imported"]
                    skipped += counts["skipped"]
                    unresolved += counts["unresolved"]

                    if counts.get("history_id"):
                        if counts["history_id"] > last_history_id:
                            last_history_id = counts["history_id"]

                    time.sleep(_THROTTLE_DELAY)

            app_conn.commit()

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        # Also grab the historyId from the response
        if results.get("historyId"):
            resp_history = results["historyId"]
            if resp_history > last_history_id:
                last_history_id = resp_history

    except Exception as e:
        error_str = str(e)
        if "404" in error_str or "notFound" in error_str:
            logger.warning(
                "Gmail historyId %s expired, falling back to full sync",
                watermark,
            )
            app_conn.close()
            app_conn = get_connection()
            return _full_sync(service, app_conn)
        raise

    # Update watermark
    app_conn.execute(
        "INSERT OR REPLACE INTO ingestion_state "
        "(source, last_synced_at, watermark, status) "
        "VALUES ('gmail', datetime('now'), ?, 'idle')",
        (str(last_history_id),),
    )
    app_conn.commit()
    app_conn.close()

    logger.info(
        "Gmail incremental sync complete: imported=%d, skipped=%d, unresolved=%d",
        imported, skipped, unresolved,
    )
    return {
        "status": "ok",
        "imported": imported,
        "skipped": skipped,
        "unresolved": unresolved,
    }


def _process_message(service, app_conn, msg_id: str) -> dict:
    """Fetch and process a single Gmail message.

    Returns dict with imported/skipped/unresolved counts and history_id.
    """
    result = {"imported": 0, "skipped": 0, "unresolved": 0, "history_id": None}

    try:
        msg = service.users().messages().get(
            userId="me",
            id=msg_id,
            format="metadata",
            metadataHeaders=["From", "To", "Subject"],
        ).execute()
    except Exception as e:
        logger.debug("Failed to fetch message %s: %s", msg_id, e)
        result["skipped"] = 1
        return result

    # Get history ID
    result["history_id"] = msg.get("historyId")

    # Determine sent vs received
    label_ids = msg.get("labelIds", [])
    headers = {
        h["name"]: h["value"]
        for h in msg.get("payload", {}).get("headers", [])
    }

    is_sent = "SENT" in label_ids
    interaction_type = "email_sent" if is_sent else "email_received"

    # Extract the relevant email address
    if is_sent:
        raw_addr = headers.get("To", "")
    else:
        raw_addr = headers.get("From", "")

    if not raw_addr:
        result["skipped"] = 1
        return result

    # Parse email from "Name <email>" format
    email_addr = _parse_email_address(raw_addr)
    if not email_addr:
        result["skipped"] = 1
        return result

    # Normalize via existing classifier
    id_type, normalized = classify_identifier(email_addr)
    if not normalized or id_type != "email":
        result["skipped"] = 1
        return result

    # Resolve to contact
    contact_id = _resolve_email(app_conn, normalized, email_addr)
    if not contact_id:
        result["unresolved"] = 1
        return result

    # Convert internalDate (epoch milliseconds) to ISO 8601
    internal_date = msg.get("internalDate")
    if not internal_date:
        result["skipped"] = 1
        return result

    try:
        unix_ts = int(internal_date) / 1000
        occurred_at = datetime.fromtimestamp(unix_ts).isoformat()
    except (ValueError, OSError, OverflowError):
        result["skipped"] = 1
        return result

    subject = headers.get("Subject", "")
    thread_id = msg.get("threadId", "")

    # Insert interaction
    try:
        app_conn.execute(
            "INSERT OR IGNORE INTO interactions "
            "(contact_id, source, interaction_type, occurred_at, "
            "source_id, metadata_json) "
            "VALUES (?, 'gmail', ?, ?, ?, ?)",
            (
                contact_id,
                interaction_type,
                occurred_at,
                f"gmail_{msg_id}",
                json.dumps({"subject": subject[:200], "thread_id": thread_id}),
            ),
        )
        result["imported"] = 1
    except Exception:
        result["skipped"] = 1

    return result


def _parse_email_address(header_value: str) -> str | None:
    """Extract email address from a header like 'Name <email>' or 'email'.

    Handles multiple recipients by taking the first one.
    """
    # Take first recipient if comma-separated
    first = header_value.split(",")[0].strip()
    _, addr = parseaddr(first)
    return addr if addr and "@" in addr else None


def _resolve_email(
    conn,
    normalized_email: str,
    raw_email: str,
) -> int | None:
    """Resolve an email address to a contact_id.

    If the email already exists in contact_identifiers, returns the
    existing contact_id. If not, creates a new contact.
    """
    row = conn.execute(
        "SELECT contact_id FROM contact_identifiers "
        "WHERE identifier_type = 'email' AND identifier_value = ?",
        (normalized_email,),
    ).fetchone()

    if row:
        return row["contact_id"]

    # Create a new contact using the email as display name
    # (will be enriched later by Apple Contacts bootstrap or manual edit)
    display_name = raw_email
    cursor = conn.execute(
        "INSERT INTO contacts (display_name) VALUES (?)",
        (display_name,),
    )
    contact_id = cursor.lastrowid

    conn.execute(
        "INSERT OR IGNORE INTO contact_identifiers "
        "(contact_id, identifier_type, identifier_value, source) "
        "VALUES (?, 'email', ?, 'gmail')",
        (contact_id, normalized_email),
    )

    return contact_id
