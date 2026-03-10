"""Bootstrap contacts from Apple's AddressBook.

Reads contacts with names, phone numbers, and emails to pre-populate the
contacts table and serve as the anchor for entity resolution across
iMessage, Gmail, and Calendar.

Two approaches:
1. Direct SQLite read of AddressBook DB (requires Full Disk Access)
2. Contacts framework via PyObjC (triggers standard macOS permission dialog)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import subprocess

from reconnect.config import ADDRESSBOOK_DB_PATH
from reconnect.database import get_connection, get_readonly_connection
from reconnect.resolution.normalizer import normalize_email, normalize_phone

logger = logging.getLogger(__name__)


def bootstrap_contacts() -> dict:
    """Import contacts from Apple Contacts into the app database.

    Tries direct SQLite first, falls back to Contacts framework.
    Returns a summary dict with counts.
    """
    contacts_data = _read_via_applescript()
    if contacts_data is None:
        logger.info("AppleScript/JXA failed, trying direct SQLite...")
        contacts_data = _read_via_sqlite()
    if contacts_data is None:
        logger.info("SQLite approach failed, trying Contacts framework...")
        contacts_data = _read_via_contacts_framework()

    if contacts_data is None:
        return {
            "status": "error",
            "reason": "Could not access Apple Contacts. "
            "Grant Contacts permission when prompted, then retry.",
        }

    app_conn = get_connection()
    imported = 0
    updated = 0
    skipped = 0

    try:
        for contact_data in contacts_data:
            first = contact_data.get("first_name", "") or ""
            last = contact_data.get("last_name", "") or ""
            display = f"{first} {last}".strip()

            if not display:
                skipped += 1
                continue

            phones = contact_data.get("phones", [])
            emails = contact_data.get("emails", [])

            if not phones and not emails:
                skipped += 1
                continue

            # Check if this contact already exists (by matching any identifier)
            existing_id = _find_existing_contact(app_conn, phones, emails)

            if existing_id:
                # Update the display name if it's currently a phone/email
                _update_name_if_needed(
                    app_conn, existing_id, display, first, last
                )
                _add_identifiers(app_conn, existing_id, phones, emails)
                updated += 1
                continue

            # Create new contact
            cursor = app_conn.execute(
                "INSERT INTO contacts (display_name, first_name, last_name) "
                "VALUES (?, ?, ?)",
                (display, first or None, last or None),
            )
            contact_id = cursor.lastrowid
            _add_identifiers(app_conn, contact_id, phones, emails)
            imported += 1

        app_conn.commit()

    finally:
        app_conn.close()

    logger.info(
        "Apple Contacts bootstrap: imported=%d, updated=%d, skipped=%d",
        imported,
        updated,
        skipped,
    )
    return {
        "status": "ok",
        "imported": imported,
        "updated": updated,
        "skipped": skipped,
    }


# --- Reading contacts ---


def _read_via_sqlite() -> list[dict] | None:
    """Try reading contacts from the AddressBook SQLite database."""
    if not ADDRESSBOOK_DB_PATH.exists():
        logger.warning("AddressBook database not found at %s", ADDRESSBOOK_DB_PATH)
        return None

    try:
        ab_conn = get_readonly_connection(ADDRESSBOOK_DB_PATH)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        logger.warning("Cannot open AddressBook database: %s", e)
        return None

    try:
        return _read_addressbook_sqlite(ab_conn)
    finally:
        ab_conn.close()


def _read_addressbook_sqlite(conn: sqlite3.Connection) -> list[dict]:
    """Read contacts with phones and emails from AddressBook database."""
    contacts = {}

    try:
        rows = conn.execute(
            "SELECT Z_PK, ZFIRSTNAME, ZLASTNAME FROM ZABCDRECORD "
            "WHERE ZFIRSTNAME IS NOT NULL OR ZLASTNAME IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        logger.warning("Could not read ZABCDRECORD table.")
        return None

    for row in rows:
        pk = row[0]
        contacts[pk] = {
            "first_name": row[1],
            "last_name": row[2],
            "phones": [],
            "emails": [],
        }

    try:
        phones = conn.execute(
            "SELECT ZOWNER, ZFULLNUMBER FROM ZABCDPHONENUMBER "
            "WHERE ZFULLNUMBER IS NOT NULL"
        ).fetchall()
        for row in phones:
            owner = row[0]
            if owner in contacts:
                normalized = normalize_phone(row[1])
                if normalized:
                    contacts[owner]["phones"].append(normalized)
    except sqlite3.OperationalError:
        logger.warning("Could not read ZABCDPHONENUMBER table.")

    try:
        emails = conn.execute(
            "SELECT ZOWNER, ZADDRESS FROM ZABCDEMAILADDRESS "
            "WHERE ZADDRESS IS NOT NULL"
        ).fetchall()
        for row in emails:
            owner = row[0]
            if owner in contacts:
                normalized = normalize_email(row[1])
                if normalized:
                    contacts[owner]["emails"].append(normalized)
    except sqlite3.OperationalError:
        logger.warning("Could not read ZABCDEMAILADDRESS table.")

    return list(contacts.values())


def _read_via_contacts_framework() -> list[dict] | None:
    """Read contacts using Apple's Contacts framework (PyObjC).

    This triggers the standard macOS Contacts permission dialog.
    """
    try:
        import Contacts
    except ImportError:
        logger.warning(
            "pyobjc-framework-Contacts not installed. "
            "Run: pip install pyobjc-framework-Contacts"
        )
        return None

    store = Contacts.CNContactStore.alloc().init()

    # Check current authorization status
    auth_status = Contacts.CNContactStore.authorizationStatusForEntityType_(
        Contacts.CNEntityTypeContacts
    )
    # 0=NotDetermined, 1=Restricted, 2=Denied, 3=Authorized
    if auth_status == 3:
        logger.info("Contacts access already authorized")
    elif auth_status in (1, 2):
        logger.error(
            "Contacts access denied (status=%d). Grant permission in "
            "System Settings > Privacy & Security > Contacts, "
            "then add Terminal (or your Python binary).",
            auth_status,
        )
        return None
    else:
        # NotDetermined — try requesting (needs NSRunLoop for dialog)
        import threading
        import time

        access_granted = threading.Event()
        access_result = [False]

        def handler(granted, error):
            access_result[0] = granted
            if error:
                logger.warning("Contacts access request error: %s", error)
            access_granted.set()

        store.requestAccessForEntityType_completionHandler_(
            Contacts.CNEntityTypeContacts, handler
        )

        # Pump the run loop so the OS permission dialog can appear
        try:
            from Foundation import NSRunLoop, NSDate

            deadline = time.time() + 60
            while not access_granted.is_set() and time.time() < deadline:
                NSRunLoop.currentRunLoop().runUntilDate_(
                    NSDate.dateWithTimeIntervalSinceNow_(0.5)
                )
        except ImportError:
            access_granted.wait(timeout=60)

        if not access_result[0]:
            logger.error(
                "Contacts access denied. Grant permission in "
                "System Settings > Privacy & Security > Contacts"
            )
            return None

    # Fetch all contacts
    keys_to_fetch = [
        Contacts.CNContactGivenNameKey,
        Contacts.CNContactFamilyNameKey,
        Contacts.CNContactPhoneNumbersKey,
        Contacts.CNContactEmailAddressesKey,
    ]

    request = Contacts.CNContactFetchRequest.alloc().initWithKeysToFetch_(
        keys_to_fetch
    )

    contacts = []

    def process_contact(contact, stop):
        first = contact.givenName() or ""
        last = contact.familyName() or ""

        phones = []
        for pv in contact.phoneNumbers():
            raw = pv.value().stringValue()
            normalized = normalize_phone(raw)
            if normalized:
                phones.append(normalized)

        emails = []
        for ev in contact.emailAddresses():
            raw = ev.value()
            normalized = normalize_email(str(raw))
            if normalized:
                emails.append(normalized)

        contacts.append({
            "first_name": first,
            "last_name": last,
            "phones": phones,
            "emails": emails,
        })

    success, error = store.enumerateContactsWithFetchRequest_error_usingBlock_(
        request, None, process_contact
    )

    if not success:
        logger.error("Failed to enumerate contacts: %s", error)
        return None

    logger.info("Read %d contacts via Contacts framework", len(contacts))
    return contacts


def _read_via_applescript() -> list[dict] | None:
    """Read contacts using JavaScript for Automation (JXA) via osascript.

    This goes through the Contacts.app scripting bridge and triggers the
    macOS Automation permission dialog, which works reliably for CLI apps.
    Uses bulk property access for performance (~5s for 1800+ contacts).
    """
    jxa_script = """
var app = Application("Contacts");
var people = app.people;
var firstNames = people.firstName();
var lastNames = people.lastName();
var allPhones = people.phones.value();
var allEmails = people.emails.value();
var result = [];
for (var i = 0; i < firstNames.length; i++) {
    result.push({
        first_name: firstNames[i] || "",
        last_name: lastNames[i] || "",
        phones: allPhones[i] || [],
        emails: allEmails[i] || []
    });
}
JSON.stringify(result);
"""

    try:
        proc = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e", jxa_script],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        logger.warning("osascript not found — not on macOS?")
        return None
    except subprocess.TimeoutExpired:
        logger.warning("AppleScript/JXA timed out reading contacts")
        return None

    if proc.returncode != 0:
        logger.warning(
            "AppleScript/JXA failed (exit %d): %s",
            proc.returncode,
            proc.stderr.strip()[:200],
        )
        return None

    try:
        raw_contacts = json.loads(proc.stdout)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("Failed to parse JXA contacts output: %s", e)
        return None

    # Normalize phones and emails to match iMessage identifier format
    contacts = []
    for entry in raw_contacts:
        phones = []
        for raw_phone in entry.get("phones", []):
            normalized = normalize_phone(str(raw_phone))
            if normalized:
                phones.append(normalized)

        emails = []
        for raw_email in entry.get("emails", []):
            normalized = normalize_email(str(raw_email))
            if normalized:
                emails.append(normalized)

        contacts.append({
            "first_name": entry.get("first_name", ""),
            "last_name": entry.get("last_name", ""),
            "phones": phones,
            "emails": emails,
        })

    logger.info("Read %d contacts via AppleScript/JXA", len(contacts))
    return contacts


# --- Database helpers ---


def _find_existing_contact(
    conn: sqlite3.Connection,
    phones: list[str],
    emails: list[str],
) -> int | None:
    """Check if any identifier already exists and return the contact_id."""
    for phone in phones:
        row = conn.execute(
            "SELECT contact_id FROM contact_identifiers "
            "WHERE identifier_type = 'phone' AND identifier_value = ?",
            (phone,),
        ).fetchone()
        if row:
            return row[0]

    for email in emails:
        row = conn.execute(
            "SELECT contact_id FROM contact_identifiers "
            "WHERE identifier_type = 'email' AND identifier_value = ?",
            (email,),
        ).fetchone()
        if row:
            return row[0]

    return None


def _update_name_if_needed(
    conn: sqlite3.Connection,
    contact_id: int,
    display_name: str,
    first_name: str,
    last_name: str,
) -> None:
    """Update a contact's name if it's currently a phone number or email."""
    row = conn.execute(
        "SELECT display_name FROM contacts WHERE id = ?", (contact_id,)
    ).fetchone()
    if not row:
        return

    current = row["display_name"] or ""
    # If current name looks like a phone/email, upgrade it
    if "+" in current or "@" in current or current.isdigit():
        conn.execute(
            "UPDATE contacts SET display_name = ?, first_name = ?, "
            "last_name = ?, updated_at = datetime('now') WHERE id = ?",
            (display_name, first_name or None, last_name or None, contact_id),
        )


def _add_identifiers(
    conn: sqlite3.Connection,
    contact_id: int,
    phones: list[str],
    emails: list[str],
) -> None:
    """Add phone and email identifiers to a contact (ignoring duplicates)."""
    for phone in phones:
        conn.execute(
            "INSERT OR IGNORE INTO contact_identifiers "
            "(contact_id, identifier_type, identifier_value, source) "
            "VALUES (?, 'phone', ?, 'contacts_app')",
            (contact_id, phone),
        )

    for email in emails:
        conn.execute(
            "INSERT OR IGNORE INTO contact_identifiers "
            "(contact_id, identifier_type, identifier_value, source) "
            "VALUES (?, 'email', ?, 'contacts_app')",
            (contact_id, email),
        )
