"""Normalize identifiers (phone numbers, emails) for matching."""

from __future__ import annotations

import re

import phonenumbers

from reconnect.config import DEFAULT_PHONE_REGION


def normalize_phone(raw: str) -> str | None:
    """Normalize a phone number to E.164 format.

    Returns None if the input cannot be parsed as a valid phone number.

    Examples:
        normalize_phone("(415) 555-1234") -> "+14155551234"
        normalize_phone("+44 7911 123456") -> "+447911123456"
        normalize_phone("not a phone") -> None
    """
    if not raw or not raw.strip():
        return None

    raw = raw.strip()

    try:
        parsed = phonenumbers.parse(raw, DEFAULT_PHONE_REGION)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(
                parsed, phonenumbers.PhoneNumberFormat.E164
            )
    except phonenumbers.NumberParseException:
        pass

    return None


def normalize_email(raw: str) -> str | None:
    """Normalize an email address to lowercase, stripped.

    Returns None if the input doesn't look like an email.
    """
    if not raw or not raw.strip():
        return None

    raw = raw.strip().lower()

    # Basic email pattern check
    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", raw):
        return raw

    return None


def classify_identifier(raw: str) -> tuple[str, str | None]:
    """Classify and normalize a raw identifier string.

    Returns (identifier_type, normalized_value) or ("unknown", None).
    Tries phone first, then email.
    """
    # Try email first (emails can contain digits that look phone-ish)
    email = normalize_email(raw)
    if email:
        return ("email", email)

    # Try phone
    phone = normalize_phone(raw)
    if phone:
        return ("phone", phone)

    return ("unknown", None)


def normalize_name(raw: str) -> str:
    """Normalize a name for comparison (lowercase, strip extra whitespace)."""
    if not raw:
        return ""
    return " ".join(raw.strip().lower().split())


def split_name(full_name: str) -> tuple[str, str]:
    """Split a full name into (first_name, last_name).

    Simple heuristic: first token is first name, rest is last name.
    """
    parts = full_name.strip().split()
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))
