"""Social profile auto-lookup via DuckDuckGo search.

Finds LinkedIn and X.com profiles for contacts using their name and email.
Uses DuckDuckGo HTML search (no API key needed) with BeautifulSoup parsing.
Results are cached in the enrichment_cache table to avoid re-searching.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time

logger = logging.getLogger(__name__)

_THROTTLE_DELAY = 1.5  # seconds between searches


def _check_deps() -> bool:
    """Check if enrichment dependencies are installed."""
    try:
        import requests  # noqa: F401
        from bs4 import BeautifulSoup  # noqa: F401
        return True
    except ImportError:
        return False


def find_linkedin(name: str, email: str | None = None) -> str | None:
    """Search DuckDuckGo for a LinkedIn profile.

    Returns the first linkedin.com/in/ URL found, or None.
    """
    if not _check_deps():
        return None

    import requests
    from bs4 import BeautifulSoup

    query = f'"{name}" site:linkedin.com/in'
    if email:
        query += f" {email}"

    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
            timeout=10,
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.select("a.result__a"):
            href = link.get("href", "")
            # DuckDuckGo wraps URLs, extract the actual URL
            match = re.search(r"linkedin\.com/in/[\w\-]+", href)
            if match:
                return f"https://www.{match.group(0)}"

    except Exception as e:
        logger.warning("LinkedIn search failed for %s: %s", name, e)

    return None


def find_twitter(name: str, email: str | None = None) -> str | None:
    """Search DuckDuckGo for an X/Twitter profile.

    Returns the first x.com/ URL found, or None.
    """
    if not _check_deps():
        return None

    import requests
    from bs4 import BeautifulSoup

    query = f'"{name}" site:x.com'
    if email:
        query += f" {email}"

    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
            timeout=10,
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.select("a.result__a"):
            href = link.get("href", "")
            match = re.search(r"x\.com/(\w+)", href)
            if match and match.group(1).lower() not in ("home", "search", "login", "i"):
                return f"https://x.com/{match.group(1)}"

    except Exception as e:
        logger.warning("Twitter search failed for %s: %s", name, e)

    return None


def enrich_contact(contact_id: int, conn: sqlite3.Connection) -> dict:
    """Look up social profiles for a contact and save to DB.

    Returns dict with linkedin_url and twitter_url (may be None).
    """
    # Check cache first
    cached = conn.execute(
        "SELECT data_json FROM enrichment_cache WHERE contact_id = ? AND source = 'social'",
        (contact_id,),
    ).fetchone()

    if cached:
        return json.loads(cached["data_json"])

    # Get contact info
    contact = conn.execute(
        "SELECT display_name FROM contacts WHERE id = ?",
        (contact_id,),
    ).fetchone()

    if not contact:
        return {}

    name = contact["display_name"]

    # Get email if available
    email_row = conn.execute(
        "SELECT identifier_value FROM contact_identifiers WHERE contact_id = ? AND identifier_type = 'email' LIMIT 1",
        (contact_id,),
    ).fetchone()
    email = email_row["identifier_value"] if email_row else None

    result = {"linkedin_url": None, "twitter_url": None}

    # Search LinkedIn
    linkedin = find_linkedin(name, email)
    if linkedin:
        result["linkedin_url"] = linkedin
    time.sleep(_THROTTLE_DELAY)

    # Search Twitter/X
    twitter = find_twitter(name, email)
    if twitter:
        result["twitter_url"] = twitter

    # Cache result
    conn.execute(
        "INSERT OR REPLACE INTO enrichment_cache (contact_id, source, data_json, fetched_at) VALUES (?, 'social', ?, datetime('now'))",
        (contact_id, json.dumps(result)),
    )

    # Update contact record
    if result["linkedin_url"] or result["twitter_url"]:
        conn.execute(
            "UPDATE contacts SET linkedin_url = COALESCE(?, linkedin_url), twitter_url = COALESCE(?, twitter_url), updated_at = datetime('now') WHERE id = ?",
            (result["linkedin_url"], result["twitter_url"], contact_id),
        )

    conn.commit()
    return result


def enrich_batch(contact_ids: list[int], conn: sqlite3.Connection) -> dict:
    """Enrich multiple contacts with rate limiting.

    Returns summary of results.
    """
    results = {"enriched": 0, "skipped": 0, "errors": 0}

    for cid in contact_ids:
        try:
            result = enrich_contact(cid, conn)
            if result.get("linkedin_url") or result.get("twitter_url"):
                results["enriched"] += 1
            else:
                results["skipped"] += 1
            time.sleep(_THROTTLE_DELAY)
        except Exception as e:
            logger.warning("Enrichment failed for contact %d: %s", cid, e)
            results["errors"] += 1

    return results
