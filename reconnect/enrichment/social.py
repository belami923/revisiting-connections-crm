"""Social profile auto-lookup via Brave Search.

Finds LinkedIn and X.com profiles for contacts using their name and email.
Uses Brave Search with BeautifulSoup parsing.
Results are cached in the enrichment_cache table to avoid re-searching.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time

logger = logging.getLogger(__name__)

_THROTTLE_DELAY = 2.0  # seconds between searches


def _check_deps() -> bool:
    """Check if enrichment dependencies are installed."""
    try:
        import requests  # noqa: F401
        from bs4 import BeautifulSoup  # noqa: F401
        return True
    except ImportError:
        return False


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _search_brave(query: str) -> list[str]:
    """Search Brave and return result URLs."""
    import requests
    from bs4 import BeautifulSoup

    try:
        resp = requests.get(
            "https://search.brave.com/search",
            params={"q": query},
            headers=_HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http"):
                urls.append(href)
        return urls
    except Exception as e:
        logger.debug("Brave search failed: %s", e)
        return []


def find_linkedin(name: str, email: str | None = None) -> str | None:
    """Search for a LinkedIn profile URL.

    Returns the first linkedin.com/in/ URL found, or None.
    """
    if not _check_deps():
        return None

    query = f'"{name}" site:linkedin.com/in'
    if email:
        query += f" {email}"

    urls = _search_brave(query)
    for url in urls:
        match = re.search(r"linkedin\.com/in/([\w\-]+)", url)
        if match:
            return f"https://www.linkedin.com/in/{match.group(1)}"

    return None


def find_twitter(name: str, email: str | None = None) -> str | None:
    """Search for an X/Twitter profile URL.

    Returns the first x.com/ URL found, or None.
    """
    if not _check_deps():
        return None

    query = f'"{name}" site:x.com'
    if email:
        query += f" {email}"

    skip_handles = {"home", "search", "login", "i", "settings", "explore", "messages"}

    urls = _search_brave(query)
    for url in urls:
        match = re.search(r"x\.com/(\w+)", url)
        if match and match.group(1).lower() not in skip_handles:
            return f"https://x.com/{match.group(1)}"

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
        "SELECT display_name, first_name, last_name FROM contacts WHERE id = ?",
        (contact_id,),
    ).fetchone()

    if not contact:
        return {}

    name = contact["display_name"]

    # Skip contacts whose name is just a phone number or a single word handle
    if re.match(r"^[\+\d\s\-\(\)]+$", name):
        logger.debug("Skipping social lookup for phone-number contact: %s", name)
        result = {"linkedin_url": None, "twitter_url": None}
        conn.execute(
            "INSERT OR REPLACE INTO enrichment_cache (contact_id, source, data_json, fetched_at) "
            "VALUES (?, 'social', ?, datetime('now'))",
            (contact_id, json.dumps(result)),
        )
        conn.commit()
        return result

    # Get email if available
    email_row = conn.execute(
        "SELECT identifier_value FROM contact_identifiers "
        "WHERE contact_id = ? AND identifier_type = 'email' LIMIT 1",
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
        "INSERT OR REPLACE INTO enrichment_cache (contact_id, source, data_json, fetched_at) "
        "VALUES (?, 'social', ?, datetime('now'))",
        (contact_id, json.dumps(result)),
    )

    # Update contact record
    if result["linkedin_url"] or result["twitter_url"]:
        conn.execute(
            "UPDATE contacts SET linkedin_url = COALESCE(?, linkedin_url), "
            "twitter_url = COALESCE(?, twitter_url), updated_at = datetime('now') "
            "WHERE id = ?",
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
