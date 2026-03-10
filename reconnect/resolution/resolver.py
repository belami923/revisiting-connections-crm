"""Entity resolution pipeline.

Merges contacts that represent the same person across different sources.
Runs after ingestion to deduplicate the contacts table.
"""

from __future__ import annotations

import logging
import sqlite3

from thefuzz import fuzz

from reconnect.database import get_connection
from reconnect.resolution.normalizer import normalize_name

logger = logging.getLogger(__name__)

# Minimum fuzzy match score to consider two names the same person
NAME_MATCH_THRESHOLD = 85


def resolve_duplicates() -> dict:
    """Find and merge duplicate contacts.

    Resolution tiers:
    1. Same phone or email across different sources -> auto-merge
    2. Fuzzy name match between contacts with overlapping interaction periods -> suggest

    Returns summary of merges performed.
    """
    conn = get_connection()
    try:
        merges = _merge_by_shared_identifiers(conn)
        conn.commit()
        return {"status": "ok", "auto_merges": merges}
    finally:
        conn.close()


def _merge_by_shared_identifiers(conn: sqlite3.Connection) -> int:
    """Merge contacts that share the same normalized identifier value.

    This handles the case where:
    - Contact A was created from Apple Contacts with phone +14155551234
    - Contact B was created from iMessage with the same phone +14155551234
    These should be the same person.
    """
    # Find identifier values that appear on multiple contacts
    dupes = conn.execute(
        """
        SELECT identifier_type, identifier_value,
               GROUP_CONCAT(DISTINCT contact_id) as contact_ids
        FROM contact_identifiers
        GROUP BY identifier_type, identifier_value
        HAVING COUNT(DISTINCT contact_id) > 1
        """
    ).fetchall()

    merge_count = 0

    for row in dupes:
        contact_ids = [int(cid) for cid in row["contact_ids"].split(",")]
        if len(contact_ids) < 2:
            continue

        # Keep the contact with the best display name (prefer non-phone/email)
        survivor_id = _pick_survivor(conn, contact_ids)
        to_merge = [cid for cid in contact_ids if cid != survivor_id]

        for merge_id in to_merge:
            _merge_contacts(conn, survivor_id, merge_id)
            merge_count += 1

    return merge_count


def _pick_survivor(conn: sqlite3.Connection, contact_ids: list[int]) -> int:
    """Pick the best contact record to keep.

    Prefers contacts with real names (from Apple Contacts) over
    contacts with phone/email as display name (from iMessage).
    """
    best_id = contact_ids[0]
    best_score = 0

    for cid in contact_ids:
        row = conn.execute(
            "SELECT display_name, first_name, last_name FROM contacts WHERE id = ?",
            (cid,),
        ).fetchone()

        if not row:
            continue

        score = 0
        # Prefer contacts with first_name set (from Apple Contacts)
        if row["first_name"]:
            score += 10
        if row["last_name"]:
            score += 5
        # Prefer contacts whose display name doesn't look like a phone/email
        display = row["display_name"] or ""
        if "@" not in display and "+" not in display and not display.isdigit():
            score += 3

        if score > best_score:
            best_score = score
            best_id = cid

    return best_id


def _merge_contacts(
    conn: sqlite3.Connection, survivor_id: int, merge_id: int
) -> None:
    """Merge merge_id into survivor_id.

    Moves all identifiers and interactions from merge_id to survivor_id,
    then deletes the merge_id contact.
    """
    logger.debug("Merging contact %d into %d", merge_id, survivor_id)

    # Move identifiers (ignore conflicts)
    conn.execute(
        """
        UPDATE OR IGNORE contact_identifiers
        SET contact_id = ?
        WHERE contact_id = ?
        """,
        (survivor_id, merge_id),
    )
    # Delete any identifiers that couldn't be moved (duplicates)
    conn.execute(
        "DELETE FROM contact_identifiers WHERE contact_id = ?",
        (merge_id,),
    )

    # Move interactions
    conn.execute(
        "UPDATE interactions SET contact_id = ? WHERE contact_id = ?",
        (survivor_id, merge_id),
    )

    # Move scores (delete old, keep survivor's)
    conn.execute(
        "DELETE FROM contact_scores WHERE contact_id = ?", (merge_id,)
    )

    # Move pattern matches
    conn.execute(
        "UPDATE pattern_matches SET contact_id = ? WHERE contact_id = ?",
        (survivor_id, merge_id),
    )

    # Move suggestions
    conn.execute(
        "UPDATE suggestions SET contact_id = ? WHERE contact_id = ?",
        (survivor_id, merge_id),
    )

    # Delete the merged contact
    conn.execute("DELETE FROM contacts WHERE id = ?", (merge_id,))


def find_possible_duplicates(limit: int = 50) -> list[dict]:
    """Find contacts that might be the same person based on fuzzy name matching.

    Returns a list of pairs for manual review.
    """
    conn = get_connection()
    try:
        contacts = conn.execute(
            "SELECT id, display_name, first_name, last_name FROM contacts "
            "WHERE is_excluded = 0 AND display_name != '' "
            "ORDER BY display_name"
        ).fetchall()

        pairs = []
        seen = set()

        for i, c1 in enumerate(contacts):
            name1 = normalize_name(c1["display_name"])
            if not name1 or len(name1) < 3:
                continue

            for c2 in contacts[i + 1 :]:
                name2 = normalize_name(c2["display_name"])
                if not name2 or len(name2) < 3:
                    continue

                pair_key = (min(c1["id"], c2["id"]), max(c1["id"], c2["id"]))
                if pair_key in seen:
                    continue

                score = fuzz.token_sort_ratio(name1, name2)
                if score >= NAME_MATCH_THRESHOLD:
                    seen.add(pair_key)
                    pairs.append(
                        {
                            "contact_1": dict(c1),
                            "contact_2": dict(c2),
                            "similarity": score,
                        }
                    )

                    if len(pairs) >= limit:
                        return pairs

        return pairs
    finally:
        conn.close()
