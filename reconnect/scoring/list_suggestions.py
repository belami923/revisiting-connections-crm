"""Auto-suggest contact lists based on interaction patterns."""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


def auto_generate_lists(conn: sqlite3.Connection) -> dict:
    """Analyze interaction data and create auto-suggested lists.

    Deletes existing auto lists and regenerates them from scratch.
    Returns a summary of lists created.
    """
    # Clear previous auto lists (CASCADE deletes memberships)
    conn.execute("DELETE FROM custom_lists WHERE is_auto = 1")
    conn.commit()

    rules = [
        _yearly_top_contacts,
        _weekend_friends,
        _work_contacts,
        _message_heavy,
        _faded_close,
        _calendar_regulars,
    ]

    lists_created = 0
    for rule_fn in rules:
        try:
            created = rule_fn(conn)
            lists_created += created
        except Exception:
            logger.exception("Auto-list rule %s failed", rule_fn.__name__)

    conn.commit()
    return {"lists_created": lists_created}


def _create_auto_list(
    conn: sqlite3.Connection,
    name: str,
    description: str,
    auto_rule: str,
    contact_ids: list[int],
) -> bool:
    """Helper to create an auto list with members. Returns True if created."""
    if not contact_ids:
        return False
    try:
        cur = conn.execute(
            "INSERT INTO custom_lists (name, description, is_auto, auto_rule) VALUES (?, ?, 1, ?)",
            (name, description, auto_rule),
        )
        list_id = cur.lastrowid
        for cid in contact_ids:
            conn.execute(
                "INSERT OR IGNORE INTO list_memberships (list_id, contact_id) VALUES (?, ?)",
                (list_id, cid),
            )
        return True
    except sqlite3.IntegrityError:
        # Name collision with a manual list — skip
        return False


def _yearly_top_contacts(conn: sqlite3.Connection) -> int:
    """Top 5 contacts by interaction count for each year with 50+ interactions."""
    rows = conn.execute(
        """
        SELECT strftime('%Y', occurred_at) as year,
               contact_id,
               COUNT(*) as cnt
        FROM interactions
        GROUP BY year, contact_id
        HAVING cnt >= 50
        ORDER BY year, cnt DESC
        """
    ).fetchall()

    # Group by year
    by_year: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        year = row["year"]
        if len(by_year[year]) < 5:
            by_year[year].append(row["contact_id"])

    created = 0
    for year, cids in sorted(by_year.items()):
        if _create_auto_list(
            conn,
            f"{year} Inner Circle",
            f"Your top {len(cids)} most-contacted people in {year}.",
            "yearly_top",
            cids,
        ):
            created += 1
    return created


def _weekend_friends(conn: sqlite3.Connection) -> int:
    """Contacts where 55%+ of interactions were on weekends."""
    rows = conn.execute(
        """
        SELECT contact_id,
               COUNT(*) as total,
               SUM(CASE WHEN CAST(strftime('%w', occurred_at) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END) as weekend
        FROM interactions
        GROUP BY contact_id
        HAVING total >= 20
        """
    ).fetchall()

    cids = []
    for row in rows:
        if row["total"] > 0 and row["weekend"] / row["total"] >= 0.55:
            cids.append(row["contact_id"])

    created = 0
    if _create_auto_list(
        conn,
        "Weekend Friends",
        "People you mostly interact with on weekends — likely personal friendships.",
        "weekend_friends",
        cids,
    ):
        created = 1
    return created


def _work_contacts(conn: sqlite3.Connection) -> int:
    """Contacts with only email/calendar interactions, never iMessage."""
    rows = conn.execute(
        """
        SELECT contact_id,
               COUNT(*) as total,
               SUM(CASE WHEN source = 'imessage' THEN 1 ELSE 0 END) as imsg
        FROM interactions
        GROUP BY contact_id
        HAVING total >= 5 AND imsg = 0
        """
    ).fetchall()

    cids = [row["contact_id"] for row in rows]
    created = 0
    if _create_auto_list(
        conn,
        "Work Contacts",
        "People you only interact with via email or calendar — likely professional relationships.",
        "work_contacts",
        cids,
    ):
        created = 1
    return created


def _message_heavy(conn: sqlite3.Connection) -> int:
    """Contacts with 200+ messages total."""
    rows = conn.execute(
        """
        SELECT contact_id, COUNT(*) as cnt
        FROM interactions
        WHERE source = 'imessage'
        GROUP BY contact_id
        HAVING cnt >= 200
        ORDER BY cnt DESC
        """
    ).fetchall()

    cids = [row["contact_id"] for row in rows]
    created = 0
    if _create_auto_list(
        conn,
        "Frequent Texters",
        "People you've exchanged 200+ messages with.",
        "message_heavy",
        cids,
    ):
        created = 1
    return created


def _faded_close(conn: sqlite3.Connection) -> int:
    """Was a top-10 contact in any year, now 1+ year dormant."""
    # Find top-10 per year
    rows = conn.execute(
        """
        SELECT strftime('%Y', occurred_at) as year,
               contact_id,
               COUNT(*) as cnt
        FROM interactions
        GROUP BY year, contact_id
        ORDER BY year, cnt DESC
        """
    ).fetchall()

    ever_top: set[int] = set()
    by_year: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        year = row["year"]
        if len(by_year[year]) < 10:
            by_year[year].append(row["contact_id"])
            ever_top.add(row["contact_id"])

    # Filter to those dormant 1+ year
    now = datetime.now()
    cids = []
    for cid in ever_top:
        score = conn.execute(
            "SELECT days_since_last FROM contact_scores WHERE contact_id = ?",
            (cid,),
        ).fetchone()
        if score and score["days_since_last"] and score["days_since_last"] >= 365:
            cids.append(cid)

    created = 0
    if _create_auto_list(
        conn,
        "Faded Close Friends",
        "Once in your top contacts, now over a year since last interaction.",
        "faded_close",
        cids,
    ):
        created = 1
    return created


def _calendar_regulars(conn: sqlite3.Connection) -> int:
    """Contacts with 10+ calendar events together."""
    rows = conn.execute(
        """
        SELECT contact_id, COUNT(*) as cnt
        FROM interactions
        WHERE source = 'calendar'
        GROUP BY contact_id
        HAVING cnt >= 10
        ORDER BY cnt DESC
        """
    ).fetchall()

    cids = [row["contact_id"] for row in rows]
    created = 0
    if _create_auto_list(
        conn,
        "Calendar Regulars",
        "People you've had 10+ calendar events with.",
        "calendar_regulars",
        cids,
    ):
        created = 1
    return created
