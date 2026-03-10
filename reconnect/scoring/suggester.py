"""Monthly suggestion generation.

Selects 10 contacts to suggest for reconnection based on scores
and pattern matches, with a controlled randomization element.
"""

from __future__ import annotations

import json
import logging
import random
import sqlite3
from datetime import date, datetime

from reconnect.config import (
    ACTIVE_DAYS_THRESHOLD,
    MIN_INTERACTIONS_THRESHOLD,
    RECENT_SUGGESTION_MONTHS,
    SUGGESTIONS_PER_BATCH,
    SURPRISE_PICK_COUNT,
    SURPRISE_POOL_SIZE,
    TOP_PICK_COUNT,
)
from reconnect.database import get_connection
from reconnect.scoring.settings import get_effective_suggestion_config

logger = logging.getLogger(__name__)


def generate_suggestions(month_label: str | None = None) -> dict:
    """Generate the monthly batch of 10 suggestions.

    Args:
        month_label: Override month label (default: current month YYYY-MM).

    Returns:
        Summary with the generated suggestions.
    """
    if not month_label:
        month_label = date.today().strftime("%Y-%m")

    conn = get_connection()
    try:
        # Load suggestion config with DB overrides
        cfg = get_effective_suggestion_config(conn)

        # Check if batch already exists for this month
        existing = conn.execute(
            "SELECT id FROM suggestion_batches WHERE month_label = ?",
            (month_label,),
        ).fetchone()

        if existing:
            logger.info("Batch already exists for %s", month_label)
            return _get_batch_summary(conn, existing["id"])

        # Get candidate contacts
        candidates = _get_candidates(conn, cfg)

        if not candidates:
            logger.warning("No candidates found for suggestions")
            return {"status": "no_candidates", "suggestions": []}

        # Select with weighted randomization
        selected = _select_suggestions(candidates, cfg)

        # Create batch
        cursor = conn.execute(
            "INSERT INTO suggestion_batches (month_label) VALUES (?)",
            (month_label,),
        )
        batch_id = cursor.lastrowid

        suggestions = []
        for rank, candidate in enumerate(selected, 1):
            contact_id = candidate["contact_id"]

            # Get the best pattern match narrative
            narratives = _get_narratives(conn, contact_id)
            primary = narratives[0] if narratives else None

            # Build conversation context enrichment
            enrichment = _build_enrichment(conn, contact_id)

            conn.execute(
                """
                INSERT INTO suggestions
                    (batch_id, contact_id, rank, score_at_time,
                     primary_rule_id, primary_narrative, all_narratives_json,
                     enrichment_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    contact_id,
                    rank,
                    candidate["suggestion_score"],
                    primary["rule_id"] if primary else None,
                    primary["narrative"] if primary else None,
                    json.dumps(narratives) if narratives else None,
                    json.dumps(enrichment) if enrichment else None,
                ),
            )

            suggestions.append(
                {
                    "rank": rank,
                    "contact_id": contact_id,
                    "display_name": candidate["display_name"],
                    "score": candidate["suggestion_score"],
                    "narrative": primary["narrative"] if primary else None,
                    "rule_id": primary["rule_id"] if primary else None,
                }
            )

        conn.commit()

        logger.info(
            "Generated %d suggestions for %s", len(suggestions), month_label
        )
        return {
            "status": "ok",
            "batch_id": batch_id,
            "month_label": month_label,
            "suggestions": suggestions,
        }

    finally:
        conn.close()


def _get_candidates(conn: sqlite3.Connection, cfg: dict | None = None) -> list[dict]:
    """Get eligible contacts ranked by suggestion score.

    Filters out:
    - Excluded contacts
    - Contacts with too few interactions
    - Still-active contacts (recent interaction)
    - Recently suggested contacts
    """
    c = cfg or {}
    min_ix = c.get("min_interactions_threshold", MIN_INTERACTIONS_THRESHOLD)
    active_days = c.get("active_days_threshold", ACTIVE_DAYS_THRESHOLD)
    recent_months = c.get("recent_suggestion_months", RECENT_SUGGESTION_MONTHS)
    pool_size = c.get("surprise_pool_size", SURPRISE_POOL_SIZE)
    top_count = c.get("top_pick_count", TOP_PICK_COUNT)

    # Calculate the cutoff date for recent suggestions
    recent_cutoff = date.today().replace(
        month=max(1, date.today().month - recent_months)
    )

    candidates = conn.execute(
        """
        SELECT c.id as contact_id, c.display_name,
               cs.suggestion_score, cs.total_interactions,
               cs.days_since_last, cs.last_interaction_at
        FROM contacts c
        JOIN contact_scores cs ON c.id = cs.contact_id
        WHERE c.is_excluded = 0
          AND cs.total_interactions >= ?
          AND cs.days_since_last >= ?
          AND cs.suggestion_score > 0
          AND c.id NOT IN (
              SELECT s.contact_id FROM suggestions s
              JOIN suggestion_batches sb ON s.batch_id = sb.id
              WHERE sb.generated_at >= ?
          )
        ORDER BY cs.suggestion_score DESC
        LIMIT ?
        """,
        (
            min_ix,
            active_days,
            recent_cutoff.isoformat(),
            pool_size + top_count,
        ),
    ).fetchall()

    return [dict(row) for row in candidates]


def _select_suggestions(candidates: list[dict], cfg: dict | None = None) -> list[dict]:
    """Select final suggestions with weighted randomization.

    top_pick_count from top tier + surprise_pick_count surprise picks from next tier.
    """
    c = cfg or {}
    total_needed = c.get("suggestions_per_batch", SUGGESTIONS_PER_BATCH)
    top_count = c.get("top_pick_count", TOP_PICK_COUNT)
    surprise_count_target = c.get("surprise_pick_count", SURPRISE_PICK_COUNT)

    if len(candidates) <= total_needed:
        return candidates[:total_needed]

    # Top picks: sample from top tier (slight shuffle within top tier)
    top_pool = candidates[: top_count + 3]
    top_picks = random.sample(
        top_pool, min(top_count, len(top_pool))
    )

    # Surprise picks: weighted random from the rest
    remaining = [c for c in candidates if c not in top_picks]
    if remaining:
        # Weight by score (higher score = higher probability)
        weights = [max(c["suggestion_score"], 0.01) for c in remaining]
        surprise_count = min(surprise_count_target, len(remaining))
        surprise_picks = random.choices(
            remaining, weights=weights, k=surprise_count
        )
        # Remove duplicates
        seen = {c["contact_id"] for c in top_picks}
        surprise_picks = [
            c for c in surprise_picks if c["contact_id"] not in seen
        ]
    else:
        surprise_picks = []

    selected = top_picks + surprise_picks
    return selected[:total_needed]


def _get_narratives(conn: sqlite3.Connection, contact_id: int) -> list[dict]:
    """Get all pattern match narratives for a contact, sorted by score."""
    rows = conn.execute(
        """
        SELECT rule_id, narrative, score_contribution, match_data_json
        FROM pattern_matches
        WHERE contact_id = ?
        ORDER BY score_contribution DESC
        """,
        (contact_id,),
    ).fetchall()

    return [
        {
            "rule_id": row["rule_id"],
            "narrative": row["narrative"],
            "score": row["score_contribution"],
            "match_data": json.loads(row["match_data_json"])
            if row["match_data_json"]
            else {},
        }
        for row in rows
    ]


def _get_batch_summary(conn: sqlite3.Connection, batch_id: int) -> dict:
    """Get summary of an existing batch."""
    batch = conn.execute(
        "SELECT * FROM suggestion_batches WHERE id = ?", (batch_id,)
    ).fetchone()

    suggestions = conn.execute(
        """
        SELECT s.*, c.display_name
        FROM suggestions s
        JOIN contacts c ON s.contact_id = c.id
        WHERE s.batch_id = ?
        ORDER BY s.rank
        """,
        (batch_id,),
    ).fetchall()

    return {
        "status": "existing",
        "batch_id": batch_id,
        "month_label": batch["month_label"],
        "suggestions": [
            {
                "rank": s["rank"],
                "contact_id": s["contact_id"],
                "display_name": s["display_name"],
                "score": s["score_at_time"],
                "narrative": s["primary_narrative"],
                "rule_id": s["primary_rule_id"],
                "feedback": s["feedback"],
            }
            for s in suggestions
        ],
    }


def _build_enrichment(conn: sqlite3.Connection, contact_id: int) -> dict:
    """Build conversation context enrichment for a suggestion.

    Pulls recent email subjects, iMessage snippets, and interaction breakdown.
    """
    enrichment = {}

    # Recent email subjects from metadata_json
    email_rows = conn.execute(
        """
        SELECT metadata_json FROM interactions
        WHERE contact_id = ? AND source = 'gmail'
        ORDER BY occurred_at DESC
        LIMIT 5
        """,
        (contact_id,),
    ).fetchall()

    subjects = []
    for row in email_rows:
        if row["metadata_json"]:
            try:
                meta = json.loads(row["metadata_json"])
                subj = meta.get("subject", "")
                if subj and subj not in subjects:
                    subjects.append(subj)
            except (json.JSONDecodeError, TypeError):
                pass
    if subjects:
        enrichment["recent_subjects"] = subjects[:5]

    # Recent iMessage text snippets
    msg_rows = conn.execute(
        """
        SELECT metadata_json, occurred_at FROM interactions
        WHERE contact_id = ? AND source = 'imessage'
          AND metadata_json LIKE '%"text"%'
        ORDER BY occurred_at DESC
        LIMIT 10
        """,
        (contact_id,),
    ).fetchall()

    messages = []
    for row in msg_rows:
        if row["metadata_json"]:
            try:
                meta = json.loads(row["metadata_json"])
                text = meta.get("text", "").strip()
                if text and len(text) > 5 and text not in messages:
                    messages.append(text)
            except (json.JSONDecodeError, TypeError):
                pass
    if messages:
        enrichment["recent_messages"] = messages[:5]

    # Last interaction date by source
    last_by_source = {}
    rows = conn.execute(
        """
        SELECT source, MAX(occurred_at) as last_at
        FROM interactions
        WHERE contact_id = ?
        GROUP BY source
        """,
        (contact_id,),
    ).fetchall()
    for row in rows:
        last_by_source[row["source"]] = row["last_at"][:10] if row["last_at"] else None
    if last_by_source:
        enrichment["last_by_source"] = last_by_source

    # Total interaction count by source
    total_by_source = {}
    rows = conn.execute(
        """
        SELECT source, COUNT(*) as cnt
        FROM interactions
        WHERE contact_id = ?
        GROUP BY source
        """,
        (contact_id,),
    ).fetchall()
    for row in rows:
        total_by_source[row["source"]] = row["cnt"]
    if total_by_source:
        enrichment["total_by_source"] = total_by_source

    return enrichment
