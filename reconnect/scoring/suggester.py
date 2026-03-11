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

from reconnect.enrichment.social import enrich_contact
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

            # Build conversation context enrichment (skip live social lookups to stay fast)
            enrichment = _build_enrichment(conn, contact_id, skip_social=True)

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
          AND (c.skip_until IS NULL OR c.skip_until < date('now'))
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


def get_replacement_candidate(
    conn: sqlite3.Connection, exclude_contact_ids: list[int]
) -> dict | None:
    """Find a single replacement candidate not in the exclude list."""
    cfg = get_effective_suggestion_config(conn)
    min_ix = cfg.get("min_interactions_threshold", MIN_INTERACTIONS_THRESHOLD)
    active_days = cfg.get("active_days_threshold", ACTIVE_DAYS_THRESHOLD)

    if not exclude_contact_ids:
        exclude_contact_ids = [0]

    placeholders = ",".join("?" * len(exclude_contact_ids))
    candidates = conn.execute(
        f"""
        SELECT c.id as contact_id, c.display_name, cs.suggestion_score
        FROM contacts c
        JOIN contact_scores cs ON c.id = cs.contact_id
        WHERE c.is_excluded = 0
          AND (c.skip_until IS NULL OR c.skip_until < date('now'))
          AND cs.total_interactions >= ?
          AND cs.days_since_last >= ?
          AND cs.suggestion_score > 0
          AND c.id NOT IN ({placeholders})
        ORDER BY cs.suggestion_score DESC
        LIMIT 10
        """,
        (min_ix, active_days, *exclude_contact_ids),
    ).fetchall()

    if not candidates:
        return None

    pick = dict(random.choice(candidates[: min(5, len(candidates))]))
    cid = pick["contact_id"]

    narratives = _get_narratives(conn, cid)
    primary = narratives[0] if narratives else None
    enrichment = _build_enrichment(conn, cid, skip_social=True)

    return {
        "contact_id": cid,
        "display_name": pick["display_name"],
        "score": pick["suggestion_score"],
        "narratives": narratives,
        "primary": primary,
        "enrichment": enrichment,
    }


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


def _build_enrichment(conn: sqlite3.Connection, contact_id: int, *, skip_social: bool = False) -> dict:
    """Build conversation context enrichment for a suggestion.

    Pulls recent email subjects, iMessage snippets, and interaction breakdown.
    """
    enrichment = {}

    # Recent email subjects from metadata_json
    email_rows = conn.execute(
        """
        SELECT metadata_json, occurred_at FROM interactions
        WHERE contact_id = ? AND source = 'gmail'
        ORDER BY occurred_at DESC
        LIMIT 50
        """,
        (contact_id,),
    ).fetchall()

    subjects = []
    for row in email_rows:
        if row["metadata_json"]:
            try:
                meta = json.loads(row["metadata_json"])
                subj = meta.get("subject", "")
                dt = row["occurred_at"][:10] if row["occurred_at"] else ""
                if subj and subj not in subjects:
                    subjects.append({"text": subj, "date": dt})
            except (json.JSONDecodeError, TypeError):
                pass
    if subjects:
        enrichment["recent_subjects"] = subjects[:50]

    # Recent calendar events from metadata_json
    cal_rows = conn.execute(
        """
        SELECT metadata_json, occurred_at FROM interactions
        WHERE contact_id = ? AND source = 'calendar'
        ORDER BY occurred_at DESC
        LIMIT 20
        """,
        (contact_id,),
    ).fetchall()

    events = []
    for row in cal_rows:
        if row["metadata_json"]:
            try:
                meta = json.loads(row["metadata_json"])
                title = meta.get("title") or meta.get("summary") or meta.get("subject", "")
                dt = row["occurred_at"][:10] if row["occurred_at"] else ""
                if title:
                    events.append({"text": title, "date": dt})
            except (json.JSONDecodeError, TypeError):
                pass
    if events:
        enrichment["recent_events"] = events[:20]

    # Recent iMessage text snippets
    msg_rows = conn.execute(
        """
        SELECT metadata_json, occurred_at FROM interactions
        WHERE contact_id = ? AND source = 'imessage'
          AND metadata_json LIKE '%"text"%'
        ORDER BY occurred_at DESC
        LIMIT 50
        """,
        (contact_id,),
    ).fetchall()

    messages = []
    seen_texts = set()
    for row in msg_rows:
        if row["metadata_json"]:
            try:
                meta = json.loads(row["metadata_json"])
                text = meta.get("text", "").strip()
                dt = row["occurred_at"][:10] if row["occurred_at"] else ""
                if text and len(text) > 5 and text not in seen_texts:
                    seen_texts.add(text)
                    messages.append({"text": text, "date": dt})
            except (json.JSONDecodeError, TypeError):
                pass
    if messages:
        enrichment["recent_messages"] = messages[:50]

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

    # --- Relationship timeline stats ---
    timeline = conn.execute(
        """
        SELECT MIN(occurred_at) as first_at, MAX(occurred_at) as last_at,
               COUNT(*) as total
        FROM interactions WHERE contact_id = ?
        """,
        (contact_id,),
    ).fetchone()

    if timeline and timeline["first_at"]:
        first_dt = date.fromisoformat(timeline["first_at"][:10])
        last_dt = date.fromisoformat(timeline["last_at"][:10])
        today = date.today()

        days_known = (today - first_dt).days
        silence_days = (today - last_dt).days

        enrichment["first_interaction_date"] = timeline["first_at"][:10]
        enrichment["days_known"] = days_known
        enrichment["silence_days"] = silence_days

        # Build relationship headline
        parts = []
        if days_known >= 730:
            parts.append(f"{days_known // 365} years of history")
        elif days_known >= 90:
            parts.append(f"Connected for {days_known // 30} months")

        if silence_days >= 365:
            years = silence_days // 365
            parts.append(
                f"off the radar for {years}+ year{'s' if years > 1 else ''}"
            )
        elif silence_days >= 60:
            parts.append(f"quiet for {silence_days // 30} months")
        elif silence_days >= 30:
            parts.append("went quiet about a month ago")

        if parts:
            enrichment["headline"] = " \u2014 ".join(parts)

    # Peak communication month
    peak = conn.execute(
        """
        SELECT strftime('%Y-%m', occurred_at) as mo, COUNT(*) as cnt
        FROM interactions WHERE contact_id = ?
        GROUP BY mo ORDER BY cnt DESC LIMIT 1
        """,
        (contact_id,),
    ).fetchone()
    if peak and peak["mo"]:
        enrichment["peak_month"] = peak["mo"]
        enrichment["peak_count"] = peak["cnt"]

    # Last conversation previews
    if messages:
        enrichment["last_message_preview"] = messages[0]["text"][:120]
        enrichment["last_message_date"] = messages[0]["date"]
    if subjects:
        enrichment["last_subject_preview"] = subjects[0]["text"][:120]
        enrichment["last_subject_date"] = subjects[0]["date"]

    # Social profile enrichment (LinkedIn, Twitter) — skip if caller says so
    if not skip_social:
        try:
            social = enrich_contact(contact_id, conn)
            if social.get("linkedin_url"):
                enrichment["linkedin_url"] = social["linkedin_url"]
            if social.get("twitter_url"):
                enrichment["twitter_url"] = social["twitter_url"]
        except Exception as e:
            logger.warning("Social enrichment failed for contact %d: %s", contact_id, e)
    else:
        # Still pull cached social data if available (instant, no network)
        cached = conn.execute(
            "SELECT data_json FROM enrichment_cache WHERE contact_id = ? AND source = 'social'",
            (contact_id,),
        ).fetchone()
        if cached:
            import json as _json
            data = _json.loads(cached["data_json"])
            if data.get("linkedin_url"):
                enrichment["linkedin_url"] = data["linkedin_url"]
            if data.get("twitter_url"):
                enrichment["twitter_url"] = data["twitter_url"]

    return enrichment
