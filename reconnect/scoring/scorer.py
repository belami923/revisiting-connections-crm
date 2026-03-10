"""Composite score computation for all contacts.

Recalculates interaction metrics and decay scores for every contact,
then runs pattern rules to detect narratives.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import date, datetime

from reconnect.config import INTERACTION_WEIGHTS
from reconnect.database import get_connection
from reconnect.scoring.rules import ALL_RULES
from reconnect.scoring.settings import apply_settings_to_rules, get_effective_weights

logger = logging.getLogger(__name__)


def recalculate_all_scores() -> dict:
    """Recalculate scores for all contacts and detect patterns.

    Returns summary stats.
    """
    conn = get_connection()
    try:
        contact_ids = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM contacts WHERE is_excluded = 0"
            ).fetchall()
        ]

        scored = 0
        patterns_found = 0

        # Load settings overrides
        weights = get_effective_weights(conn)
        active_rules = apply_settings_to_rules(conn, ALL_RULES)

        # Clear old pattern matches (will re-detect)
        conn.execute("DELETE FROM pattern_matches")

        for contact_id in contact_ids:
            interactions = _get_interactions(conn, contact_id)
            if not interactions:
                continue

            # Compute base metrics with weight overrides
            metrics = _compute_metrics(interactions, weights=weights)

            # Get existing feedback boost
            existing = conn.execute(
                "SELECT feedback_boost FROM contact_scores WHERE contact_id = ?",
                (contact_id,),
            ).fetchone()
            feedback_boost = existing["feedback_boost"] if existing else 0.0

            # Compute final suggestion score
            suggestion_score = _compute_suggestion_score(
                metrics, feedback_boost
            )

            # Upsert contact_scores
            conn.execute(
                """
                INSERT INTO contact_scores
                    (contact_id, total_interactions, peak_density,
                     peak_start, peak_end, last_interaction_at,
                     days_since_last, decay_score, suggestion_score,
                     feedback_boost, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(contact_id) DO UPDATE SET
                    total_interactions = excluded.total_interactions,
                    peak_density = excluded.peak_density,
                    peak_start = excluded.peak_start,
                    peak_end = excluded.peak_end,
                    last_interaction_at = excluded.last_interaction_at,
                    days_since_last = excluded.days_since_last,
                    decay_score = excluded.decay_score,
                    suggestion_score = excluded.suggestion_score,
                    updated_at = excluded.updated_at
                """,
                (
                    contact_id,
                    metrics["total_interactions"],
                    metrics["peak_density"],
                    metrics["peak_start"],
                    metrics["peak_end"],
                    metrics["last_interaction_at"],
                    metrics["days_since_last"],
                    metrics["decay_score"],
                    suggestion_score,
                    feedback_boost,
                ),
            )
            scored += 1

            # Run pattern rules (only active/enabled rules)
            ix_dicts = [dict(ix) for ix in interactions]
            for rule in active_rules:
                try:
                    match = rule.detect(contact_id, ix_dicts, conn)
                    if match:
                        conn.execute(
                            """
                            INSERT INTO pattern_matches
                                (contact_id, rule_id, narrative,
                                 score_contribution, match_data_json)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                contact_id,
                                match.rule_id,
                                match.narrative,
                                match.score_contribution,
                                json.dumps(match.match_data),
                            ),
                        )
                        patterns_found += 1
                except Exception as e:
                    logger.warning(
                        "Rule %s failed for contact %d: %s",
                        rule.rule_id,
                        contact_id,
                        e,
                    )

        conn.commit()
        logger.info(
            "Scored %d contacts, found %d pattern matches",
            scored,
            patterns_found,
        )
        return {
            "status": "ok",
            "contacts_scored": scored,
            "patterns_found": patterns_found,
        }
    finally:
        conn.close()


def _get_interactions(
    conn: sqlite3.Connection, contact_id: int
) -> list[sqlite3.Row]:
    """Get all interactions for a contact, ordered by date."""
    return conn.execute(
        """
        SELECT contact_id, source, interaction_type, occurred_at,
               source_id, metadata_json
        FROM interactions
        WHERE contact_id = ?
        ORDER BY occurred_at ASC
        """,
        (contact_id,),
    ).fetchall()


def _compute_metrics(interactions: list[sqlite3.Row], weights: dict | None = None) -> dict:
    """Compute interaction metrics for a contact."""
    total = len(interactions)
    w = weights or INTERACTION_WEIGHTS

    # Weighted interaction count
    weighted_total = sum(
        w.get(ix["interaction_type"], 1.0)
        for ix in interactions
    )

    # Parse dates
    dates = []
    for ix in interactions:
        try:
            dates.append(datetime.fromisoformat(ix["occurred_at"]).date())
        except (ValueError, KeyError):
            continue

    if not dates:
        return {
            "total_interactions": total,
            "weighted_total": 0,
            "peak_density": 0,
            "peak_start": None,
            "peak_end": None,
            "last_interaction_at": None,
            "days_since_last": 0,
            "decay_score": 0,
        }

    dates.sort()
    last_date = dates[-1]
    days_since = (date.today() - last_date).days

    # Peak density: find the 8-week window with the highest density
    peak_density, peak_start, peak_end = _find_peak_window(dates, window_days=56)

    # Decay score: logarithmic decay based on time since last interaction
    # relative to the expected interaction frequency at peak
    if peak_density > 0:
        expected_gap = 7.0 / peak_density  # days between interactions at peak
        decay_score = min(
            1.0, math.log(1 + days_since / max(expected_gap, 1)) / math.log(11)
        )
    else:
        decay_score = min(1.0, days_since / 365.0)

    return {
        "total_interactions": total,
        "weighted_total": weighted_total,
        "peak_density": peak_density,
        "peak_start": peak_start.isoformat() if peak_start else None,
        "peak_end": peak_end.isoformat() if peak_end else None,
        "last_interaction_at": last_date.isoformat(),
        "days_since_last": days_since,
        "decay_score": decay_score,
    }


def _find_peak_window(
    dates: list[date], window_days: int = 56
) -> tuple[float, date | None, date | None]:
    """Find the window with highest interaction density (interactions/week).

    Returns (density_per_week, window_start, window_end).
    """
    if not dates:
        return (0.0, None, None)

    from datetime import timedelta

    best_density = 0.0
    best_start = dates[0]
    best_end = dates[0]

    for i, start in enumerate(dates):
        window_end_date = start + timedelta(days=window_days)
        count = sum(1 for d in dates[i:] if d <= window_end_date)
        density = count / (window_days / 7)  # interactions per week

        if density > best_density:
            best_density = density
            best_start = start
            best_end = min(window_end_date, dates[-1])

    return (best_density, best_start, best_end)


def _compute_suggestion_score(metrics: dict, feedback_boost: float) -> float:
    """Compute the final suggestion score.

    Score = relationship_value * decay_signal * feedback_modifier

    High score = high historical value AND high decay AND positive feedback.
    """
    weighted_total = metrics.get("weighted_total", 0)
    peak_density = metrics.get("peak_density", 0)
    decay_score = metrics.get("decay_score", 0)

    if weighted_total == 0 or decay_score == 0:
        return 0.0

    # Relationship value: how important was this person historically?
    relationship_value = math.log(1 + weighted_total) * (
        1 + math.log(1 + peak_density)
    )

    # Decay signal: higher = more decayed = more suggestable
    decay_signal = decay_score

    # Feedback modifier
    feedback_modifier = 1.0 + feedback_boost

    return relationship_value * decay_signal * max(feedback_modifier, 0.1)
