"""Process user feedback on suggestions to improve future recommendations."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

from reconnect.config import (
    FEEDBACK_BOOST_MAX,
    FEEDBACK_BOOST_MIN,
    FEEDBACK_NO_PENALTY,
    FEEDBACK_YES_BOOST,
)
from reconnect.database import get_connection

logger = logging.getLogger(__name__)


def submit_feedback(
    suggestion_id: int, feedback: str, reason: str | None = None
) -> dict:
    """Record yes/no feedback on a suggestion and adjust scores.

    Args:
        suggestion_id: The suggestion to provide feedback on.
        feedback: 'yes' or 'no'.
        reason: Optional reason for 'no' (e.g., 'moved away').

    Returns:
        Summary of the feedback action.
    """
    if feedback not in ("yes", "no"):
        return {"status": "error", "reason": "feedback must be 'yes' or 'no'"}

    conn = get_connection()
    try:
        # Get the suggestion
        suggestion = conn.execute(
            "SELECT * FROM suggestions WHERE id = ?", (suggestion_id,)
        ).fetchone()

        if not suggestion:
            return {"status": "error", "reason": "suggestion not found"}

        if suggestion["feedback"]:
            return {"status": "already_submitted", "existing": suggestion["feedback"]}

        # Record the feedback
        conn.execute(
            """
            UPDATE suggestions
            SET feedback = ?, feedback_at = datetime('now')
            WHERE id = ?
            """,
            (feedback, suggestion_id),
        )

        # Adjust the contact's feedback boost
        contact_id = suggestion["contact_id"]
        adjustment = FEEDBACK_YES_BOOST if feedback == "yes" else FEEDBACK_NO_PENALTY

        conn.execute(
            """
            UPDATE contact_scores
            SET feedback_boost = MIN(?, MAX(?, feedback_boost + ?)),
                updated_at = datetime('now')
            WHERE contact_id = ?
            """,
            (FEEDBACK_BOOST_MAX, FEEDBACK_BOOST_MIN, adjustment, contact_id),
        )

        # If user wants to permanently exclude
        if reason == "exclude":
            conn.execute(
                "UPDATE contacts SET is_excluded = 1 WHERE id = ?",
                (contact_id,),
            )

        conn.commit()

        logger.info(
            "Feedback recorded: suggestion=%d, contact=%d, feedback=%s",
            suggestion_id,
            contact_id,
            feedback,
        )

        return {
            "status": "ok",
            "suggestion_id": suggestion_id,
            "contact_id": contact_id,
            "feedback": feedback,
            "adjustment": adjustment,
        }
    finally:
        conn.close()
