"""Pattern detection rules for suggesting reconnections.

Each rule detects a specific interaction pattern and generates a human-readable
narrative explaining why this person is worth reconnecting with.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class RuleMatch:
    """A detected pattern match for a contact."""

    rule_id: str
    contact_id: int
    score_contribution: float
    narrative: str
    match_data: dict = field(default_factory=dict)


class PatternRule(ABC):
    """Base class for pattern detection rules."""

    rule_id: str
    name: str
    description: str = ""
    parameters: dict = {}

    @abstractmethod
    def detect(
        self, contact_id: int, interactions: list[dict], conn: sqlite3.Connection
    ) -> RuleMatch | None:
        """Check if this pattern applies to the contact.

        Args:
            contact_id: The contact to check.
            interactions: All interactions for this contact, sorted by date.
            conn: Database connection for additional queries if needed.

        Returns:
            A RuleMatch if the pattern is found, None otherwise.
        """
        ...


class YearlyTopContact(PatternRule):
    """Detect contacts who were among the most-messaged in a given year."""

    rule_id = "yearly_top_contact"
    name = "Yearly Top Contact"
    description = "Detects contacts who were among your most-messaged people in a given year but have since gone silent."
    min_yearly_interactions = 50
    silence_days = 90
    parameters = {
        "min_yearly_interactions": {"default": 50, "description": "Minimum interactions in a year to be notable", "type": "int"},
        "silence_days": {"default": 90, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        if not interactions:
            return None

        # Count interactions per year
        yearly_counts = defaultdict(int)
        for ix in interactions:
            try:
                year = datetime.fromisoformat(ix["occurred_at"]).year
                yearly_counts[year] += 1
            except (ValueError, KeyError):
                continue

        if not yearly_counts:
            return None

        # Find the year with the most interactions for this contact
        best_year = max(yearly_counts, key=yearly_counts.get)
        best_count = yearly_counts[best_year]

        if best_count < self.min_yearly_interactions:
            return None

        # Check if this is still an active relationship
        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        # Find their rank among all contacts for that year
        rank = _get_yearly_rank(conn, contact_id, best_year)

        contact_name = _get_contact_name(conn, contact_id)

        if rank and rank <= 10:
            narrative = (
                f"In {best_year}, {contact_name} was your #{rank} most-messaged "
                f"person ({best_count:,} interactions). "
                f"You haven't been in touch for {_format_days(days_since)}."
            )
        else:
            narrative = (
                f"In {best_year}, you had {best_count:,} interactions with "
                f"{contact_name}. "
                f"You haven't been in touch for {_format_days(days_since)}."
            )

        # Score higher for higher counts and longer silence
        score = min(best_count / 100, 5.0) * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "year": best_year,
                "count": best_count,
                "rank": rank,
                "days_since_last": days_since,
            },
        )


class MeetingBurst(PatternRule):
    """Detect bursts of calendar meetings with a person in a short window."""

    rule_id = "meeting_burst"
    name = "Meeting Burst"
    description = "Detects bursts of calendar meetings in a short window that indicate an intense working relationship."
    min_meetings = 3
    window_days = 14
    silence_days = 60
    parameters = {
        "min_meetings": {"default": 3, "description": "Minimum meetings in burst window", "type": "int"},
        "window_days": {"default": 14, "description": "Window size in days", "type": "int"},
        "silence_days": {"default": 60, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        # Filter to calendar events only
        cal_events = [
            ix for ix in interactions if ix["interaction_type"] == "calendar_event"
        ]
        if len(cal_events) < self.min_meetings:
            return None

        best_burst = _find_burst(cal_events, window_days=self.window_days, min_count=self.min_meetings)
        if not best_burst:
            return None

        burst_count, burst_start, burst_end = best_burst

        # Check if still active
        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)
        month_year = burst_start.strftime("%B %Y")
        window_desc = f"{(burst_end - burst_start).days + 1} days"

        narrative = (
            f"In {month_year}, you had {burst_count} meetings with "
            f"{contact_name} in {window_desc}. "
            f"You haven't connected for {_format_days(days_since)}."
        )

        score = burst_count * 1.5 * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "burst_count": burst_count,
                "burst_start": burst_start.isoformat(),
                "burst_end": burst_end.isoformat(),
                "days_since_last": days_since,
            },
        )


class RegularSyncs(PatternRule):
    """Detect contacts you had regular recurring meetings with."""

    rule_id = "regular_syncs"
    name = "Regular Syncs"
    description = "Detects contacts you had regular recurring meetings with over consecutive months."
    min_calendar_events = 6
    min_streak_months = 3
    silence_days = 60
    parameters = {
        "min_calendar_events": {"default": 6, "description": "Minimum total calendar events", "type": "int"},
        "min_streak_months": {"default": 3, "description": "Minimum consecutive months with meetings", "type": "int"},
        "silence_days": {"default": 60, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        cal_events = [
            ix for ix in interactions if ix["interaction_type"] == "calendar_event"
        ]
        if len(cal_events) < self.min_calendar_events:
            return None

        # Group by month
        monthly = defaultdict(int)
        for ix in cal_events:
            try:
                dt = datetime.fromisoformat(ix["occurred_at"])
                monthly[(dt.year, dt.month)] += 1
            except (ValueError, KeyError):
                continue

        if not monthly:
            return None

        # Find consecutive months with meetings
        sorted_months = sorted(monthly.keys())
        best_streak_start = sorted_months[0]
        best_streak_len = 1
        current_start = sorted_months[0]
        current_len = 1
        total_in_streak = monthly[sorted_months[0]]

        for i in range(1, len(sorted_months)):
            prev = sorted_months[i - 1]
            curr = sorted_months[i]

            # Check if consecutive month
            expected_next = (prev[0] + (prev[1] // 12), (prev[1] % 12) + 1)
            if curr == expected_next:
                current_len += 1
                total_in_streak += monthly[curr]
            else:
                if current_len > best_streak_len:
                    best_streak_len = current_len
                    best_streak_start = current_start
                current_start = curr
                current_len = 1
                total_in_streak = monthly[curr]

        if current_len > best_streak_len:
            best_streak_len = current_len
            best_streak_start = current_start

        # Need at least min_streak_months consecutive months
        if best_streak_len < self.min_streak_months:
            return None

        # Check if still active
        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)

        start_str = date(best_streak_start[0], best_streak_start[1], 1).strftime(
            "%b %Y"
        )
        end_month = best_streak_start[1] + best_streak_len - 1
        end_year = best_streak_start[0] + (end_month - 1) // 12
        end_month = ((end_month - 1) % 12) + 1
        end_str = date(end_year, end_month, 1).strftime("%b %Y")

        narrative = (
            f"From {start_str} to {end_str}, you had regular syncs with "
            f"{contact_name} ({total_in_streak} meetings over "
            f"{best_streak_len} months). "
            f"The syncs stopped {_format_days(days_since)} ago."
        )

        score = best_streak_len * 1.2 * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "streak_months": best_streak_len,
                "streak_start": f"{best_streak_start[0]}-{best_streak_start[1]:02d}",
                "total_meetings": total_in_streak,
                "days_since_last": days_since,
            },
        )


class MessageBurst(PatternRule):
    """Detect unusually high message volume in a short window."""

    rule_id = "message_burst"
    name = "Message Burst"
    description = "Detects unusually high message volume in a short window, indicating an intense texting relationship."
    min_messages = 50
    window_days = 14
    silence_days = 60
    parameters = {
        "min_messages": {"default": 50, "description": "Minimum messages in burst window", "type": "int"},
        "window_days": {"default": 14, "description": "Window size in days", "type": "int"},
        "silence_days": {"default": 60, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        # Filter to messages only
        messages = [
            ix
            for ix in interactions
            if ix["interaction_type"] in ("message_sent", "message_received")
        ]
        if len(messages) < 20:
            return None

        # Find the densest window
        best_burst = _find_burst(messages, window_days=self.window_days, min_count=self.min_messages)
        if not best_burst:
            return None

        burst_count, burst_start, burst_end = best_burst

        # Check if still active
        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)
        month_year = burst_start.strftime("%B %Y")
        window_days = (burst_end - burst_start).days + 1

        narrative = (
            f"In {month_year}, you and {contact_name} exchanged "
            f"{burst_count:,} messages in {window_days} days. "
            f"Then silence for {_format_days(days_since)}."
        )

        score = min(burst_count / 50, 5.0) * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "burst_count": burst_count,
                "burst_start": burst_start.isoformat(),
                "burst_end": burst_end.isoformat(),
                "window_days": window_days,
                "days_since_last": days_since,
            },
        )


class MultiChannel(PatternRule):
    """Detect contacts who appeared across all 3 sources in the same period."""

    rule_id = "multi_channel"
    name = "Multi-Channel Relationship"
    description = "Detects contacts you interacted with across multiple channels (text, email, meetings) in the same period."
    min_sources = 2
    silence_days = 90
    parameters = {
        "min_sources": {"default": 2, "description": "Minimum different sources in a quarter", "type": "int"},
        "silence_days": {"default": 90, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        if not interactions:
            return None

        # Group interactions by quarter and source
        quarterly_sources = defaultdict(set)
        for ix in interactions:
            try:
                dt = datetime.fromisoformat(ix["occurred_at"])
                quarter = (dt.year, (dt.month - 1) // 3 + 1)
                quarterly_sources[quarter].add(ix["source"])
            except (ValueError, KeyError):
                continue

        # Find quarters with min_sources+ sources
        multi_quarters = [
            (q, sources)
            for q, sources in quarterly_sources.items()
            if len(sources) >= self.min_sources
        ]

        if not multi_quarters:
            return None

        # Pick the best quarter (most sources, most recent)
        multi_quarters.sort(key=lambda x: (len(x[1]), x[0]), reverse=True)
        best_quarter, best_sources = multi_quarters[0]

        # Check if still active
        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)
        q_label = f"Q{best_quarter[1]} {best_quarter[0]}"
        source_names = {
            "imessage": "texting",
            "gmail": "emailing",
            "calendar": "meeting with",
        }
        channels = " and ".join(
            source_names.get(s, s) for s in sorted(best_sources)
        )

        if len(best_sources) >= 3:
            narrative = (
                f"In {q_label}, you were texting, emailing, AND meeting with "
                f"{contact_name} regularly. Triple-channel relationships are rare. "
                f"You haven't connected for {_format_days(days_since)}."
            )
            score = 4.0 * min(days_since / 365, 3.0)
        else:
            narrative = (
                f"In {q_label}, you were {channels} {contact_name} regularly. "
                f"You haven't connected for {_format_days(days_since)}."
            )
            score = 2.5 * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "quarter": q_label,
                "sources": list(best_sources),
                "days_since_last": days_since,
            },
        )


class WeekendFriend(PatternRule):
    """Detect contacts whose interactions are concentrated on weekends."""

    rule_id = "weekend_friend"
    name = "Weekend Friend"
    description = "Detects contacts whose interactions are concentrated on weekends, indicating a personal friendship."
    min_interactions = 20
    weekend_ratio = 0.55
    silence_days = 60
    parameters = {
        "min_interactions": {"default": 20, "description": "Minimum total interactions", "type": "int"},
        "weekend_ratio": {"default": 0.55, "description": "Minimum weekend interaction ratio (0-1)", "type": "float"},
        "silence_days": {"default": 60, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        if len(interactions) < self.min_interactions:
            return None

        weekend_count = 0
        total = 0
        for ix in interactions:
            try:
                dt = datetime.fromisoformat(ix["occurred_at"])
                total += 1
                if dt.weekday() >= 5:  # Saturday=5, Sunday=6
                    weekend_count += 1
            except (ValueError, KeyError):
                continue

        if total < self.min_interactions:
            return None

        weekend_ratio = weekend_count / total

        # Weekends are 2/7 ≈ 28.6% of the week. If ratio exceeds threshold,
        # this is a personal friendship signal.
        if weekend_ratio < self.weekend_ratio:
            return None

        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)
        pct = int(weekend_ratio * 100)

        narrative = (
            f"{pct}% of your interactions with {contact_name} were on weekends "
            f"- this was a personal friendship, not just a work contact. "
            f"You haven't been in touch for {_format_days(days_since)}."
        )

        score = 2.0 * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "weekend_ratio": weekend_ratio,
                "weekend_count": weekend_count,
                "total_count": total,
                "days_since_last": days_since,
            },
        )


class ReciprocalInitiator(PatternRule):
    """Detect contacts where both sides initiated conversations equally."""

    rule_id = "reciprocal_initiator"
    name = "Balanced Relationship"
    description = "Detects contacts where both sides initiated conversations equally, indicating a healthy mutual relationship."
    min_messages = 30
    min_balance_ratio = 0.35
    silence_days = 60
    parameters = {
        "min_messages": {"default": 30, "description": "Minimum total sent + received messages", "type": "int"},
        "min_balance_ratio": {"default": 0.35, "description": "Minimum balance ratio (min/max of sent vs received)", "type": "float"},
        "silence_days": {"default": 60, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        sent = sum(
            1
            for ix in interactions
            if ix["interaction_type"] in ("message_sent", "email_sent")
        )
        received = sum(
            1
            for ix in interactions
            if ix["interaction_type"] in ("message_received", "email_received")
        )

        total = sent + received
        if total < self.min_messages:
            return None

        # Check balance: ideal is 50/50
        ratio = min(sent, received) / max(sent, received) if max(sent, received) > 0 else 0
        if ratio < self.min_balance_ratio:
            return None

        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)
        sent_pct = int(sent / total * 100)

        narrative = (
            f"You and {contact_name} had balanced conversations - "
            f"you initiated {sent_pct}% and they initiated {100 - sent_pct}% of the time. "
            f"That kind of mutual effort is rare. "
            f"You haven't been in touch for {_format_days(days_since)}."
        )

        score = 2.0 * ratio * min(days_since / 365, 3.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "sent": sent,
                "received": received,
                "ratio": ratio,
                "days_since_last": days_since,
            },
        )


class SeasonalFriend(PatternRule):
    """Detect contacts you interacted with at the same time each year."""

    rule_id = "seasonal_friend"
    name = "Seasonal Friend"
    description = "Detects contacts you used to catch up with at the same time each year — a broken seasonal tradition."
    min_per_month = 3
    min_years = 3
    years_broken = 2
    silence_days = 90
    parameters = {
        "min_per_month": {"default": 3, "description": "Minimum interactions per month to count", "type": "int"},
        "min_years": {"default": 3, "description": "Minimum different years with pattern", "type": "int"},
        "years_broken": {"default": 2, "description": "Years since last pattern before triggering", "type": "int"},
        "silence_days": {"default": 90, "description": "Days of silence before triggering", "type": "int"},
    }

    def detect(self, contact_id, interactions, conn):
        if len(interactions) < 20:
            return None

        # Group interactions by (year, month)
        monthly = defaultdict(int)
        for ix in interactions:
            try:
                dt = datetime.fromisoformat(ix["occurred_at"])
                monthly[(dt.year, dt.month)] += 1
            except (ValueError, KeyError):
                continue

        if not monthly:
            return None

        # For each month (1-12), check how many different years it appears in
        month_years = defaultdict(set)
        for (year, month), count in monthly.items():
            if count >= self.min_per_month:
                month_years[month].add(year)

        # Find months that appear in min_years+ different years
        seasonal_months = [
            (month, years)
            for month, years in month_years.items()
            if len(years) >= self.min_years
        ]

        if not seasonal_months:
            return None

        # Pick the month with the most years
        seasonal_months.sort(key=lambda x: len(x[1]), reverse=True)
        best_month, best_years = seasonal_months[0]

        # Check if the pattern has been broken
        sorted_years = sorted(best_years)
        last_pattern_year = sorted_years[-1]
        current_year = date.today().year

        if current_year - last_pattern_year < self.years_broken:
            return None  # Pattern is still active

        last_ix = interactions[-1]
        try:
            last_date = datetime.fromisoformat(last_ix["occurred_at"]).date()
        except (ValueError, KeyError):
            return None

        days_since = (date.today() - last_date).days
        if days_since < self.silence_days:
            return None

        contact_name = _get_contact_name(conn, contact_id)
        import calendar as cal_mod

        month_name = cal_mod.month_name[best_month]
        year_list = ", ".join(str(y) for y in sorted_years)

        narrative = (
            f"You and {contact_name} used to catch up every {month_name} "
            f"({year_list}). You broke the tradition in {last_pattern_year + 1}."
        )

        score = len(best_years) * 1.5 * min(days_since / 365, 2.0)

        return RuleMatch(
            rule_id=self.rule_id,
            contact_id=contact_id,
            score_contribution=score,
            narrative=narrative,
            match_data={
                "month": best_month,
                "month_name": month_name,
                "years": sorted_years,
                "last_pattern_year": last_pattern_year,
                "days_since_last": days_since,
            },
        )


# --- Helper functions ---


def _find_burst(
    interactions: list[dict], window_days: int, min_count: int
) -> tuple[int, date, date] | None:
    """Find the densest window of interactions.

    Returns (count, start_date, end_date) or None.
    """
    dates = []
    for ix in interactions:
        try:
            dates.append(datetime.fromisoformat(ix["occurred_at"]).date())
        except (ValueError, KeyError):
            continue

    if not dates:
        return None

    dates.sort()

    best_count = 0
    best_start = dates[0]
    best_end = dates[0]

    for i, start in enumerate(dates):
        window_end = start + timedelta(days=window_days)
        # Count interactions in this window
        count = sum(1 for d in dates[i:] if d <= window_end)
        if count > best_count:
            best_count = count
            best_start = start
            # Find the actual last date in the window
            best_end = max(d for d in dates[i:] if d <= window_end)

    if best_count >= min_count:
        return (best_count, best_start, best_end)

    return None


def _get_yearly_rank(
    conn: sqlite3.Connection, contact_id: int, year: int
) -> int | None:
    """Get a contact's rank among all contacts for a given year."""
    rows = conn.execute(
        """
        SELECT contact_id, COUNT(*) as cnt
        FROM interactions
        WHERE occurred_at >= ? AND occurred_at < ?
        GROUP BY contact_id
        ORDER BY cnt DESC
        """,
        (f"{year}-01-01", f"{year + 1}-01-01"),
    ).fetchall()

    for rank, row in enumerate(rows, 1):
        if row["contact_id"] == contact_id:
            return rank

    return None


def _get_contact_name(conn: sqlite3.Connection, contact_id: int) -> str:
    """Get the display name for a contact."""
    row = conn.execute(
        "SELECT display_name FROM contacts WHERE id = ?", (contact_id,)
    ).fetchone()
    return row["display_name"] if row else "Unknown"


def _format_days(days: int) -> str:
    """Format a number of days into a human-readable string."""
    if days < 30:
        return f"{days} days"
    elif days < 365:
        months = days // 30
        return f"{months} month{'s' if months != 1 else ''}"
    else:
        years = days // 365
        remaining_months = (days % 365) // 30
        if remaining_months > 0:
            return (
                f"{years} year{'s' if years != 1 else ''} and "
                f"{remaining_months} month{'s' if remaining_months != 1 else ''}"
            )
        return f"{years} year{'s' if years != 1 else ''}"


# Registry of all available rules
ALL_RULES: list[PatternRule] = [
    YearlyTopContact(),
    MeetingBurst(),
    RegularSyncs(),
    MessageBurst(),
    MultiChannel(),
    WeekendFriend(),
    ReciprocalInitiator(),
    SeasonalFriend(),
]
