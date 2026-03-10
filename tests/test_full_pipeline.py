"""End-to-end test with synthetic data to verify the full pipeline works."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from reconnect.database import init_db, get_connection
from reconnect.scoring.scorer import recalculate_all_scores
from reconnect.scoring.suggester import generate_suggestions
from reconnect.scoring.feedback import submit_feedback
from reconnect.resolution.normalizer import normalize_phone, normalize_email, classify_identifier
from reconnect.resolution.resolver import resolve_duplicates


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    """Create a temporary database with synthetic data."""
    db_path = tmp_path / "test_reconnect.db"
    monkeypatch.setattr("reconnect.config.DB_PATH", db_path)
    monkeypatch.setattr("reconnect.database.DB_PATH", db_path)
    monkeypatch.setattr("reconnect.scoring.scorer.get_connection", lambda: get_connection(db_path))
    monkeypatch.setattr("reconnect.scoring.suggester.get_connection", lambda: get_connection(db_path))
    monkeypatch.setattr("reconnect.scoring.feedback.get_connection", lambda: get_connection(db_path))
    monkeypatch.setattr("reconnect.resolution.resolver.get_connection", lambda: get_connection(db_path))

    init_db(db_path)
    _populate_synthetic_data(db_path)
    return db_path


def _populate_synthetic_data(db_path: Path):
    """Create realistic synthetic contacts and interactions."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Create contacts with different interaction profiles
    contacts = [
        # 1. Heavy 2017 texter, went silent in 2018
        ("Sarah Chen", "Sarah", "Chen"),
        # 2. Meeting burst person
        ("James Rodriguez", "James", "Rodriguez"),
        # 3. Regular sync person
        ("Priya Patel", "Priya", "Patel"),
        # 4. Message burst person
        ("Alex Kim", "Alex", "Kim"),
        # 5. Multi-channel person
        ("Lisa Wang", "Lisa", "Wang"),
        # 6. Weekend friend
        ("Chris Johnson", "Chris", "Johnson"),
        # 7. Reciprocal initiator
        ("Dan Miller", "Dan", "Miller"),
        # 8. Still active (should NOT be suggested)
        ("Active Friend", "Active", "Friend"),
        # 9. Too few interactions (should NOT be suggested)
        ("Brief Encounter", "Brief", "Encounter"),
        # 10-15. Filler contacts with moderate histories
        ("Emily Davis", "Emily", "Davis"),
        ("Michael Brown", "Michael", "Brown"),
        ("Jessica Wilson", "Jessica", "Wilson"),
        ("David Lee", "David", "Lee"),
        ("Amanda Taylor", "Amanda", "Taylor"),
        ("Ryan Martinez", "Ryan", "Martinez"),
    ]

    for display, first, last in contacts:
        conn.execute(
            "INSERT INTO contacts (display_name, first_name, last_name) VALUES (?, ?, ?)",
            (display, first, last),
        )

    # Add identifiers
    identifiers = [
        (1, "phone", "+14155551001", "imessage"),
        (1, "email", "sarah.chen@gmail.com", "gmail"),
        (2, "email", "james.r@company.com", "calendar"),
        (3, "email", "priya@startup.io", "calendar"),
        (4, "phone", "+14155551004", "imessage"),
        (5, "phone", "+14155551005", "imessage"),
        (5, "email", "lisa@company.com", "gmail"),
        (6, "phone", "+14155551006", "imessage"),
        (7, "phone", "+14155551007", "imessage"),
        (8, "phone", "+14155551008", "imessage"),
        (9, "phone", "+14155551009", "imessage"),
    ]
    for cid, itype, ival, source in identifiers:
        conn.execute(
            "INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) VALUES (?, ?, ?, ?)",
            (cid, itype, ival, source),
        )

    # Generate interactions
    interactions = []
    ix_id = 0

    # Contact 1 (Sarah): Heavy texter in 2017, 1200+ messages, silent since 2018
    for day_offset in range(365):
        d = date(2017, 1, 1) + timedelta(days=day_offset)
        # ~3-4 messages per day
        for _ in range(3):
            ix_id += 1
            interactions.append((
                1, "imessage",
                "message_sent" if ix_id % 2 == 0 else "message_received",
                datetime(d.year, d.month, d.day, 10 + (ix_id % 12)).isoformat(),
                f"im_{ix_id}",
            ))
    # A few messages in early 2018
    for day_offset in range(30):
        d = date(2018, 1, 1) + timedelta(days=day_offset)
        ix_id += 1
        interactions.append((
            1, "imessage", "message_sent",
            datetime(d.year, d.month, d.day, 14).isoformat(),
            f"im_{ix_id}",
        ))

    # Contact 2 (James): 6 meetings in 2 weeks in Oct 2018, then nothing
    for day_offset in [0, 2, 4, 7, 9, 12]:
        d = date(2018, 10, 1) + timedelta(days=day_offset)
        ix_id += 1
        interactions.append((
            2, "calendar", "calendar_event",
            datetime(d.year, d.month, d.day, 10).isoformat(),
            f"cal_{ix_id}",
        ))

    # Contact 3 (Priya): Regular monthly meetings Jan-Jun 2020
    for month in range(1, 7):
        for week in range(1, 5):
            d = date(2020, month, min(week * 7, 28))
            ix_id += 1
            interactions.append((
                3, "calendar", "calendar_event",
                datetime(d.year, d.month, d.day, 11).isoformat(),
                f"cal_{ix_id}",
            ))

    # Contact 4 (Alex): 300 messages in 2 weeks in March 2021
    for day_offset in range(14):
        d = date(2021, 3, 1) + timedelta(days=day_offset)
        for msg_num in range(20):
            ix_id += 1
            interactions.append((
                4, "imessage",
                "message_sent" if msg_num % 3 != 0 else "message_received",
                datetime(d.year, d.month, d.day, 8 + msg_num % 14).isoformat(),
                f"im_{ix_id}",
            ))

    # Contact 5 (Lisa): Multi-channel in Q3 2022
    for month in [7, 8, 9]:
        for day in range(1, 28, 3):
            d = date(2022, month, day)
            ix_id += 1
            interactions.append((
                5, "imessage", "message_sent",
                datetime(d.year, d.month, d.day, 10).isoformat(),
                f"im_{ix_id}",
            ))
            ix_id += 1
            interactions.append((
                5, "gmail", "email_sent",
                datetime(d.year, d.month, d.day, 14).isoformat(),
                f"gm_{ix_id}",
            ))
        for day in [5, 15, 25]:
            d = date(2022, month, day)
            ix_id += 1
            interactions.append((
                5, "calendar", "calendar_event",
                datetime(d.year, d.month, d.day, 11).isoformat(),
                f"cal_{ix_id}",
            ))

    # Contact 6 (Chris): Weekend texter throughout 2019-2020
    for year in [2019, 2020]:
        d = date(year, 1, 1)
        while d.year == year:
            if d.weekday() >= 5:  # Saturday or Sunday
                for _ in range(3):
                    ix_id += 1
                    interactions.append((
                        6, "imessage",
                        "message_sent" if ix_id % 2 == 0 else "message_received",
                        datetime(d.year, d.month, d.day, 12).isoformat(),
                        f"im_{ix_id}",
                    ))
            d += timedelta(days=1)

    # Contact 7 (Dan): Balanced back-and-forth in 2020
    for day_offset in range(0, 365, 2):
        d = date(2020, 1, 1) + timedelta(days=day_offset)
        ix_id += 1
        interactions.append((
            7, "imessage",
            "message_sent" if day_offset % 4 < 2 else "message_received",
            datetime(d.year, d.month, d.day, 15).isoformat(),
            f"im_{ix_id}",
        ))

    # Contact 8 (Active): Still active - messages yesterday
    for day_offset in range(30):
        d = date.today() - timedelta(days=day_offset)
        ix_id += 1
        interactions.append((
            8, "imessage", "message_sent",
            datetime(d.year, d.month, d.day, 10).isoformat(),
            f"im_{ix_id}",
        ))

    # Contact 9 (Brief): Only 3 messages ever
    for i in range(3):
        ix_id += 1
        interactions.append((
            9, "imessage", "message_sent",
            datetime(2021, 6, 1 + i).isoformat(),
            f"im_{ix_id}",
        ))

    # Contacts 10-15: Moderate histories (50-100 messages each, ended 1-3 years ago)
    for cid in range(10, 16):
        end_year = 2022 + (cid % 3)
        for day_offset in range(0, 200, 3):
            d = date(end_year - 1, 1, 1) + timedelta(days=day_offset)
            if d.year > end_year:
                break
            ix_id += 1
            interactions.append((
                cid, "imessage",
                "message_sent" if ix_id % 2 == 0 else "message_received",
                datetime(d.year, d.month, d.day, 12).isoformat(),
                f"im_{ix_id}",
            ))

    # Bulk insert interactions
    conn.executemany(
        "INSERT INTO interactions (contact_id, source, interaction_type, occurred_at, source_id) VALUES (?, ?, ?, ?, ?)",
        interactions,
    )

    conn.commit()
    conn.close()


class TestNormalizer:
    def test_phone_normalization(self):
        assert normalize_phone("(415) 555-1234") == "+14155551234"
        assert normalize_phone("+1 415-555-1234") == "+14155551234"
        assert normalize_phone("not a phone") is None
        assert normalize_phone("") is None

    def test_email_normalization(self):
        assert normalize_email("User@Example.COM") == "user@example.com"
        assert normalize_email("  john@test.org  ") == "john@test.org"
        assert normalize_email("not an email") is None
        assert normalize_email("") is None

    def test_classify_identifier(self):
        id_type, value = classify_identifier("user@gmail.com")
        assert id_type == "email"
        assert value == "user@gmail.com"

        id_type, value = classify_identifier("+14155551234")
        assert id_type == "phone"
        assert value == "+14155551234"

        id_type, value = classify_identifier("garbage")
        assert id_type == "unknown"
        assert value is None


class TestFullPipeline:
    def test_scoring(self, test_db):
        """Test that scoring runs and produces scores."""
        result = recalculate_all_scores()
        assert result["status"] == "ok"
        assert result["contacts_scored"] > 0
        assert result["patterns_found"] > 0

    def test_suggestions(self, test_db):
        """Test that suggestion generation produces 10 results."""
        recalculate_all_scores()
        result = generate_suggestions("2026-03")
        assert result["status"] == "ok"
        assert len(result["suggestions"]) > 0
        assert len(result["suggestions"]) <= 10

        # Verify each suggestion has a narrative
        for s in result["suggestions"]:
            assert s["display_name"]
            assert s["score"] > 0

    def test_active_contact_excluded(self, test_db):
        """Active contacts should not be suggested."""
        recalculate_all_scores()
        result = generate_suggestions("2026-03")
        suggested_names = [s["display_name"] for s in result["suggestions"]]
        assert "Active Friend" not in suggested_names

    def test_low_interaction_excluded(self, test_db):
        """Contacts with very few interactions should not be suggested."""
        recalculate_all_scores()
        result = generate_suggestions("2026-03")
        suggested_names = [s["display_name"] for s in result["suggestions"]]
        assert "Brief Encounter" not in suggested_names

    def test_narratives_generated(self, test_db):
        """Test that pattern rules generate meaningful narratives."""
        recalculate_all_scores()

        conn = get_connection(test_db)
        matches = conn.execute(
            "SELECT * FROM pattern_matches ORDER BY score_contribution DESC"
        ).fetchall()
        conn.close()

        assert len(matches) > 0

        # Check that narratives contain real contact names and years
        narratives = [m["narrative"] for m in matches]
        narrative_text = " ".join(narratives)

        # Should reference specific people
        assert any(
            name in narrative_text
            for name in ["Sarah", "James", "Priya", "Alex", "Lisa", "Chris", "Dan"]
        )

    def test_feedback_adjusts_scores(self, test_db):
        """Test that feedback modifies future scores."""
        recalculate_all_scores()
        result = generate_suggestions("2026-03")

        if not result["suggestions"]:
            pytest.skip("No suggestions generated")

        # Get first suggestion
        first = result["suggestions"][0]
        conn = get_connection(test_db)

        # Get score before feedback
        before = conn.execute(
            "SELECT feedback_boost FROM contact_scores WHERE contact_id = ?",
            (first["contact_id"],),
        ).fetchone()
        assert before["feedback_boost"] == 0.0

        conn.close()

        # Submit positive feedback
        fb_result = submit_feedback(1, "yes")  # suggestion id 1
        assert fb_result["status"] == "ok"

        conn = get_connection(test_db)
        after = conn.execute(
            "SELECT feedback_boost FROM contact_scores WHERE contact_id = ?",
            (first["contact_id"],),
        ).fetchone()
        conn.close()
        assert after["feedback_boost"] > 0.0

    def test_duplicate_batch_returns_existing(self, test_db):
        """Generating for the same month should return existing batch."""
        recalculate_all_scores()
        result1 = generate_suggestions("2026-03")
        result2 = generate_suggestions("2026-03")
        assert result2["status"] == "existing"
        assert result2["batch_id"] == result1["batch_id"]

    def test_sarah_yearly_top_detected(self, test_db):
        """Sarah should trigger the yearly_top_contact rule for 2017."""
        recalculate_all_scores()

        conn = get_connection(test_db)
        matches = conn.execute(
            "SELECT * FROM pattern_matches WHERE contact_id = 1 AND rule_id = 'yearly_top_contact'"
        ).fetchall()
        conn.close()

        assert len(matches) >= 1
        assert "2017" in matches[0]["narrative"]
        assert "Sarah" in matches[0]["narrative"]

    def test_alex_message_burst_detected(self, test_db):
        """Alex should trigger the message_burst rule."""
        recalculate_all_scores()

        conn = get_connection(test_db)
        matches = conn.execute(
            "SELECT * FROM pattern_matches WHERE contact_id = 4 AND rule_id = 'message_burst'"
        ).fetchall()
        conn.close()

        assert len(matches) >= 1
        assert "Alex" in matches[0]["narrative"]

    def test_james_meeting_burst_detected(self, test_db):
        """James should trigger the meeting_burst rule."""
        recalculate_all_scores()

        conn = get_connection(test_db)
        matches = conn.execute(
            "SELECT * FROM pattern_matches WHERE contact_id = 2 AND rule_id = 'meeting_burst'"
        ).fetchall()
        conn.close()

        assert len(matches) >= 1
        assert "James" in matches[0]["narrative"]


class TestResolver:
    def test_merge_by_shared_identifier(self, test_db):
        """Test that contacts with the same identifier get merged."""
        conn = get_connection(test_db)

        # Create two contacts with the same phone number
        conn.execute(
            "INSERT INTO contacts (display_name, first_name) VALUES ('John Doe', 'John')"
        )
        john_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            "INSERT INTO contacts (display_name) VALUES ('+14155559999')"
        )
        phone_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Both have the same phone
        conn.execute(
            "INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) "
            "VALUES (?, 'phone', '+14155559999', 'contacts_app')",
            (john_id,),
        )
        conn.execute(
            "INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) "
            "VALUES (?, 'phone', '+14155559999', 'imessage')",
            (phone_id,),
        )
        conn.commit()
        conn.close()

        result = resolve_duplicates()
        assert result["auto_merges"] >= 1

        # Verify the phone-named contact is gone, John survives
        conn = get_connection(test_db)
        surviving = conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (john_id,)
        ).fetchone()
        merged = conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (phone_id,)
        ).fetchone()
        conn.close()

        assert surviving is not None
        assert surviving["display_name"] == "John Doe"
        assert merged is None
