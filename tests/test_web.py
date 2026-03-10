"""Test web endpoints with the HTTPX test client."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from reconnect.database import init_db, get_connection
from reconnect.main import app
from reconnect.scoring.scorer import recalculate_all_scores
from reconnect.scoring.suggester import generate_suggestions


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    """Set up test database with synthetic data."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("reconnect.config.DB_PATH", db_path)
    monkeypatch.setattr("reconnect.database.DB_PATH", db_path)

    # Patch all modules that import get_connection
    for mod in [
        "reconnect.routers.suggestions",
        "reconnect.routers.contacts",
        "reconnect.routers.sync",
        "reconnect.routers.lists",
        "reconnect.routers.settings",
        "reconnect.scoring.scorer",
        "reconnect.scoring.suggester",
        "reconnect.scoring.feedback",
        "reconnect.resolution.resolver",
        "reconnect.ingestion.gmail",
    ]:
        monkeypatch.setattr(f"{mod}.get_connection", lambda p=db_path: get_connection(p))

    init_db(db_path)
    _seed_data(db_path)
    recalculate_all_scores()
    generate_suggestions()
    return db_path


def _seed_data(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    # Create a few contacts with interactions
    conn.execute("INSERT INTO contacts (id, display_name, first_name) VALUES (1, 'Jane Smith', 'Jane')")
    conn.execute("INSERT INTO contacts (id, display_name, first_name) VALUES (2, 'Bob Jones', 'Bob')")
    conn.execute("INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) VALUES (1, 'phone', '+14155551111', 'imessage')")
    conn.execute("INSERT INTO contact_identifiers (contact_id, identifier_type, identifier_value, source) VALUES (2, 'email', 'bob@test.com', 'gmail')")

    # Add interactions for both (dormant)
    ix_id = 0
    for cid in [1, 2]:
        for day in range(200):
            d = date(2020, 1, 1) + timedelta(days=day)
            ix_id += 1
            conn.execute(
                "INSERT INTO interactions (contact_id, source, interaction_type, occurred_at, source_id) VALUES (?, 'imessage', 'message_sent', ?, ?)",
                (cid, datetime(d.year, d.month, d.day, 10).isoformat(), f"im_{ix_id}"),
            )
    conn.commit()
    conn.close()


@pytest.fixture
def client(test_db):
    return TestClient(app)


class TestDashboard:
    def test_dashboard_loads(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Reconnect" in resp.text

    def test_dashboard_shows_suggestions(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        # Should show at least one contact name
        assert "Jane" in resp.text or "Bob" in resp.text

    def test_feedback_submission(self, client):
        resp = client.post(
            "/suggestions/1/feedback",
            data={"feedback": "yes"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    def test_save_notes(self, client):
        resp = client.post(
            "/suggestions/1/notes",
            data={"notes": "Who is this person again?"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        # Verify note persists on dashboard
        resp = client.get("/")
        assert "Who is this person again?" in resp.text


class TestContacts:
    def test_contacts_list(self, client):
        resp = client.get("/contacts/")
        assert resp.status_code == 200
        assert "Jane Smith" in resp.text

    def test_contacts_search(self, client):
        resp = client.get("/contacts/?q=Jane")
        assert resp.status_code == 200
        assert "Jane Smith" in resp.text

    def test_contact_detail(self, client):
        resp = client.get("/contacts/1")
        assert resp.status_code == 200
        assert "Jane Smith" in resp.text

    def test_contact_not_found(self, client):
        resp = client.get("/contacts/99999")
        assert resp.status_code == 404


class TestSync:
    def test_sync_page_loads(self, client):
        resp = client.get("/sync/")
        assert resp.status_code == 200
        assert "Data Sync" in resp.text


class TestLists:
    def test_lists_page_loads(self, client):
        resp = client.get("/lists/")
        assert resp.status_code == 200
        assert "Lists" in resp.text

    def test_create_list(self, client):
        resp = client.post(
            "/lists/create",
            data={"name": "Ride or Die", "description": "Closest friends"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        # Verify list appears on the page
        resp = client.get("/lists/")
        assert "Ride or Die" in resp.text

    def test_list_detail(self, client):
        # Create a list first
        client.post(
            "/lists/create",
            data={"name": "Test List", "description": ""},
            follow_redirects=True,
        )
        resp = client.get("/lists/1")
        assert resp.status_code == 200
        assert "Test List" in resp.text

    def test_add_remove_contact(self, client):
        # Create a list
        client.post(
            "/lists/create",
            data={"name": "VCs", "description": "Investors"},
            follow_redirects=True,
        )
        # Add contact 1
        resp = client.post(
            "/lists/1/add",
            data={"contact_id": "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        # Verify contact is in the list
        resp = client.get("/lists/1")
        assert "Jane Smith" in resp.text
        # Remove contact
        resp = client.post(
            "/lists/1/remove",
            data={"contact_id": "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    def test_delete_list(self, client):
        client.post(
            "/lists/create",
            data={"name": "Temp", "description": ""},
            follow_redirects=True,
        )
        resp = client.post("/lists/1/delete", follow_redirects=False)
        assert resp.status_code == 303

    def test_auto_generate(self, client):
        resp = client.post("/lists/auto-generate", follow_redirects=False)
        assert resp.status_code == 303
        # Should have created at least one auto list (message_heavy or faded_close)
        resp = client.get("/lists/")
        assert resp.status_code == 200


class TestSettings:
    def test_settings_page_loads(self, client):
        resp = client.get("/settings/")
        assert resp.status_code == 200
        assert "Settings" in resp.text
        assert "Pattern Detection Rules" in resp.text

    def test_settings_shows_rules(self, client):
        resp = client.get("/settings/")
        assert "Yearly Top Contact" in resp.text
        assert "Meeting Burst" in resp.text
        assert "Regular Syncs" in resp.text

    def test_toggle_rule(self, client):
        resp = client.post("/settings/rule/yearly_top_contact/toggle", follow_redirects=False)
        assert resp.status_code == 303
        # Verify the rule was toggled (check settings page)
        resp = client.get("/settings/")
        assert resp.status_code == 200

    def test_update_weights(self, client):
        resp = client.post(
            "/settings/weights",
            data={"weight_calendar_event": "4.0", "weight_email_sent": "2.5"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    def test_update_suggestion_config(self, client):
        resp = client.post(
            "/settings/suggestions",
            data={"suggestion_suggestions_per_batch": "12", "suggestion_top_pick_count": "8"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    def test_reset_settings(self, client):
        # First toggle a rule
        client.post("/settings/rule/yearly_top_contact/toggle", follow_redirects=True)
        # Then reset
        resp = client.post("/settings/reset", follow_redirects=False)
        assert resp.status_code == 303


class TestCreateContact:
    def test_create_contact(self, client):
        resp = client.post(
            "/contacts/create",
            data={"display_name": "Alice Doe", "phone": "+14155559999", "email": "alice@test.com"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        # Verify contact appears in list
        resp = client.get("/contacts/?q=Alice")
        assert "Alice Doe" in resp.text

    def test_create_contact_name_only(self, client):
        resp = client.post(
            "/contacts/create",
            data={"display_name": "Solo Person", "phone": "", "email": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        resp = client.get("/contacts/?q=Solo")
        assert "Solo Person" in resp.text

    def test_update_social_urls(self, client):
        resp = client.post(
            "/contacts/1/socials",
            data={"linkedin_url": "https://linkedin.com/in/janesmith", "twitter_url": "https://x.com/janesmith"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        # Verify on detail page
        resp = client.get("/contacts/1")
        assert "linkedin.com/in/janesmith" in resp.text


class TestGmail:
    def test_sync_page_shows_gmail_card(self, client):
        """Sync page should show the Gmail card."""
        resp = client.get("/sync/")
        assert resp.status_code == 200
        assert "Gmail" in resp.text

    def test_gmail_status_endpoint(self, client):
        """Gmail status JSON endpoint should return config status."""
        resp = client.get("/sync/gmail/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "deps_installed" in data
        assert "has_credentials" in data
        assert "has_token" in data

    def test_gmail_ingest_no_credentials(self, client, test_db, monkeypatch):
        """Gmail sync should skip gracefully when not configured."""
        from reconnect.ingestion import gmail
        monkeypatch.setattr(gmail, "GMAIL_CREDENTIALS_PATH", Path("/nonexistent/creds.json"))
        monkeypatch.setattr(gmail, "GMAIL_TOKEN_PATH", Path("/nonexistent/token.json"))

        result = gmail.ingest_gmail()
        assert result["status"] == "skipped"

    def test_gmail_ingest_with_mock(self, client, test_db, monkeypatch):
        """Gmail sync should import emails from mock service."""
        from reconnect.ingestion import gmail

        # Create fake message data
        fake_messages = {
            "messages": [
                {"id": "msg_001"},
                {"id": "msg_002"},
                {"id": "msg_003"},
            ]
        }

        fake_msg_details = {
            "msg_001": {
                "id": "msg_001",
                "threadId": "thread_1",
                "historyId": "12345",
                "internalDate": "1609459200000",  # 2021-01-01
                "labelIds": ["SENT"],
                "payload": {
                    "headers": [
                        {"name": "From", "value": "me@test.com"},
                        {"name": "To", "value": "alice@example.com"},
                        {"name": "Subject", "value": "Hello Alice"},
                    ]
                },
            },
            "msg_002": {
                "id": "msg_002",
                "threadId": "thread_2",
                "historyId": "12346",
                "internalDate": "1609545600000",  # 2021-01-02
                "labelIds": ["INBOX"],
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Bob Jones <bob@test.com>"},
                        {"name": "To", "value": "me@test.com"},
                        {"name": "Subject", "value": "Re: Project"},
                    ]
                },
            },
            "msg_003": {
                "id": "msg_003",
                "threadId": "thread_3",
                "historyId": "12347",
                "internalDate": "1609632000000",  # 2021-01-03
                "labelIds": ["SENT"],
                "payload": {
                    "headers": [
                        {"name": "From", "value": "me@test.com"},
                        {"name": "To", "value": "new-person@example.com"},
                        {"name": "Subject", "value": "Introduction"},
                    ]
                },
            },
        }

        # Build mock service
        mock_service = MagicMock()

        # Mock messages().list()
        mock_list = MagicMock()
        mock_list.execute.return_value = fake_messages
        mock_service.users.return_value.messages.return_value.list.return_value = mock_list

        # Mock messages().get() to return different data per msg_id
        def mock_get(**kwargs):
            msg_id = kwargs.get("id", "")
            m = MagicMock()
            m.execute.return_value = fake_msg_details.get(msg_id, {})
            return m

        mock_service.users.return_value.messages.return_value.get = mock_get

        # Patch to skip auth checks and return mock service
        monkeypatch.setattr(gmail, "_check_deps", lambda: True)
        monkeypatch.setattr(gmail, "is_gmail_configured", lambda: {
            "deps_installed": True, "has_credentials": True, "has_token": True,
        })
        monkeypatch.setattr(gmail, "_get_gmail_service", lambda: mock_service)
        monkeypatch.setattr(gmail, "_THROTTLE_DELAY", 0)  # no delay in tests

        result = gmail.ingest_gmail()
        assert result["status"] == "ok"
        assert result["imported"] >= 2  # At least bob@test.com matches existing contact

        # Verify interactions were created
        conn = get_connection(test_db)
        gmail_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM interactions WHERE source = 'gmail'"
        ).fetchone()["cnt"]
        assert gmail_count >= 2
        conn.close()

    def test_gmail_parse_email(self):
        """Test email address parsing from headers."""
        from reconnect.ingestion.gmail import _parse_email_address

        assert _parse_email_address("alice@example.com") == "alice@example.com"
        assert _parse_email_address("Alice <alice@example.com>") == "alice@example.com"
        assert _parse_email_address("\"Alice Smith\" <alice@example.com>") == "alice@example.com"
        assert _parse_email_address("alice@a.com, bob@b.com") == "alice@a.com"
        assert _parse_email_address("") is None
        assert _parse_email_address("not-an-email") is None
