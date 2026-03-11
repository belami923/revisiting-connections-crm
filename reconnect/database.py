"""Database schema initialization and connection management."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from reconnect.config import DB_PATH

SCHEMA_SQL = """
-- Unified contact record
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    display_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    is_excluded INTEGER DEFAULT 0,
    notes TEXT,
    linkedin_url TEXT,
    twitter_url TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- A contact can have multiple identifiers (phones, emails)
CREATE TABLE IF NOT EXISTS contact_identifiers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL REFERENCES contacts(id),
    identifier_type TEXT NOT NULL,
    identifier_value TEXT NOT NULL,
    source TEXT NOT NULL,
    UNIQUE(identifier_type, identifier_value, source)
);
CREATE INDEX IF NOT EXISTS idx_ci_contact ON contact_identifiers(contact_id);
CREATE INDEX IF NOT EXISTS idx_ci_lookup ON contact_identifiers(identifier_type, identifier_value);

-- Unified interaction log
CREATE TABLE IF NOT EXISTS interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL REFERENCES contacts(id),
    source TEXT NOT NULL,
    interaction_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source_id TEXT,
    metadata_json TEXT,
    UNIQUE(source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_interactions_contact ON interactions(contact_id);
CREATE INDEX IF NOT EXISTS idx_interactions_time ON interactions(occurred_at);
CREATE INDEX IF NOT EXISTS idx_interactions_contact_time ON interactions(contact_id, occurred_at);

-- Precomputed scores
CREATE TABLE IF NOT EXISTS contact_scores (
    contact_id INTEGER PRIMARY KEY REFERENCES contacts(id),
    total_interactions INTEGER DEFAULT 0,
    peak_density REAL DEFAULT 0,
    peak_start TEXT,
    peak_end TEXT,
    last_interaction_at TEXT,
    days_since_last INTEGER DEFAULT 0,
    decay_score REAL DEFAULT 0,
    suggestion_score REAL DEFAULT 0,
    feedback_boost REAL DEFAULT 0,
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Pattern matches detected by rule engine
CREATE TABLE IF NOT EXISTS pattern_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL REFERENCES contacts(id),
    rule_id TEXT NOT NULL,
    narrative TEXT NOT NULL,
    score_contribution REAL NOT NULL,
    match_data_json TEXT,
    detected_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pm_contact ON pattern_matches(contact_id);

-- Monthly suggestion batches
CREATE TABLE IF NOT EXISTS suggestion_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month_label TEXT NOT NULL,
    generated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES suggestion_batches(id),
    contact_id INTEGER NOT NULL REFERENCES contacts(id),
    rank INTEGER NOT NULL,
    score_at_time REAL NOT NULL,
    primary_rule_id TEXT,
    primary_narrative TEXT,
    all_narratives_json TEXT,
    feedback TEXT,
    feedback_at TEXT,
    notes TEXT,
    enrichment_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_suggestions_batch ON suggestions(batch_id);
CREATE INDEX IF NOT EXISTS idx_suggestions_contact ON suggestions(contact_id);

-- Sync tracking
CREATE TABLE IF NOT EXISTS ingestion_state (
    source TEXT PRIMARY KEY,
    last_synced_at TEXT,
    watermark TEXT,
    status TEXT DEFAULT 'idle',
    error_message TEXT
);

-- Custom lists
CREATE TABLE IF NOT EXISTS custom_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    is_auto INTEGER DEFAULT 0,
    auto_rule TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS list_memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id INTEGER NOT NULL REFERENCES custom_lists(id) ON DELETE CASCADE,
    contact_id INTEGER NOT NULL REFERENCES contacts(id),
    added_at TEXT DEFAULT (datetime('now')),
    UNIQUE(list_id, contact_id)
);
CREATE INDEX IF NOT EXISTS idx_lm_list ON list_memberships(list_id);
CREATE INDEX IF NOT EXISTS idx_lm_contact ON list_memberships(contact_id);

-- Enrichment cache
CREATE TABLE IF NOT EXISTS enrichment_cache (
    contact_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    data_json TEXT NOT NULL,
    fetched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (contact_id, source)
);

-- User settings (key-value store for rule overrides, weights, etc.)
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);
"""


_USE_MEMORY_DB = False


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a connection to the app database."""
    if _USE_MEMORY_DB:
        conn = sqlite3.connect(":memory:")
    else:
        path = db_path or DB_PATH
        conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path | None = None) -> None:
    """Initialize the database schema."""
    global _USE_MEMORY_DB
    try:
        conn = get_connection(db_path)
    except sqlite3.DatabaseError:
        # Sandboxed environments may block SQLite; fall back to in-memory DB
        _USE_MEMORY_DB = True
        conn = get_connection()
    try:
        conn.executescript(SCHEMA_SQL)
        # Migrations for columns added after initial schema creation
        for col in ("linkedin_url TEXT", "twitter_url TEXT", "skip_until TEXT"):
            try:
                conn.execute(f"ALTER TABLE contacts ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass  # column already exists
        # Add columns to suggestions for notes and two-stage reach-out tracking
        for col in ("notes TEXT", "reached_out_at TEXT"):
            try:
                conn.execute(f"ALTER TABLE suggestions ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


def get_readonly_connection(db_path: Path) -> sqlite3.Connection:
    """Get a read-only connection (for iMessage, AddressBook, etc.)."""
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn
