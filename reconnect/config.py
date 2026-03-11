from __future__ import annotations

import os
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# Data directory (runtime data, gitignored) — overridable via env var
DATA_DIR = Path(os.environ.get("RECONNECT_DATA_DIR", PROJECT_ROOT / "data"))
DATA_DIR.mkdir(exist_ok=True)

# App database
DB_PATH = DATA_DIR / "reconnect.db"

# macOS data sources
IMESSAGE_DB_PATH = Path.home() / "Library" / "Messages" / "chat.db"
ADDRESSBOOK_DB_PATH = (
    Path.home()
    / "Library"
    / "Application Support"
    / "AddressBook"
    / "AddressBook-v22.abcddb"
)

# Interaction type weights for scoring
INTERACTION_WEIGHTS = {
    "calendar_event": 3.0,
    "email_sent": 2.0,
    "email_received": 1.5,
    "message_sent": 1.0,
    "message_received": 0.8,
}

# Suggestion settings
SUGGESTIONS_PER_BATCH = 10
MIN_INTERACTIONS_THRESHOLD = 5
ACTIVE_DAYS_THRESHOLD = 30  # contacts active within this many days are excluded
RECENT_SUGGESTION_MONTHS = 3  # don't re-suggest within this window
TOP_PICK_COUNT = 7  # guaranteed high-score picks
SURPRISE_PICK_COUNT = 3  # serendipity picks from next tier
SURPRISE_POOL_SIZE = 20  # pool size for surprise picks

# Feedback adjustments
FEEDBACK_YES_BOOST = 0.15
FEEDBACK_NO_PENALTY = -0.10
FEEDBACK_BOOST_MIN = -0.5
FEEDBACK_BOOST_MAX = 1.0

# Gmail settings
GMAIL_CREDENTIALS_PATH = DATA_DIR / "gmail_credentials.json"
GMAIL_TOKEN_PATH = DATA_DIR / "gmail_token.json"
GMAIL_BATCH_SIZE = 100  # messages per commit batch

# Default country for phone normalization
DEFAULT_PHONE_REGION = "US"
