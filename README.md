# Reconnect

A personal CRM that analyzes your iMessage, Gmail, and Apple Calendar history to suggest people you should reconnect with. Built with FastAPI, Jinja2, and SQLite.

Reconnect looks at your real communication patterns — who you've texted, emailed, and met with — and surfaces the contacts you've drifted from but might want to reach out to. It combines a scoring algorithm with serendipity picks so you don't just see the obvious choices.

> **macOS only** — Reconnect reads from your local iMessage and Apple Calendar databases, which are only available on macOS.

## Features

- **Smart suggestions** — Scores contacts based on interaction frequency, recency, and communication patterns. Each monthly batch includes 7 high-score picks and 3 surprise/serendipity picks.
- **Two-stage reach-out flow** — Mark someone as "Reaching Out," then confirm with "I reached out" when you do. Contacts move through auto-managed lists so you can track your progress.
- **Conversation context** — See recent iMessage texts, email subjects, and calendar events right on the suggestion card so you remember what you last talked about.
- **Multi-source sync** — Imports contacts from Apple Contacts, messages from iMessage (with full text extraction), emails from Gmail (via OAuth), and events from Apple Calendar.
- **Contact resolution** — Automatically detects and merges duplicate contacts by normalizing phone numbers and email addresses.
- **Social enrichment** — Optionally looks up LinkedIn and Twitter/X profiles for suggested contacts via Brave Search.
- **Custom lists** — Create your own contact groupings. The reach-out workflow auto-manages "Reaching Out" and "Reached Out" lists.
- **Configurable scoring rules** — Tune interaction weights, silence thresholds, suggestion counts, and pattern detection rules from the Rules page.
- **Skip & snooze** — Snooze contacts for 3/6/12 months or permanently exclude them. Manage excluded contacts from the Never Show page.

## Screenshots

*Coming soon*

## Setup

### Prerequisites

- **macOS** (required for iMessage and Calendar access)
- **Python 3.9+**
- **Full Disk Access** for your terminal app (System Settings > Privacy & Security > Full Disk Access) — required to read the iMessage database

### Install

```bash
git clone https://github.com/belami923/revisiting-connections-crm.git
cd revisiting-connections-crm

python -m venv .venv
source .venv/bin/activate

# Install with all optional dependencies
pip install -e ".[all]"
```

You can also install only the extras you need:

```bash
pip install -e "."                   # Core only (iMessage + Apple Contacts)
pip install -e ".[gmail]"            # + Gmail integration
pip install -e ".[calendar]"         # + Apple Calendar
pip install -e ".[enrichment]"       # + Social profile lookups
pip install -e ".[dev]"              # + Tests
```

### Gmail setup (optional)

1. Create a project in [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the Gmail API
3. Create OAuth 2.0 credentials (Desktop app type)
4. Download the credentials JSON and save it as `data/gmail_credentials.json`
5. On first Gmail sync, the app will open a browser window for OAuth authorization

### Run

```bash
source .venv/bin/activate
python -m uvicorn reconnect.main:app --host 127.0.0.1 --port 8000 --reload
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

### First-time usage

1. Go to **Sync** and run Apple Contacts to bootstrap your contact list
2. Run iMessage sync to import your message history
3. (Optional) Set up Gmail and Calendar sync
4. Go to the **Dashboard** — suggestions will be generated automatically

## Project structure

```
reconnect/
  main.py              # FastAPI app entry point
  config.py            # Paths, weights, and tunable parameters
  database.py          # SQLite schema and migrations
  routers/
    suggestions.py     # Dashboard, feedback, reach-out flow
    contacts.py        # Contact list, detail views, never-show
    sync.py            # Data sync orchestration
    lists.py           # Custom lists CRUD
    settings.py        # Scoring rules configuration
  ingestion/
    apple_contacts.py  # Import from macOS Address Book
    imessage.py        # iMessage extraction with text backfill
    gmail.py           # Gmail OAuth ingestion
  scoring/
    scorer.py          # Contact score calculation
    suggester.py       # Monthly suggestion generation
    rules.py           # Pattern detection rules
    feedback.py        # User feedback adjustments
    settings.py        # Settings persistence
  resolution/
    normalizer.py      # Phone/email normalization
    resolver.py        # Duplicate contact merging
  enrichment/
    social.py          # LinkedIn/Twitter profile lookup
  templates/           # Jinja2 HTML templates
  static/              # CSS
```

## Configuration

All scoring parameters are configurable from the **Rules** page in the UI. You can also set them via environment variables or by editing `reconnect/config.py`:

| Parameter | Default | Description |
|---|---|---|
| `SUGGESTIONS_PER_BATCH` | 10 | Suggestions generated per month |
| `TOP_PICK_COUNT` | 7 | High-score picks per batch |
| `SURPRISE_PICK_COUNT` | 3 | Serendipity picks per batch |
| `MIN_INTERACTIONS_THRESHOLD` | 5 | Minimum interactions to be suggested |
| `ACTIVE_DAYS_THRESHOLD` | 30 | Contacts active within this many days are excluded |
| `RECENT_SUGGESTION_MONTHS` | 3 | Don't re-suggest within this window |

## Tech stack

- **Backend**: FastAPI + Uvicorn
- **Frontend**: Jinja2 templates + vanilla CSS (no JavaScript frameworks)
- **Database**: SQLite with WAL mode
- **Data sources**: macOS iMessage/Contacts/Calendar databases, Gmail API

## Tests

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT
