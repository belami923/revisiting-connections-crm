#!/usr/bin/env python3
"""Initial setup: create database and import contacts from Apple AddressBook."""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from reconnect.database import init_db
from reconnect.ingestion.apple_contacts import bootstrap_contacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    print("=" * 60)
    print("  Reconnect - Initial Setup")
    print("=" * 60)
    print()

    # Initialize database
    print("[1/2] Initializing database...")
    init_db()
    print("  Database created successfully.")
    print()

    # Bootstrap contacts
    print("[2/2] Importing contacts from Apple AddressBook...")
    result = bootstrap_contacts()

    if result["status"] == "ok":
        print(f"  Imported {result['imported']} contacts")
        print(f"  Skipped {result['skipped']} (no identifiers or duplicates)")
    elif result["status"] == "skipped":
        print(f"  Skipped: {result['reason']}")
        print(
            "  Note: You may need to grant Full Disk Access to Terminal "
            "in System Settings > Privacy & Security > Full Disk Access"
        )
    elif result["status"] == "error":
        print(f"  Error: {result['reason']}")
        print(
            "  Tip: Grant Full Disk Access to Terminal in "
            "System Settings > Privacy & Security > Full Disk Access"
        )

    print()
    print("Setup complete. Run sync_all.py to import interactions.")


if __name__ == "__main__":
    main()
