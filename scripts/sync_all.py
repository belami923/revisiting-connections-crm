#!/usr/bin/env python3
"""Run the full pipeline: ingest, resolve, score, and suggest."""

import logging
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from reconnect.database import init_db
from reconnect.ingestion.imessage import ingest_imessage
from reconnect.resolution.resolver import resolve_duplicates
from reconnect.scoring.scorer import recalculate_all_scores
from reconnect.scoring.suggester import generate_suggestions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    print("=" * 60)
    print("  Reconnect - Full Sync Pipeline")
    print("=" * 60)
    print()

    # Ensure DB exists
    init_db()

    # Step 1: Ingest iMessage
    print("[1/4] Ingesting iMessage data...")
    im_result = ingest_imessage()
    if im_result["status"] == "ok":
        print(f"  Imported {im_result['imported']} messages")
        print(f"  Skipped {im_result['skipped']} (invalid/duplicate)")
        print(f"  Group chats skipped: {im_result['group_skipped']}")
        print(f"  Unresolved handles: {im_result['unresolved']}")
    else:
        print(f"  Status: {im_result['status']}")
        if "reason" in im_result:
            print(f"  Reason: {im_result['reason']}")
    print()

    # Step 2: Resolve duplicates
    print("[2/4] Resolving duplicate contacts...")
    resolve_result = resolve_duplicates()
    if resolve_result["status"] == "ok":
        print(f"  Auto-merged {resolve_result['auto_merges']} duplicate contacts")
    print()

    # Step 3: Score all contacts
    print("[3/4] Scoring contacts and detecting patterns...")
    score_result = recalculate_all_scores()
    if score_result["status"] == "ok":
        print(f"  Scored {score_result['contacts_scored']} contacts")
        print(f"  Found {score_result['patterns_found']} pattern matches")
    print()

    # Step 4: Generate suggestions
    month = date.today().strftime("%Y-%m")
    print(f"[4/4] Generating suggestions for {month}...")
    suggest_result = generate_suggestions(month)

    if suggest_result.get("suggestions"):
        print()
        print("-" * 60)
        print(f"  10 People to Reconnect With ({month})")
        print("-" * 60)
        print()

        for s in suggest_result["suggestions"]:
            rank = s["rank"]
            name = s["display_name"]
            narrative = s.get("narrative") or "No specific pattern detected."
            score = s["score"]

            print(f"  #{rank}  {name}")
            print(f"       {narrative}")
            print(f"       (score: {score:.2f})")
            print()
    else:
        print("  No suggestions generated.")
        if suggest_result.get("status") == "no_candidates":
            print(
                "  This could mean you don't have enough dormant "
                "contacts with sufficient interaction history."
            )

    print("-" * 60)
    print("Sync complete.")


if __name__ == "__main__":
    main()
