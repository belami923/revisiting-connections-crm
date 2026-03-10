"""Settings service: load, save, merge user overrides with defaults."""

from __future__ import annotations

import json
import logging
import sqlite3

from reconnect.config import (
    INTERACTION_WEIGHTS,
    SUGGESTIONS_PER_BATCH,
    MIN_INTERACTIONS_THRESHOLD,
    ACTIVE_DAYS_THRESHOLD,
    RECENT_SUGGESTION_MONTHS,
    TOP_PICK_COUNT,
    SURPRISE_PICK_COUNT,
    SURPRISE_POOL_SIZE,
)

logger = logging.getLogger(__name__)


def get_setting(conn: sqlite3.Connection, key: str, default=None):
    """Get a single setting value, falling back to default."""
    row = conn.execute(
        "SELECT value_json FROM settings WHERE key = ?", (key,)
    ).fetchone()
    if row:
        return json.loads(row["value_json"])
    return default


def save_setting(conn: sqlite3.Connection, key: str, value) -> None:
    """Upsert a setting value."""
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value_json, updated_at) "
        "VALUES (?, ?, datetime('now'))",
        (key, json.dumps(value)),
    )
    conn.commit()


def get_effective_weights(conn: sqlite3.Connection) -> dict:
    """Return interaction weights, merged with any DB overrides."""
    weights = dict(INTERACTION_WEIGHTS)
    for itype in weights:
        override = get_setting(conn, f"weight.{itype}")
        if override is not None:
            weights[itype] = float(override)
    return weights


def get_effective_suggestion_config(conn: sqlite3.Connection) -> dict:
    """Return suggestion config, merged with any DB overrides."""
    defaults = {
        "suggestions_per_batch": SUGGESTIONS_PER_BATCH,
        "min_interactions_threshold": MIN_INTERACTIONS_THRESHOLD,
        "active_days_threshold": ACTIVE_DAYS_THRESHOLD,
        "recent_suggestion_months": RECENT_SUGGESTION_MONTHS,
        "top_pick_count": TOP_PICK_COUNT,
        "surprise_pick_count": SURPRISE_PICK_COUNT,
        "surprise_pool_size": SURPRISE_POOL_SIZE,
    }
    result = {}
    for key, default_val in defaults.items():
        override = get_setting(conn, f"suggestion.{key}")
        if override is not None:
            result[key] = type(default_val)(override)
        else:
            result[key] = default_val
    return result


def get_effective_config(conn: sqlite3.Connection) -> dict:
    """Return the full merged config for the settings page."""
    from reconnect.scoring.rules import ALL_RULES

    rules = []
    for rule in ALL_RULES:
        rule_data = {
            "rule_id": rule.rule_id,
            "name": rule.name,
            "description": getattr(rule, "description", ""),
            "enabled": get_setting(conn, f"rule.{rule.rule_id}.enabled", True),
            "parameters": {},
        }
        params = getattr(rule, "parameters", {})
        for param_name, param_info in params.items():
            current = get_setting(
                conn, f"rule.{rule.rule_id}.{param_name}", param_info["default"]
            )
            rule_data["parameters"][param_name] = {
                **param_info,
                "current": current,
            }
        rules.append(rule_data)

    return {
        "rules": rules,
        "weights": get_effective_weights(conn),
        "suggestions": get_effective_suggestion_config(conn),
    }


def apply_settings_to_rules(conn: sqlite3.Connection, rules):
    """Apply DB overrides to rule instances. Returns only enabled rules."""
    active_rules = []
    for rule in rules:
        enabled = get_setting(conn, f"rule.{rule.rule_id}.enabled", True)
        if not enabled:
            continue

        params = getattr(rule, "parameters", {})
        for param_name, param_info in params.items():
            override = get_setting(conn, f"rule.{rule.rule_id}.{param_name}")
            if override is not None:
                setattr(rule, param_name, type(param_info["default"])(override))

        active_rules.append(rule)

    return active_rules


def reset_all_settings(conn: sqlite3.Connection) -> None:
    """Delete all settings, resetting to defaults."""
    conn.execute("DELETE FROM settings")
    conn.commit()
