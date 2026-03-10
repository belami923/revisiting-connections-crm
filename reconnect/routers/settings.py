"""Routes for the settings page: rules, weights, suggestion config."""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from reconnect.database import get_connection
from reconnect.scoring.settings import (
    get_effective_config,
    save_setting,
    reset_all_settings,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings")
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
def settings_page(request: Request):
    """Render the settings page with all rules, weights, and suggestion config."""
    conn = get_connection()
    try:
        config = get_effective_config(conn)
        return templates.TemplateResponse(
            request,
            "settings.html",
            {"config": config},
        )
    finally:
        conn.close()


@router.post("/rule/{rule_id}/toggle")
def toggle_rule(rule_id: str):
    """Enable or disable a rule."""
    conn = get_connection()
    try:
        from reconnect.scoring.settings import get_setting

        current = get_setting(conn, f"rule.{rule_id}.enabled", True)
        save_setting(conn, f"rule.{rule_id}.enabled", not current)
    finally:
        conn.close()
    return RedirectResponse(url="/settings/", status_code=303)


@router.post("/rule/{rule_id}/params")
def update_rule_params(request: Request, rule_id: str):
    """Update rule threshold parameters."""
    import asyncio

    conn = get_connection()
    try:
        # Get form data synchronously
        loop = asyncio.new_event_loop()
        form_data = loop.run_until_complete(request.form())
        loop.close()

        for key, value in form_data.items():
            if key.startswith("param_"):
                param_name = key[6:]  # Remove "param_" prefix
                # Try to convert to appropriate type
                try:
                    # Try int first, then float
                    if "." in value:
                        save_setting(conn, f"rule.{rule_id}.{param_name}", float(value))
                    else:
                        save_setting(conn, f"rule.{rule_id}.{param_name}", int(value))
                except ValueError:
                    save_setting(conn, f"rule.{rule_id}.{param_name}", value)
    finally:
        conn.close()
    return RedirectResponse(url="/settings/", status_code=303)


@router.post("/weights")
def update_weights(request: Request):
    """Update interaction weights."""
    import asyncio

    conn = get_connection()
    try:
        loop = asyncio.new_event_loop()
        form_data = loop.run_until_complete(request.form())
        loop.close()

        for key, value in form_data.items():
            if key.startswith("weight_"):
                weight_name = key[7:]  # Remove "weight_" prefix
                try:
                    save_setting(conn, f"weight.{weight_name}", float(value))
                except ValueError:
                    pass
    finally:
        conn.close()
    return RedirectResponse(url="/settings/", status_code=303)


@router.post("/suggestions")
def update_suggestion_config(request: Request):
    """Update suggestion settings."""
    import asyncio

    conn = get_connection()
    try:
        loop = asyncio.new_event_loop()
        form_data = loop.run_until_complete(request.form())
        loop.close()

        for key, value in form_data.items():
            if key.startswith("suggestion_"):
                setting_name = key[11:]  # Remove "suggestion_" prefix
                try:
                    if "." in value:
                        save_setting(conn, f"suggestion.{setting_name}", float(value))
                    else:
                        save_setting(conn, f"suggestion.{setting_name}", int(value))
                except ValueError:
                    pass
    finally:
        conn.close()
    return RedirectResponse(url="/settings/", status_code=303)


@router.post("/reset")
def reset_settings():
    """Reset all settings to defaults."""
    conn = get_connection()
    try:
        reset_all_settings(conn)
    finally:
        conn.close()
    return RedirectResponse(url="/settings/", status_code=303)
