#!/usr/bin/env python3
"""Standalone server launcher that works without venv activation."""
import sys
import os

# Ensure the project root is on the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Change to project directory so static/template paths resolve
os.chdir(project_root)

import uvicorn

if __name__ == "__main__":
    uvicorn.run("reconnect.main:app", host="127.0.0.1", port=8000)
