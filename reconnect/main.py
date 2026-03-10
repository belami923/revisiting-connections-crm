"""FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from reconnect.database import init_db
from reconnect.routers import suggestions, contacts, sync, lists, settings

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Reconnect", version="0.1.0", lifespan=lifespan)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Include routers
app.include_router(suggestions.router)
app.include_router(contacts.router)
app.include_router(sync.router)
app.include_router(lists.router)
app.include_router(settings.router)


def run():
    uvicorn.run("reconnect.main:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    run()
