"""Automated weekly pipeline scheduling for Scopefish.

Uses APScheduler's BackgroundScheduler to run the paper discovery pipeline
and project/funding fetchers on a weekly cadence. Safe for Hugging Face Spaces
(background thread, not a separate process).
"""

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_scheduler: Optional[BackgroundScheduler] = None
_lock = threading.Lock()

_last_pipeline_run: Optional[str] = None
_last_pipeline_status: Optional[str] = None
_last_projects_run: Optional[str] = None
_last_projects_status: Optional[str] = None


# ---------------------------------------------------------------------------
# Scheduled job wrappers (catch all exceptions)
# ---------------------------------------------------------------------------

def _run_pipeline() -> None:
    """Execute the paper discovery pipeline."""
    global _last_pipeline_run, _last_pipeline_status
    log.info("Scheduler: starting weekly pipeline run")
    try:
        from .pipeline import run as pipeline_run
        pipeline_run(days=7, max_papers=500)
        _last_pipeline_run = datetime.now(timezone.utc).isoformat()
        _last_pipeline_status = "success"
        log.info("Scheduler: pipeline run completed successfully")
    except Exception:
        _last_pipeline_run = datetime.now(timezone.utc).isoformat()
        _last_pipeline_status = "error"
        log.exception("Scheduler: pipeline run failed")


def _run_project_fetch() -> None:
    """Fetch projects and funding calls, then persist to DB."""
    global _last_projects_run, _last_projects_status
    log.info("Scheduler: starting weekly project/funding fetch")
    try:
        from .fetchers.projects import fetch_all_projects
        from . import models

        result = fetch_all_projects()
        conn = models.get_connection()
        try:
            models.save_funded_projects(conn, result.get("projects", []))
            for call in result.get("calls", []):
                models.save_funding_call(conn, call)
            conn.commit()
        finally:
            conn.close()

        _last_projects_run = datetime.now(timezone.utc).isoformat()
        _last_projects_status = "success"
        log.info("Scheduler: project/funding fetch completed successfully")
    except Exception:
        _last_projects_run = datetime.now(timezone.utc).isoformat()
        _last_projects_status = "error"
        log.exception("Scheduler: project/funding fetch failed")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start() -> None:
    """Start the background scheduler (idempotent — safe to call twice)."""
    global _scheduler
    with _lock:
        if _scheduler is not None and _scheduler.running:
            log.info("Scheduler: already running, skipping start")
            return

        _scheduler = BackgroundScheduler(timezone="UTC")

        # Weekly paper pipeline — Monday 06:00 UTC
        _scheduler.add_job(
            _run_pipeline,
            trigger="cron",
            day_of_week="mon",
            hour=6,
            minute=0,
            id="weekly_pipeline",
            replace_existing=True,
            misfire_grace_time=3600,
        )

        # Weekly project/funding fetch — Monday 08:00 UTC
        _scheduler.add_job(
            _run_project_fetch,
            trigger="cron",
            day_of_week="mon",
            hour=8,
            minute=0,
            id="weekly_projects",
            replace_existing=True,
            misfire_grace_time=3600,
        )

        _scheduler.start()
        log.info("Scheduler: started with weekly pipeline (Mon 06:00) and project fetch (Mon 08:00)")


def stop() -> None:
    """Shut down the scheduler gracefully."""
    global _scheduler
    with _lock:
        if _scheduler is not None and _scheduler.running:
            _scheduler.shutdown(wait=False)
            log.info("Scheduler: stopped")
        _scheduler = None


def get_status() -> dict:
    """Return scheduler status including next run times and last run info."""
    status: dict = {
        "scheduler_running": _scheduler is not None and _scheduler.running,
        "pipeline": {
            "last_run": _last_pipeline_run,
            "last_status": _last_pipeline_status,
            "next_run": None,
        },
        "projects": {
            "last_run": _last_projects_run,
            "last_status": _last_projects_status,
            "next_run": None,
        },
    }

    if _scheduler is not None and _scheduler.running:
        for job in _scheduler.get_jobs():
            next_run = job.next_run_time.isoformat() if job.next_run_time else None
            if job.id == "weekly_pipeline":
                status["pipeline"]["next_run"] = next_run
            elif job.id == "weekly_projects":
                status["projects"]["next_run"] = next_run

    return status


def trigger_pipeline_now() -> None:
    """Trigger an immediate pipeline run in the background."""
    if _scheduler is None or not _scheduler.running:
        raise RuntimeError("Scheduler is not running")
    _scheduler.add_job(
        _run_pipeline,
        id="manual_pipeline_trigger",
        replace_existing=True,
    )
    log.info("Scheduler: manual pipeline run triggered")
