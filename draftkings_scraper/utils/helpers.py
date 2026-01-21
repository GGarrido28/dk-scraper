"""Shared utility functions for DraftKings scrapers."""

import datetime
from typing import Dict, Any

import pytz


def is_contest_final(contest_detail: Dict[str, Any]) -> bool:
    """Check if contest is in final state (completed or cancelled)."""
    status = contest_detail.get("contestStateDetail", "")
    return status.lower().strip() in ["completed", "cancelled"]


def is_contest_cancelled(contest_detail: Dict[str, Any]) -> bool:
    """Check if contest is cancelled."""
    status = contest_detail.get("contestStateDetail", "")
    return status.lower().strip() == "cancelled"


def convert_datetime(dt_str: str) -> datetime.datetime:
    """
    Convert DraftKings datetime string to Eastern timezone.

    Args:
        dt_str: ISO format datetime string from DraftKings API.

    Returns:
        datetime object in US/Eastern timezone.
    """
    dt_str = dt_str.split(".")[0]
    dt_obj = datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
    dt_obj = dt_obj.replace(tzinfo=pytz.utc)
    dt_obj = dt_obj.astimezone(pytz.timezone("US/Eastern"))
    return dt_obj
