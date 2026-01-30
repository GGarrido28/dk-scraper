"""Shared utility functions for DraftKings scrapers."""

import datetime
import os
from pathlib import Path
from typing import Dict, Any, Optional

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


def find_latest_matching_file(path: str, file_name: str) -> Optional[str]:
    """
    Find the most recently modified file containing the given name.

    Args:
        path: Directory path to search in.
        file_name: Substring to match in file names.

    Returns:
        The matching filename, or None if not found.
    """
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    file_list = sorted(os.listdir(path), key=mtime, reverse=True)
    for filename in file_list:
        if file_name in filename:
            return filename
    return None


def move_file(file: str, source_directory: Path, target_directory: Path) -> None:
    """
    Move a file from source to target directory, overwriting if exists.

    Args:
        file: Name of the file to move.
        source_directory: Source directory path.
        target_directory: Target directory path.
    """
    file_address = source_directory / file
    target_file_address = target_directory / file
    if target_file_address.exists():
        target_file_address.unlink()
    file_address.rename(target_file_address)
