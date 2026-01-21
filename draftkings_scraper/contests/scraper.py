import urllib.request
import json
import datetime
import logging
import os
import argparse
import time
import concurrent.futures
from typing import List, Dict, Any, Optional

import requests
from marshmallow import ValidationError

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.schemas import ContestSchema
from draftkings_scraper.constants import LOBBY_URL, CONTEST_API_URL
from draftkings_scraper.http_handler import HTTPHandler
from draftkings_scraper.utils.helpers import (
    is_contest_final,
    is_contest_cancelled,
    convert_datetime,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ContestsScraper:
    """
    Scraper for DraftKings contests data.
    Populates the draftkings.contests table.
    """

    def __init__(self, sport: str):
        """
        Initialize the ContestsScraper.

        Args:
            sport: Sport code (e.g., 'NFL', 'MLB', 'MMA')
        """
        self.sport = sport
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = LoggerManager(
            self.script_name,
            self.script_path,
            sport=sport,
            database="defaultdb",
            schema="draftkings",
        )
        self.logger.log_exceptions()

        self.database = "defaultdb"
        self.schema = "draftkings"
        self.draftkings_connection = PostgresManager(
            "digital_ocean", self.database, self.schema, return_logging=False
        )

        # API URLs
        self.url = LOBBY_URL
        self.contest_url = CONTEST_API_URL

        # State tracking
        self.contest_id_list = []

        # Schema for validation
        self.contest_schema = ContestSchema()

        # HTTP handler with retry logic
        self.http = HTTPHandler()

    def _update_contests(
        self,
        sport: str,
        raw_contests: List[Dict[str, Any]],
        draft_group_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """Update contests table with filtered contest data.

        Args:
            sport: Sport code.
            raw_contests: Raw contest data from lobby API.
            draft_group_ids: List of draft group IDs to filter by.
                           Only contests in these draft groups will be included.
                           If empty/None, no filtering by draft group.

        Returns:
            list: List of validated contest dictionaries that were inserted.
        """
        self.contest_id_list = []
        contest_attributes_map = {
            "IsGuaranteed": "guranteed",
            "IsStarred": "starred",
            "IsDoubleUp": "double_up",
            "IsFiftyfifty": "fifty_fifty",
            "League": "league",
            "IsSteps": "multiplier",
            "IsQualifier": "qualifier",
        }
        exclude_contests = ["satellite", "supersat", "reignmakers"]

        msg = f"Collecting contest ids for {sport}."
        self.logger.log(level="info", message=msg)

        contests = []
        validation_errors = []

        for contest in raw_contests:
            # Filter by draft_group_ids if provided
            if draft_group_ids and contest["dg"] not in draft_group_ids:
                continue

            # Check exclusions
            contest_name_lower = contest["n"].lower()
            skip = False
            for exclude in exclude_contests:
                if exclude in contest_name_lower:
                    skip = True
                    break
            if skip:
                continue

            c_atts = contest["attr"]
            atts_dict = {}
            for att in contest_attributes_map:
                atts_dict[contest_attributes_map.get(att)] = att in c_atts

            # Filter out non-guaranteed contests
            if not atts_dict["guranteed"]:
                continue

            # Filter small contests
            if contest["m"] <= 100 and contest["a"] <= 25:
                continue

            # Filter small double-up/fifty-fifty contests
            if (atts_dict.get("double_up") or atts_dict.get("fifty_fifty")) and contest[
                "m"
            ] <= 100:
                continue

            c = {
                "contest_id": contest["id"],
                "contest_name": contest["n"],
                "entry_fee": contest["a"],
                "crown_amount": contest["crownAmount"],
                "max_entries": contest["m"],
                "entries_per_user": contest["mec"],
                "draft_group_id": contest["dg"],
                "pd": contest["pd"],
                "po": contest["po"],
                "attr": contest["attr"],
                "contest_date": contest["sdstring"],
                "contest_url": "https://www.draftkings.com/draft/contest/"
                + str(contest["id"]),
                "is_downloaded": False,
            }
            c.update(atts_dict)

            # Validate with schema
            try:
                validated_contest = self.contest_schema.load(c)
                contests.append(validated_contest)
                self.contest_id_list.append(contest["id"])
            except ValidationError as err:
                validation_errors.append(
                    {"contest_id": contest["id"], "errors": err.messages}
                )
                msg = f"Validation error for contest {contest['id']}: {err.messages}"
                self.logger.log(level="warning", message=msg)

        if validation_errors:
            msg = f"Skipped {len(validation_errors)} contests due to validation errors."
            self.logger.log(level="warning", message=msg)

        if contests:
            # Get column names from validated data
            contest_cols = list(contests[0].keys())
            self.draftkings_connection.insert_rows(
                "contests", contest_cols, contests, contains_dicts=True, update=True
            )
            msg = f"Imported {len(contests)} contests for {sport}."
            self.logger.log(level="info", message=msg)

        return contests

    def fetch_lobby_data(self) -> Dict[str, Any]:
        """
        Fetch raw lobby data from DraftKings API.

        Returns:
            dict: Raw lobby data containing Contests, GameTypes, DraftGroups, etc.
        """
        url = self.url % self.sport

        with urllib.request.urlopen(url) as url_response:
            data = json.loads(url_response.read().decode())

        return data

    def scrape(
        self,
        lobby_data: Optional[Dict[str, Any]] = None,
        draft_group_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Main scraping method for contests.
        Fetches lobby data and updates the contests table.

        Args:
            lobby_data: Optional pre-fetched lobby data. If None, fetches from API.
            draft_group_ids: List of draft group IDs to filter by.
                           Only contests in these draft groups will be included.

        Returns:
            dict: Contains 'contests' list and 'lobby_data' dict.
        """
        start_time = datetime.datetime.now()
        contests = []

        try:
            msg = f"Starting contests scraper for {self.sport}."
            self.logger.log(level="info", message=msg)

            # Fetch lobby data if not provided
            if lobby_data is None:
                lobby_data = self.fetch_lobby_data()

            if len(lobby_data.get("Contests", [])) > 0:
                # Reset state
                self.contest_id_list = []

                # Update contests table
                contests = self._update_contests(
                    self.sport, lobby_data["Contests"], draft_group_ids=draft_group_ids
                )
            else:
                msg = f"No contests found in Lobby or {self.sport} is in offseason. No new contests added."
                self.logger.log(level="info", message=msg)

            msg = f"Finished scraping contests for {self.sport}."
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed contests scraper: {e}"
            self.logger.log(level="error", message=msg)
            raise e

        finally:
            if self.logger.warning_logs or self.logger.error_logs:
                logs = sorted(
                    list(set(self.logger.warning_logs))
                    + list(set(self.logger.error_logs))
                )
                logs_str = ",".join(logs)
                self.logger.check_alert_log(
                    alert_name=f"Error processing contests data for {self.sport}",
                    alert_description=f"Error processing contests data: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.contests",
                )
            self._close_sql_connections()

        return {"contests": contests, "lobby_data": lobby_data}

    def _fetch_contest_attributes(self, contest_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch attributes for a single contest from the API.

        Args:
            contest_id: The contest ID to fetch attributes for.

        Returns:
            dict: Contest update data or None if failed.
        """
        try:
            url = self.contest_url % contest_id
            response = self.http.get(url)
            response.raise_for_status()

            data = json.loads(response.text)

            if not data or "contestDetail" not in data:
                msg = f"Invalid response data for contest {contest_id}"
                self.logger.log(level="warning", message=msg)
                return None

            contest_detail = data["contestDetail"]
            contest_update = {
                "contest_id": contest_id,
                "is_final": is_contest_final(contest_detail),
                "is_cancelled": is_contest_cancelled(contest_detail),
                "start_time": convert_datetime(contest_detail["contestStartTime"]),
            }

            if "name" in contest_detail:
                contest_update["contest_name"] = contest_detail["name"]

            if "maximumEntries" in contest_detail:
                contest_update["max_entries"] = contest_detail["maximumEntries"]

            return contest_update

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 404:
                msg = f"Contest {contest_id} not found (404)."
                self.logger.log(level="warning", message=msg)
            else:
                msg = f"HTTP error for contest {contest_id}: {str(http_err)}"
                self.logger.log(level="error", message=msg)
            return None

        except Exception as e:
            msg = f"Error fetching attributes for contest {contest_id}: {str(e)}"
            self.logger.log(level="error", message=msg)
            return None

    def _get_contests_to_update(self) -> List[int]:
        """Get contest IDs that need attribute updates."""
        q = """
            SELECT contest_id
            FROM draftkings.contests
            WHERE
                (start_time >= CURRENT_DATE OR
                COALESCE(is_final, FALSE) = FALSE OR
                COALESCE(is_cancelled, FALSE) = FALSE) AND
                COALESCE(is_downloaded, FALSE) = FALSE
        """
        results = self.draftkings_connection.execute(q)
        return [x["contest_id"] for x in results]

    def update_attributes(
        self, contest_ids: Optional[List[int]] = None, batch_size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Update contest attributes (is_final, is_cancelled, start_time) for existing contests.

        Args:
            contest_ids: Optional list of contest IDs to update. If None, fetches from database.
            batch_size: Number of concurrent requests per batch.

        Returns:
            list: List of updated contest dictionaries.
        """
        start_time = datetime.datetime.now()
        updated_contests = []

        try:
            # Get contests to update
            if contest_ids is None:
                contest_ids = self._get_contests_to_update()

            if not contest_ids:
                msg = "No contests require attribute updates."
                self.logger.log(level="info", message=msg)
                return updated_contests

            msg = f"Updating contest attributes for {len(contest_ids)} contests."
            self.logger.log(level="info", message=msg)

            # Process in batches
            for i in range(0, len(contest_ids), batch_size):
                batch = contest_ids[i : i + batch_size]
                msg = f"Processing batch {i // batch_size + 1} ({len(batch)} contests)"
                self.logger.log(level="info", message=msg)

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=batch_size
                ) as executor:
                    future_to_contest = {
                        executor.submit(self._fetch_contest_attributes, cid): cid
                        for cid in batch
                    }

                    for future in concurrent.futures.as_completed(future_to_contest):
                        contest_id = future_to_contest[future]
                        try:
                            contest_data = future.result()
                            if contest_data:
                                updated_contests.append(contest_data)
                        except Exception as e:
                            msg = f"Error updating attributes for contest {contest_id}: {str(e)}"
                            self.logger.log(level="error", message=msg)

                # Rate limiting between batches
                if i + batch_size < len(contest_ids):
                    time.sleep(0.5)

            # Bulk update to database
            if updated_contests:
                chunk_size = 100
                for i in range(0, len(updated_contests), chunk_size):
                    chunk = updated_contests[i : i + chunk_size]
                    try:
                        self.draftkings_connection.insert_rows(
                            "contests",
                            chunk[0].keys(),
                            chunk,
                            contains_dicts=True,
                            update=True,
                        )
                        msg = f"Updated attributes for {len(chunk)} contests."
                        self.logger.log(level="info", message=msg)
                    except Exception as e:
                        msg = f"Error batch updating contests: {str(e)}"
                        self.logger.log(level="error", message=msg)

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Completed updating {len(updated_contests)} contests in {elapsed_time}."
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed updating contest attributes: {e}"
            self.logger.log(level="error", message=msg)
            raise e

        finally:
            if self.logger.warning_logs or self.logger.error_logs:
                logs = sorted(
                    list(set(self.logger.warning_logs))
                    + list(set(self.logger.error_logs))
                )
                logs_str = ",".join(logs)
                self.logger.check_alert_log(
                    alert_name=f"Error updating contest attributes",
                    alert_description=f"Error updating contest attributes: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.contests",
                )
            self._close_sql_connections()

        return updated_contests

    def _close_sql_connections(self):
        """Close all database connections."""
        self.logger.close_logger()
        self.draftkings_connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings contests for a specific sport."
    )
    parser.add_argument(
        "sport", type=str, help="Sport code (e.g., NFL, MLB, MMA, GOLF, CFB)"
    )
    parser.add_argument(
        "--update-attributes",
        action="store_true",
        help="Update attributes (is_final, is_cancelled, start_time) for existing contests instead of scraping new ones",
    )
    parser.add_argument(
        "--contest-ids",
        type=str,
        help="Comma-separated contest IDs to update (only used with --update-attributes)",
    )

    args = parser.parse_args()

    scraper = ContestsScraper(sport=args.sport)

    if args.update_attributes:
        contest_ids = None
        if args.contest_ids:
            contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]
        scraper.update_attributes(contest_ids=contest_ids)
    else:
        scraper.scrape()


if __name__ == "__main__":
    main()
