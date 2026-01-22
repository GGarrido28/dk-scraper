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
    Returns validated contest data.
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
        self.logger = logging.getLogger(__name__)

        # API URLs
        self.url = LOBBY_URL
        self.contest_url = CONTEST_API_URL

        # State tracking
        self.contest_id_list = []

        # Schema for validation
        self.contest_schema = ContestSchema()

        # HTTP handler with retry logic
        self.http = HTTPHandler()

    def _parse_contests(
        self,
        sport: str,
        raw_contests: List[Dict[str, Any]],
        draft_group_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """Parse and validate contest data."""
        self.contest_id_list = []
        contest_attributes_map = {
            "IsGuaranteed": "guaranteed",
            "IsStarred": "starred",
            "IsDoubleUp": "double_up",
            "IsFiftyfifty": "fifty_fifty",
            "League": "league",
            "IsSteps": "multiplier",
            "IsQualifier": "qualifier",
        }
        exclude_contests = ["satellite", "supersat", "reignmakers"]

        self.logger.info(f"Collecting contest ids for {sport}.")

        contests = []
        validation_errors = []

        for contest in raw_contests:
            if draft_group_ids and contest["dg"] not in draft_group_ids:
                continue

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

            if not atts_dict["guaranteed"]:
                continue

            if contest["m"] <= 100 and contest["a"] <= 25:
                continue

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

            try:
                validated_contest = self.contest_schema.load(c)
                contests.append(validated_contest)
                self.contest_id_list.append(contest["id"])
            except ValidationError as err:
                validation_errors.append(
                    {"contest_id": contest["id"], "errors": err.messages}
                )
                self.logger.warning(f"Validation error for contest {contest['id']}: {err.messages}")

        if validation_errors:
            self.logger.warning(f"Skipped {len(validation_errors)} contests due to validation errors.")

        self.logger.info(f"Parsed {len(contests)} contests for {sport}.")

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
        Fetches lobby data and parses contests.

        Args:
            lobby_data: Optional pre-fetched lobby data. If None, fetches from API.
            draft_group_ids: List of draft group IDs to filter by.

        Returns:
            dict: Contains 'contests' list and 'lobby_data' dict.
        """
        start_time = datetime.datetime.now()
        contests = []

        try:
            self.logger.info(f"Starting contests scraper for {self.sport}.")

            if lobby_data is None:
                lobby_data = self.fetch_lobby_data()

            if len(lobby_data.get("Contests", [])) > 0:
                self.contest_id_list = []

                contests = self._parse_contests(
                    self.sport, lobby_data["Contests"], draft_group_ids=draft_group_ids
                )
            else:
                self.logger.info(f"No contests found in Lobby or {self.sport} is in offseason. No new contests added.")

            self.logger.info(f"Finished scraping contests for {self.sport}.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed contests scraper: {e}")
            raise e

        return {"contests": contests, "lobby_data": lobby_data}

    def _fetch_contest_attributes(self, contest_id: int) -> Optional[Dict[str, Any]]:
        """Fetch attributes for a single contest from the API."""
        try:
            url = self.contest_url % contest_id
            response = self.http.get(url)
            response.raise_for_status()

            data = json.loads(response.text)

            if not data or "contestDetail" not in data:
                self.logger.warning(f"Invalid response data for contest {contest_id}")
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
                self.logger.warning(f"Contest {contest_id} not found (404).")
            else:
                self.logger.error(f"HTTP error for contest {contest_id}: {str(http_err)}")
            return None

        except Exception as e:
            self.logger.error(f"Error fetching attributes for contest {contest_id}: {str(e)}")
            return None

    def fetch_attributes(
        self, contest_ids: List[int], batch_size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch contest attributes (is_final, is_cancelled, start_time) for given contest IDs.

        Args:
            contest_ids: List of contest IDs to fetch attributes for.
            batch_size: Number of concurrent requests per batch.

        Returns:
            list: List of contest attribute dictionaries.
        """
        start_time = datetime.datetime.now()
        fetched_contests = []

        try:
            if not contest_ids:
                self.logger.info("No contest IDs provided.")
                return fetched_contests

            self.logger.info(f"Fetching contest attributes for {len(contest_ids)} contests.")

            for i in range(0, len(contest_ids), batch_size):
                batch = contest_ids[i : i + batch_size]
                self.logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} contests)")

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
                                fetched_contests.append(contest_data)
                        except Exception as e:
                            self.logger.error(f"Error fetching attributes for contest {contest_id}: {str(e)}")

                if i + batch_size < len(contest_ids):
                    time.sleep(0.5)

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Fetched attributes for {len(fetched_contests)} contests in {elapsed_time}.")

        except Exception as e:
            self.logger.error(f"Failed fetching contest attributes: {e}")
            raise e

        return fetched_contests


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings contests for a specific sport."
    )
    parser.add_argument(
        "sport", type=str, help="Sport code (e.g., NFL, MLB, MMA, GOLF, CFB)"
    )
    parser.add_argument(
        "--fetch-attributes",
        action="store_true",
        help="Fetch attributes (is_final, is_cancelled, start_time) for given contest IDs",
    )
    parser.add_argument(
        "--contest-ids",
        type=str,
        help="Comma-separated contest IDs (only used with --fetch-attributes)",
    )

    args = parser.parse_args()

    scraper = ContestsScraper(sport=args.sport)

    if args.fetch_attributes:
        contest_ids = None
        if args.contest_ids:
            contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]
        if contest_ids:
            result = scraper.fetch_attributes(contest_ids=contest_ids)
            print(f"Fetched attributes for {len(result)} contests")
        else:
            print("No contest IDs provided for attribute fetching")
    else:
        result = scraper.scrape()
        print(f"Scraped {len(result['contests'])} contests")


if __name__ == "__main__":
    main()
