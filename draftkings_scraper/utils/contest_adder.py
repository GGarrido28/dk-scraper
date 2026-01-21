import json
import urllib.request
import datetime
import logging
import os
import re
from typing import Dict, Any, Optional

from bs4 import BeautifulSoup

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.constants import (
    SPORT_MAP,
    LOBBY_URL,
    CONTEST_API_URL,
    DRAFT_URL,
)
from draftkings_scraper.http_handler import HTTPHandler
from draftkings_scraper.utils.helpers import (
    is_contest_final,
    is_contest_cancelled,
    convert_datetime,
)
from draftkings_scraper.payout import PayoutScraper
from draftkings_scraper.player_salary import PlayerSalaryScraper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ContestAdder:
    """
    Utility class for adding a single contest to the database by ID.
    Composes existing scrapers to insert related records (contest, draft group, payouts, player salaries).
    """

    def __init__(self):
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = LoggerManager(
            self.script_name,
            self.script_path,
            sport=None,
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
        self.contest_url = CONTEST_API_URL
        self.lobby_url = LOBBY_URL
        self.draft_url = DRAFT_URL

        # HTTP handler with retry logic
        self.http = HTTPHandler()

    def _extract_draft_group_id_from_page(self, contest_id: int) -> Optional[int]:
        """Extract draft group ID from the draft page HTML."""
        try:
            response = self.http.get(self.draft_url % contest_id)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            soup_str = str(soup)

            draft_group_match = re.search(r"draftGroupId\s*:\s*(\d+)", soup_str)
            if draft_group_match:
                return int(draft_group_match.group(1))
        except Exception as e:
            msg = f"Error extracting draft group ID from page: {e}"
            self.logger.log(level="warning", message=msg)

        return None

    def add_contest(self, contest_id: int) -> Dict[str, Any]:
        """
        Add a single contest to the database with all related data.

        Fetches contest data from API and lobby, then inserts:
        - Contest record
        - Draft group record
        - Payout records
        - Player salary records

        Args:
            contest_id: The DraftKings contest ID to add.

        Returns:
            dict: Status information including contest_id, status, sport, draft_group_id.
        """
        try:
            # Step 1: Get basic contest info from API to determine sport
            contest_response = self.http.get(self.contest_url % contest_id)
            contest_response.raise_for_status()
            contest_data = json.loads(contest_response.text)

            if not contest_data or "contestDetail" not in contest_data:
                msg = f"No contest data found for contest {contest_id}"
                self.logger.log(level="warning", message=msg)
                return {"contest_id": contest_id, "status": "not_found"}

            contest_detail = contest_data["contestDetail"]

            # Extract and map sport
            sport = contest_detail.get("sport", "").lower()
            sport = SPORT_MAP.get(sport, sport)

            if not sport:
                msg = f"Could not determine sport for contest {contest_id}"
                self.logger.log(level="warning", message=msg)
                return {
                    "contest_id": contest_id,
                    "status": "error",
                    "message": "Sport not found",
                }

            msg = f"Fetching lobby data for sport {sport} to find contest {contest_id}"
            self.logger.log(level="info", message=msg)

            # Step 2: Fetch lobby data
            try:
                with urllib.request.urlopen(self.lobby_url % sport) as url:
                    lobby_data = json.loads(url.read().decode())
            except Exception as e:
                msg = f"Error fetching lobby data for sport {sport}: {str(e)}"
                self.logger.log(level="error", message=msg)
                lobby_data = {"Contests": [], "DraftGroups": [], "GameTypes": []}

            # Step 3: Find contest in lobby data
            contest_found = False
            contest_info = None
            draft_group_id = None

            for contest in lobby_data.get("Contests", []):
                if contest["id"] == contest_id:
                    contest_found = True
                    contest_info = contest
                    draft_group_id = contest["dg"]
                    break

            # Try to get draft_group_id from other sources if not found
            if not contest_found:
                if "draftGroupId" in contest_detail:
                    draft_group_id = contest_detail["draftGroupId"]
                else:
                    draft_group_id = self._extract_draft_group_id_from_page(contest_id)
                    if draft_group_id:
                        msg = (
                            f"Extracted draft group ID {draft_group_id} from draft page"
                        )
                        self.logger.log(level="info", message=msg)
                    else:
                        draft_group_id = 0

            # Step 4: Insert contest record
            if contest_found:
                self._insert_contest_from_lobby(
                    contest_id, contest_info, contest_detail, draft_group_id
                )
            else:
                msg = f"Contest {contest_id} not found in lobby. Using partial data from contest API."
                self.logger.log(level="warning", message=msg)
                self._insert_contest_from_api(
                    contest_id, contest_detail, draft_group_id
                )

            # Step 5: Insert draft group if found in lobby
            if contest_found and draft_group_id:
                for dg in lobby_data.get("DraftGroups", []):
                    if dg["DraftGroupId"] == draft_group_id:
                        self._insert_draft_group(dg, sport)
                        break
            elif draft_group_id and draft_group_id > 0:
                self._insert_minimal_draft_group(draft_group_id, sport, contest_detail)

            # Step 6: Process payouts using PayoutScraper
            if "payoutSummary" in contest_detail and contest_detail["payoutSummary"]:
                self._process_payouts(contest_id, sport)

            # Step 7: Process player salaries using PlayerSalaryScraper
            if draft_group_id and draft_group_id > 0:
                self._process_player_salaries(draft_group_id, sport)

            msg = f"Added contest {contest_id} to database."
            self.logger.log(level="info", message=msg)

            return {
                "contest_id": contest_id,
                "status": "added",
                "sport": sport,
                "draft_group_id": draft_group_id,
                "from_lobby": contest_found,
            }

        except Exception as e:
            msg = f"Error adding contest {contest_id} to database: {str(e)}"
            self.logger.log(level="error", message=msg)
            return {"contest_id": contest_id, "status": "error", "message": str(e)}

        finally:
            self._close_sql_connections()

    def _insert_contest_from_lobby(
        self,
        contest_id: int,
        contest_info: Dict[str, Any],
        contest_detail: Dict[str, Any],
        draft_group_id: int,
    ) -> None:
        """Insert contest using complete lobby data."""
        contest_attributes_map = {
            "IsGuaranteed": "guranteed",
            "IsStarred": "starred",
            "IsDoubleUp": "double_up",
            "IsFiftyfifty": "fifty_fifty",
            "League": "league",
            "IsSteps": "multiplier",
            "IsQualifier": "qualifier",
        }

        c_atts = contest_info.get("attr", [])
        atts_dict = {v: k in c_atts for k, v in contest_attributes_map.items()}

        contest = {
            "contest_id": contest_id,
            "contest_name": contest_info.get("n", ""),
            "entry_fee": contest_info.get("a", 0),
            "crown_amount": contest_info.get("crownAmount", 0),
            "max_entries": contest_info.get("m", 0),
            "entries_per_user": contest_info.get("mec", 0),
            "draft_group_id": draft_group_id,
            "pd": json.dumps(contest_info.get("pd", {})),
            "po": contest_info.get("po", 0),
            "attr": json.dumps(contest_info.get("attr", [])),
            "contest_date": contest_info.get("sdstring", ""),
            "contest_url": f"https://www.draftkings.com/draft/contest/{contest_id}",
            "is_downloaded": False,
            "start_time": convert_datetime(contest_detail.get("contestStartTime", "")),
            "is_final": is_contest_final(contest_detail),
            "is_cancelled": is_contest_cancelled(contest_detail),
        }
        contest.update(atts_dict)

        self.draftkings_connection.insert_rows(
            "contests", contest.keys(), [contest], contains_dicts=True, update=True
        )

        msg = f"Added contest {contest_id} with complete lobby data."
        self.logger.log(level="info", message=msg)

    def _insert_contest_from_api(
        self, contest_id: int, contest_detail: Dict[str, Any], draft_group_id: int
    ) -> None:
        """Insert contest using partial API data."""
        attr_dict = {}
        feature_list = contest_detail.get("features", [])
        for feature in feature_list:
            attr_dict[feature] = True

        contest = {
            "contest_id": contest_id,
            "contest_name": contest_detail.get(
                "name", contest_detail.get("contestName", "")
            ),
            "entry_fee": contest_detail.get("entryFee", 0),
            "crown_amount": (
                contest_detail.get("crownAmount", 0)
                if "crownAmount" in contest_detail
                else 0
            ),
            "max_entries": contest_detail.get("maximumEntries", 0),
            "entries_per_user": contest_detail.get("maximumEntriesPerUser", 0),
            "draft_group_id": draft_group_id,
            "pd": json.dumps(contest_detail.get("prizeDescription", {})),
            "po": contest_detail.get(
                "prizePool", contest_detail.get("totalPayouts", 0)
            ),
            "attr": json.dumps(attr_dict),
            "guranteed": "IsGuaranteed" in attr_dict,
            "starred": "IsStarred" in attr_dict,
            "double_up": "IsDoubleUp" in attr_dict,
            "fifty_fifty": "IsFiftyfifty" in attr_dict,
            "league": "League" in attr_dict,
            "multiplier": "IsSteps" in attr_dict,
            "qualifier": "IsQualifier" in attr_dict,
            "contest_date": contest_detail.get("contestStartTime", ""),
            "start_time": convert_datetime(contest_detail.get("contestStartTime", "")),
            "is_final": is_contest_final(contest_detail),
            "is_cancelled": is_contest_cancelled(contest_detail),
            "contest_url": f"https://www.draftkings.com/draft/contest/{contest_id}",
            "is_downloaded": False,
        }

        self.draftkings_connection.insert_rows(
            "contests", contest.keys(), [contest], contains_dicts=True, update=True
        )

    def _insert_draft_group(self, draft_group: Dict[str, Any], sport: str) -> None:
        """Insert draft group from lobby data."""
        contest_start_time_suffix = draft_group.get("ContestStartTimeSuffix")
        if contest_start_time_suffix:
            contest_start_time_suffix = contest_start_time_suffix.strip()

        dg = {
            "draft_group_id": draft_group["DraftGroupId"],
            "allow_ugc": draft_group.get("AllowUGC"),
            "contest_start_time_suffix": contest_start_time_suffix,
            "contest_start_time_type": draft_group["ContestStartTimeType"],
            "contest_type_id": draft_group["ContestTypeId"],
            "draft_group_series_id": draft_group["DraftGroupSeriesId"],
            "draft_group_tag": (
                None
                if draft_group["DraftGroupTag"] == ""
                else draft_group["DraftGroupTag"]
            ),
            "game_count": draft_group["GameCount"],
            "game_set_key": draft_group["GameSetKey"],
            "game_type": draft_group["GameType"],
            "game_type_id": draft_group["GameTypeId"],
            "games": draft_group["Games"],
            "sort_order": draft_group["SortOrder"],
            "sport": draft_group["Sport"],
            "start_date": draft_group["StartDate"],
            "start_date_est": draft_group["StartDateEst"],
        }

        self.draftkings_connection.insert_rows(
            "draft_groups", dg.keys(), [dg], contains_dicts=True, update=True
        )

        msg = f"Added draft group {draft_group['DraftGroupId']}."
        self.logger.log(level="info", message=msg)

    def _insert_minimal_draft_group(
        self, draft_group_id: int, sport: str, contest_detail: Dict[str, Any]
    ) -> None:
        """Insert minimal draft group when not found in lobby."""
        draft_group = {
            "draft_group_id": draft_group_id,
            "sport": sport,
            "start_date": contest_detail.get(
                "contestStartTime", datetime.datetime.now().isoformat()
            ),
            "start_date_est": contest_detail.get(
                "contestStartTime", datetime.datetime.now().isoformat()
            ),
            "game_count": contest_detail.get("gameCount", 0),
            "game_type_id": contest_detail.get("gameTypeId", 0),
            "game_type": contest_detail.get("gameType", ""),
        }

        self.draftkings_connection.insert_rows(
            "draft_groups",
            draft_group.keys(),
            [draft_group],
            contains_dicts=True,
            update=True,
        )

    def _process_payouts(self, contest_id: int, sport: str) -> None:
        """Process payouts using PayoutScraper."""
        try:
            # Check if payout already exists
            q = f"SELECT contest_id FROM draftkings.payout WHERE contest_id = {contest_id}"
            existing = self.draftkings_connection.execute(q)
            if existing:
                msg = f"Payout for contest {contest_id} already exists."
                self.logger.log(level="info", message=msg)
                return

            payout_scraper = PayoutScraper(sport=sport)
            payout_scraper.scrape(contest_ids=[contest_id])
        except Exception as e:
            msg = f"Error processing payouts for contest {contest_id}: {e}"
            self.logger.log(level="warning", message=msg)

    def _process_player_salaries(self, draft_group_id: int, sport: str) -> None:
        """Process player salaries using PlayerSalaryScraper."""
        try:
            # Check if player salaries already exist for this draft group
            q = f"SELECT COUNT(*) as count FROM draftkings.player_salary WHERE draft_group_id = {draft_group_id}"
            result = self.draftkings_connection.execute(q)
            if result and result[0]["count"] > 0:
                msg = f"Player salaries for draft group {draft_group_id} already exist."
                self.logger.log(level="info", message=msg)
                return

            player_salary_scraper = PlayerSalaryScraper(sport=sport)
            player_salary_scraper.scrape(draft_group_ids=[draft_group_id])
        except Exception as e:
            msg = f"Error processing player salaries for draft group {draft_group_id}: {e}"
            self.logger.log(level="warning", message=msg)

    def _close_sql_connections(self) -> None:
        """Close database connections."""
        self.logger.close_logger()
        self.draftkings_connection.close()


def main():
    """CLI entry point for adding a contest."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Add a single DraftKings contest to the database."
    )
    parser.add_argument("contest_id", type=int, help="The contest ID to add")

    args = parser.parse_args()

    adder = ContestAdder()
    result = adder.add_contest(args.contest_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
