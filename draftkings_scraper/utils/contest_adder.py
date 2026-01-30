import json
import urllib.request
import datetime
import logging
import os
import re
from typing import Dict, Any, Optional, List

from bs4 import BeautifulSoup

from draftkings_scraper.constants import (
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
    Utility class for fetching a single contest's data by ID.
    Returns all related data (contest, draft group, payouts, player salaries).
    """

    def __init__(self):
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

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
            self.logger.warning(f"Error extracting draft group ID from page: {e}")

        return None

    def _parse_contest_from_lobby(
        self,
        contest_id: int,
        contest_info: Dict[str, Any],
        contest_detail: Dict[str, Any],
        draft_group_id: int,
    ) -> Dict[str, Any]:
        """Parse contest using complete lobby data."""
        contest_attributes_map = {
            "IsGuaranteed": "guaranteed",
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
            "pd": contest_info.get("pd", {}),
            "po": contest_info.get("po", 0),
            "attr": contest_info.get("attr", []),
            "contest_date": contest_info.get("sdstring", ""),
            "contest_url": f"https://www.draftkings.com/draft/contest/{contest_id}",
            "is_downloaded": False,
            "start_time": convert_datetime(contest_detail.get("contestStartTime", "")),
            "is_final": is_contest_final(contest_detail),
            "is_cancelled": is_contest_cancelled(contest_detail),
        }
        contest.update(atts_dict)

        return contest

    def _parse_contest_from_api(
        self, contest_id: int, contest_detail: Dict[str, Any], draft_group_id: int
    ) -> Dict[str, Any]:
        """Parse contest using partial API data."""
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
            "pd": contest_detail.get("prizeDescription", {}),
            "po": contest_detail.get(
                "prizePool", contest_detail.get("totalPayouts", 0)
            ),
            "attr": attr_dict,
            "guaranteed": "IsGuaranteed" in attr_dict,
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

        return contest

    def _parse_draft_group(self, draft_group: Dict[str, Any], sport: str) -> Dict[str, Any]:
        """Parse draft group from lobby data."""
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

        return dg

    def _parse_minimal_draft_group(
        self, draft_group_id: int, sport: str, contest_detail: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Parse minimal draft group when not found in lobby."""
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

        return draft_group

    def get_contest(self, contest_id: int) -> Dict[str, Any]:
        """
        Get a single contest's data by ID with all related data.

        Args:
            contest_id: The DraftKings contest ID to fetch.

        Returns:
            dict: Contains contest, draft_group, payouts, player_salaries data.
        """
        result = {
            "contest_id": contest_id,
            "status": "not_found",
            "contest": None,
            "draft_group": None,
            "payouts": [],
            "player_salaries": [],
        }

        try:
            contest_response = self.http.get(self.contest_url % contest_id)
            contest_response.raise_for_status()
            contest_data = json.loads(contest_response.text)

            if not contest_data or "contestDetail" not in contest_data:
                self.logger.warning(f"No contest data found for contest {contest_id}")
                return result

            contest_detail = contest_data["contestDetail"]

            sport = contest_detail.get("sport", "").lower()

            if not sport:
                self.logger.warning(f"Could not determine sport for contest {contest_id}")
                result["status"] = "error"
                result["message"] = "Sport not found"
                return result

            self.logger.info(f"Fetching lobby data for sport {sport} to find contest {contest_id}")

            try:
                with urllib.request.urlopen(self.lobby_url % sport) as url:
                    lobby_data = json.loads(url.read().decode())
            except Exception as e:
                self.logger.error(f"Error fetching lobby data for sport {sport}: {str(e)}")
                lobby_data = {"Contests": [], "DraftGroups": [], "GameTypes": []}

            contest_found = False
            contest_info = None
            draft_group_id = None

            for contest in lobby_data.get("Contests", []):
                if contest["id"] == contest_id:
                    contest_found = True
                    contest_info = contest
                    draft_group_id = contest["dg"]
                    break

            if not contest_found:
                if "draftGroupId" in contest_detail:
                    draft_group_id = contest_detail["draftGroupId"]
                else:
                    draft_group_id = self._extract_draft_group_id_from_page(contest_id)
                    if draft_group_id:
                        self.logger.info(f"Extracted draft group ID {draft_group_id} from draft page")
                    else:
                        draft_group_id = 0

            if contest_found:
                result["contest"] = self._parse_contest_from_lobby(
                    contest_id, contest_info, contest_detail, draft_group_id
                )
            else:
                self.logger.warning(f"Contest {contest_id} not found in lobby. Using partial data from contest API.")
                result["contest"] = self._parse_contest_from_api(
                    contest_id, contest_detail, draft_group_id
                )

            if contest_found and draft_group_id:
                for dg in lobby_data.get("DraftGroups", []):
                    if dg["DraftGroupId"] == draft_group_id:
                        result["draft_group"] = self._parse_draft_group(dg, sport)
                        break
            elif draft_group_id and draft_group_id > 0:
                result["draft_group"] = self._parse_minimal_draft_group(draft_group_id, sport, contest_detail)

            if "payoutSummary" in contest_detail and contest_detail["payoutSummary"]:
                payout_scraper = PayoutScraper(sport=sport)
                result["payouts"] = payout_scraper.scrape(contest_ids=[contest_id])

            if draft_group_id and draft_group_id > 0:
                player_salary_scraper = PlayerSalaryScraper(sport=sport)
                result["player_salaries"] = player_salary_scraper.scrape(draft_group_ids=[draft_group_id])

            result["status"] = "success"
            result["sport"] = sport
            result["draft_group_id"] = draft_group_id
            result["from_lobby"] = contest_found

            self.logger.info(f"Fetched contest {contest_id} data.")

            return result

        except Exception as e:
            self.logger.error(f"Error fetching contest {contest_id}: {str(e)}")
            result["status"] = "error"
            result["message"] = str(e)
            return result

    def get_contest_status(self, contest_id: int) -> str:
        """
        Get the status of a contest by ID.

        Args:
            contest_id: The DraftKings contest ID to check.

        Returns:
            str: Status of the contest ('final', 'cancelled', 'upcoming', 'unknown').
        """
        try:
            contest_response = self.http.get(self.contest_url % contest_id)
            contest_response.raise_for_status()
            contest_data = json.loads(contest_response.text)

            if not contest_data or "contestDetail" not in contest_data:
                self.logger.warning(f"No contest data found for contest {contest_id}")
                return "unknown"

            contest_detail = contest_data["contestDetail"]

            if is_contest_cancelled(contest_detail):
                return "cancelled"
            elif is_contest_final(contest_detail):
                return "final"
            else:
                return "upcoming"

        except Exception as e:
            self.logger.error(f"Error fetching contest status for {contest_id}: {str(e)}")
            return "unknown"

def main():
    """CLI entry point for fetching a contest."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch a single DraftKings contest's data."
    )
    parser.add_argument("contest_id", type=int, help="The contest ID to fetch")

    args = parser.parse_args()

    adder = ContestAdder()
    result = adder.get_contest(args.contest_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
