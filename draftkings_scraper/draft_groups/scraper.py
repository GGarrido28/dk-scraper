import datetime
import logging
import os
import argparse
from typing import List, Dict, Any, Optional

from marshmallow import ValidationError

from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.schemas import DraftGroupSchema

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DraftGroupsScraper:
    """
    Scraper for DraftKings draft groups data.
    Returns validated draft group data.
    Uses lobby data from ContestsScraper.
    """

    def __init__(self, sport: str):
        self.sport = sport
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

        self.draft_group_schema = DraftGroupSchema()
        self.contests_scraper = ContestsScraper(sport=sport)
        self.draft_group_list = []

    def _parse_draft_groups(
        self,
        raw_draft_groups: List[Dict[str, Any]],
        game_type_ids: Optional[List[int]] = None,
        slate_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Parse and validate draft groups data from DraftKings API."""
        self.logger.info(f"Parsing Draft Groups for {self.sport}.")

        draft_groups = []
        validation_errors = []
        self.draft_group_list = []

        for draft_group in raw_draft_groups:
            if game_type_ids and draft_group["GameTypeId"] not in game_type_ids:
                continue

            contest_start_time_suffix = draft_group.get("ContestStartTimeSuffix")
            if contest_start_time_suffix:
                contest_start_time_suffix = contest_start_time_suffix.strip()

            if slate_types and contest_start_time_suffix not in slate_types:
                continue

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

            try:
                validated_draft_group = self.draft_group_schema.load(dg)
                draft_groups.append(validated_draft_group)
                self.draft_group_list.append(draft_group["DraftGroupId"])
            except ValidationError as err:
                validation_errors.append(
                    {
                        "draft_group_id": draft_group["DraftGroupId"],
                        "errors": err.messages,
                    }
                )
                self.logger.warning(f"Validation error for draft_group {draft_group['DraftGroupId']}: {err.messages}")

        if validation_errors:
            self.logger.warning(f"Skipped {len(validation_errors)} draft groups due to validation errors.")

        self.logger.info(f"Parsed {len(draft_groups)} draft groups for {self.sport}.")

        return draft_groups

    def scrape(
        self,
        lobby_data: Optional[Dict[str, Any]] = None,
        game_type_ids: Optional[List[int]] = None,
        slate_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for draft groups.

        Args:
            lobby_data: Optional pre-fetched lobby data. If None, fetches from API.
            game_type_ids: List of game type IDs to filter by. If empty/None, no filtering.
            slate_types: List of slate types to filter by. If empty/None, no filtering.

        Returns:
            list: List of validated draft group dictionaries.
        """
        start_time = datetime.datetime.now()
        draft_groups = []

        try:
            self.logger.info(f"Starting draft groups scraper for {self.sport}.")

            if lobby_data is None:
                lobby_data = self.contests_scraper.fetch_lobby_data()

            if "DraftGroups" in lobby_data and len(lobby_data["DraftGroups"]) > 0:
                draft_groups = self._parse_draft_groups(
                    lobby_data["DraftGroups"],
                    game_type_ids=game_type_ids,
                    slate_types=slate_types,
                )
            else:
                self.logger.info(f"No draft groups found in Lobby for {self.sport}.")

            self.logger.info(f"Finished scraping draft groups for {self.sport}.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed draft groups scraper: {e}")
            raise e

        return draft_groups


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings draft groups for a specific sport."
    )
    parser.add_argument("sport", type=str, help="Sport code (e.g., NFL, MLB, MMA)")
    args = parser.parse_args()

    scraper = DraftGroupsScraper(sport=args.sport)
    result = scraper.scrape()
    print(f"Scraped {len(result)} draft groups")


if __name__ == "__main__":
    main()
