import json
import datetime
import logging
import os
import argparse
from typing import List, Dict, Any, Optional

from marshmallow import ValidationError

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager
from numpy import rint

from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.schemas import DraftGroupSchema, draft_groups

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DraftGroupsScraper:
    """
    Scraper for DraftKings draft groups data.
    Populates the draftkings.draft_groups table.
    Uses lobby data from ContestsScraper.
    """

    def __init__(self, sport: str):
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

        self.draft_group_schema = DraftGroupSchema()
        self.contests_scraper = ContestsScraper(sport=sport)
        self.draft_group_list = []

    def _update_draft_groups(
        self,
        raw_draft_groups: List[Dict[str, Any]],
        game_type_ids: Optional[List[int]] = None,
        slate_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Update draft_groups table with data from DraftKings API.

        Args:
            raw_draft_groups: Raw draft group data from lobby API.
            game_type_ids: List of game type IDs to filter by. If empty, no filtering.
            slate_types: List of slate types to filter by (e.g., ['Main', 'Night']).
                        If empty, no filtering.

        Returns:
            list: List of validated draft group dictionaries that were inserted.
        """
        msg = f"Updating Draft Groups for {self.sport}."
        self.logger.log(level="info", message=msg)

        draft_groups = []
        validation_errors = []
        self.draft_group_list = []

        for draft_group in raw_draft_groups:
            # Filter by game_type_ids if provided
            if game_type_ids and draft_group["GameTypeId"] not in game_type_ids:
                continue

            contest_start_time_suffix = draft_group.get("ContestStartTimeSuffix")
            if contest_start_time_suffix:
                contest_start_time_suffix = contest_start_time_suffix.strip()

            # Filter by slate_types if provided
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
                msg = f"Validation error for draft_group {draft_group['DraftGroupId']}: {err.messages}"
                self.logger.log(level="warning", message=msg)

        if validation_errors:
            msg = f"Skipped {len(validation_errors)} draft groups due to validation errors."
            self.logger.log(level="warning", message=msg)

        if draft_groups:
            draft_group_cols = list(draft_groups[0].keys())
            self.draftkings_connection.insert_rows(
                "draft_groups",
                draft_group_cols,
                draft_groups,
                contains_dicts=True,
                update=True,
            )
            msg = f"Updated {len(draft_groups)} draft groups for {self.sport}."
            self.logger.log(level="info", message=msg)

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
            list: List of validated draft group dictionaries that were inserted.
        """
        start_time = datetime.datetime.now()
        draft_groups = []

        try:
            msg = f"Starting draft groups scraper for {self.sport}."
            self.logger.log(level="info", message=msg)

            if lobby_data is None:
                lobby_data = self.contests_scraper.fetch_lobby_data()

            if "DraftGroups" in lobby_data and len(lobby_data["DraftGroups"]) > 0:
                draft_groups = self._update_draft_groups(
                    lobby_data["DraftGroups"],
                    game_type_ids=game_type_ids,
                    slate_types=slate_types,
                )
            else:
                msg = f"No draft groups found in Lobby for {self.sport}."
                self.logger.log(level="info", message=msg)

            msg = f"Finished scraping draft groups for {self.sport}."
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed draft groups scraper: {e}"
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
                    alert_name=f"Error processing draft groups data for {self.sport}",
                    alert_description=f"Error processing draft groups data: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.draft_groups",
                )
            self._close_sql_connections()

        return draft_groups

    def _close_sql_connections(self):
        self.logger.close_logger()
        self.draftkings_connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings draft groups for a specific sport."
    )
    parser.add_argument("sport", type=str, help="Sport code (e.g., NFL, MLB, MMA)")
    args = parser.parse_args()

    scraper = DraftGroupsScraper(sport=args.sport)
    scraper.scrape()


if __name__ == "__main__":
    main()
