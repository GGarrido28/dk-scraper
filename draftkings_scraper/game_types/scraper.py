import json
import datetime
import logging
import os
import argparse
from typing import List, Dict, Any, Optional

from marshmallow import ValidationError

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.schemas import GameTypeSchema

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GameTypesScraper:
    """
    Scraper for DraftKings game types data.
    Populates the draftkings.game_types table.
    Uses lobby data from ContestsScraper.
    """

    def __init__(self, sport: str):
        """
        Initialize the GameTypesScraper.

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

        # Schema for validation
        self.game_type_schema = GameTypeSchema()

        # Initialize contests scraper for fetching lobby data
        self.contests_scraper = ContestsScraper(sport=sport)

    def _update_game_types(
        self, raw_game_types: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Update game_types table with data from DraftKings API.

        Returns:
            list: List of validated game type dictionaries that were inserted.
        """
        msg = f"Updating Game Types for {self.sport}."
        self.logger.log(level="info", message=msg)

        game_types = []
        validation_errors = []

        for game_type in raw_game_types:
            gt = {
                "game_type_id": game_type["GameTypeId"],
                "name": game_type["Name"],
                "description": game_type["Description"],
                "tag": None if game_type["Tag"] == "" else game_type["Tag"],
                "sport_id": game_type["SportId"],
                "draft_type": game_type["DraftType"],
                "game_style": game_type["GameStyle"],
            }

            # Validate with schema
            try:
                validated_game_type = self.game_type_schema.load(gt)
                game_types.append(validated_game_type)
            except ValidationError as err:
                validation_errors.append(
                    {"game_type_id": game_type["GameTypeId"], "errors": err.messages}
                )
                msg = f"Validation error for game_type {game_type['GameTypeId']}: {err.messages}"
                self.logger.log(level="warning", message=msg)

        if validation_errors:
            msg = (
                f"Skipped {len(validation_errors)} game types due to validation errors."
            )
            self.logger.log(level="warning", message=msg)

        if game_types:
            game_type_cols = list(game_types[0].keys())
            self.draftkings_connection.insert_rows(
                "game_types",
                game_type_cols,
                game_types,
                contains_dicts=True,
                update=True,
            )
            msg = f"Updated {len(game_types)} game types for {self.sport}."
            self.logger.log(level="info", message=msg)

        return game_types

    def scrape(
        self, lobby_data: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for game types.
        Uses lobby data (from ContestsScraper) to update the game_types table.

        Args:
            lobby_data: Optional pre-fetched lobby data. If None, fetches from API.

        Returns:
            list: List of validated game type dictionaries that were inserted.
        """
        start_time = datetime.datetime.now()
        game_types = []

        try:
            msg = f"Starting game types scraper for {self.sport}."
            self.logger.log(level="info", message=msg)

            # Fetch lobby data if not provided
            if lobby_data is None:
                lobby_data = self.contests_scraper.fetch_lobby_data()

            if "GameTypes" in lobby_data and len(lobby_data["GameTypes"]) > 0:
                # Update game_types table
                game_types = self._update_game_types(lobby_data["GameTypes"])
            else:
                msg = f"No game types found in Lobby for {self.sport}."
                self.logger.log(level="info", message=msg)

            msg = f"Finished scraping game types for {self.sport}."
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed game types scraper: {e}"
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
                    alert_name=f"Error processing game types data for {self.sport}",
                    alert_description=f"Error processing game types data: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.game_types",
                )
            self._close_sql_connections()

        return game_types

    def _close_sql_connections(self):
        """Close all database connections."""
        self.logger.close_logger()
        self.draftkings_connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings game types for a specific sport."
    )
    parser.add_argument(
        "sport", type=str, help="Sport code (e.g., NFL, MLB, MMA, GOLF, CFB)"
    )

    args = parser.parse_args()

    scraper = GameTypesScraper(sport=args.sport)
    scraper.scrape()


if __name__ == "__main__":
    main()
