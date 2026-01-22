import datetime
import logging
import os
import argparse
from typing import List, Dict, Any, Optional

from marshmallow import ValidationError

from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.schemas import GameTypeSchema

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GameTypesScraper:
    """
    Scraper for DraftKings game types data.
    Returns validated game type data.
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
        self.logger = logging.getLogger(__name__)

        # Schema for validation
        self.game_type_schema = GameTypeSchema()

        # Initialize contests scraper for fetching lobby data
        self.contests_scraper = ContestsScraper(sport=sport)

    def _parse_game_types(
        self, raw_game_types: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse and validate game types data from DraftKings API."""
        self.logger.info(f"Parsing Game Types for {self.sport}.")

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

            try:
                validated_game_type = self.game_type_schema.load(gt)
                game_types.append(validated_game_type)
            except ValidationError as err:
                validation_errors.append(
                    {"game_type_id": game_type["GameTypeId"], "errors": err.messages}
                )
                self.logger.warning(f"Validation error for game_type {game_type['GameTypeId']}: {err.messages}")

        if validation_errors:
            self.logger.warning(f"Skipped {len(validation_errors)} game types due to validation errors.")

        self.logger.info(f"Parsed {len(game_types)} game types for {self.sport}.")

        return game_types

    def scrape(
        self, lobby_data: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for game types.
        Uses lobby data (from ContestsScraper) to parse game types.

        Args:
            lobby_data: Optional pre-fetched lobby data. If None, fetches from API.

        Returns:
            list: List of validated game type dictionaries.
        """
        start_time = datetime.datetime.now()
        game_types = []

        try:
            self.logger.info(f"Starting game types scraper for {self.sport}.")

            if lobby_data is None:
                lobby_data = self.contests_scraper.fetch_lobby_data()

            if "GameTypes" in lobby_data and len(lobby_data["GameTypes"]) > 0:
                game_types = self._parse_game_types(lobby_data["GameTypes"])
            else:
                self.logger.info(f"No game types found in Lobby for {self.sport}.")

            self.logger.info(f"Finished scraping game types for {self.sport}.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed game types scraper: {e}")
            raise e

        return game_types


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings game types for a specific sport."
    )
    parser.add_argument(
        "sport", type=str, help="Sport code (e.g., NFL, MLB, MMA, GOLF, CFB)"
    )

    args = parser.parse_args()

    scraper = GameTypesScraper(sport=args.sport)
    result = scraper.scrape()
    print(f"Scraped {len(result)} game types")


if __name__ == "__main__":
    main()
