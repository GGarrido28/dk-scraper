import datetime
import logging
import os
import argparse
from typing import List, Dict, Any, Optional

from marshmallow import ValidationError

from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.schemas import GameSetSchema

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GameSetsScraper:
    """
    Scraper for DraftKings game sets data.
    Returns validated game set data including competitions and game styles.
    Uses lobby data from ContestsScraper.
    """

    def __init__(self, sport: str):
        """
        Initialize the GameSetsScraper.

        Args:
            sport: Sport code (e.g., 'NFL', 'MLB', 'CS')
        """
        self.sport = sport
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

        self.game_set_schema = GameSetSchema()
        self.contests_scraper = ContestsScraper(sport=sport)
        self.game_set_keys = []

    def _parse_game_sets(
        self,
        raw_game_sets: List[Dict[str, Any]],
        tags: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Parse and validate game sets data from DraftKings API.

        Args:
            raw_game_sets: Raw game sets from lobby data.
            tags: Optional list of tags to filter by (e.g., 'Featured').

        Returns:
            list: List of validated game set dictionaries.
        """
        self.logger.info(f"Parsing Game Sets for {self.sport}.")

        game_sets = []
        validation_errors = []
        self.game_set_keys = []

        for game_set in raw_game_sets:
            tag = game_set.get("Tag")
            if tags and tag not in tags:
                continue

            try:
                validated_game_set = self.game_set_schema.load(game_set)
                game_sets.append(validated_game_set)
                self.game_set_keys.append(game_set["GameSetKey"])
            except ValidationError as err:
                validation_errors.append(
                    {
                        "game_set_key": game_set.get("GameSetKey"),
                        "errors": err.messages,
                    }
                )
                self.logger.warning(f"Validation error for game_set {game_set.get('GameSetKey')}: {err.messages}")

        if validation_errors:
            self.logger.warning(f"Skipped {len(validation_errors)} game sets due to validation errors.")

        self.logger.info(f"Parsed {len(game_sets)} game sets for {self.sport}.")

        return game_sets

    def scrape(
        self,
        lobby_data: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for game sets.

        Args:
            lobby_data: Optional pre-fetched lobby data. If None, fetches from API.
            tags: Optional list of tags to filter by (e.g., 'Featured').

        Returns:
            list: List of validated game set dictionaries.
        """
        start_time = datetime.datetime.now()
        game_sets = []

        try:
            self.logger.info(f"Starting game sets scraper for {self.sport}.")

            if lobby_data is None:
                lobby_data = self.contests_scraper.fetch_lobby_data()

            if "GameSets" in lobby_data and len(lobby_data["GameSets"]) > 0:
                game_sets = self._parse_game_sets(
                    lobby_data["GameSets"],
                    tags=tags,
                )
            else:
                self.logger.info(f"No game sets found in Lobby for {self.sport}.")

            self.logger.info(f"Finished scraping game sets for {self.sport}.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed game sets scraper: {e}")
            raise e

        return game_sets


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings game sets for a specific sport."
    )
    parser.add_argument("sport", type=str, help="Sport code (e.g., NFL, MLB, CS)")
    parser.add_argument(
        "--tags",
        type=str,
        help="Comma-separated list of tags to filter by (e.g., 'Featured')",
    )
    args = parser.parse_args()

    tags = None
    if args.tags:
        tags = [t.strip() for t in args.tags.split(",")]

    scraper = GameSetsScraper(sport=args.sport)
    result = scraper.scrape(tags=tags)
    print(f"Scraped {len(result)} game sets")

    for game_set in result:
        competitions = game_set.get("competitions", [])
        game_styles = game_set.get("game_styles", [])
        print(f"  - {game_set['game_set_key']}: {len(competitions)} competitions, {len(game_styles)} game styles")


if __name__ == "__main__":
    main()
