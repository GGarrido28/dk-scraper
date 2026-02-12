import datetime
import logging
import argparse
from typing import List, Dict, Any

from marshmallow import ValidationError

from draftkings_scraper.constants import SPORTS_URL
from draftkings_scraper.http_handler import HTTPHandler
from draftkings_scraper.schemas import SportSchema

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SportScraper:
    """
    Scraper for DraftKings sports data.
    Returns validated sport data from the DraftKings API.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.sport_schema = SportSchema()
        self.http = HTTPHandler()

    def _parse_sports(
        self, raw_sports: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse and validate sports data from DraftKings API."""
        self.logger.info("Parsing sports data.")

        sports = []
        validation_errors = []

        for sport in raw_sports:
            s = {
                "sport_id": sport["sportId"],
                "full_name": sport["fullName"],
                "sort_order": sport["sortOrder"],
                "has_public_contests": sport["hasPublicContests"],
                "is_enabled": sport["isEnabled"],
                "region_full_sport_name": sport["regionFullSportName"],
                "region_abbreviated_sport_name": sport["regionAbbreviatedSportName"],
            }

            try:
                validated_sport = self.sport_schema.load(s)
                sports.append(validated_sport)
            except ValidationError as err:
                validation_errors.append(
                    {"sport_id": sport["sportId"], "errors": err.messages}
                )
                self.logger.warning(
                    f"Validation error for sport {sport['sportId']}: {err.messages}"
                )

        if validation_errors:
            self.logger.warning(
                f"Skipped {len(validation_errors)} sports due to validation errors."
            )

        self.logger.info(f"Parsed {len(sports)} sports.")

        return sports

    def scrape(self) -> List[Dict[str, Any]]:
        """
        Main scraping method for sports.
        Fetches sports data from the DraftKings API and returns validated results.

        Returns:
            list: List of validated sport dictionaries.
        """
        start_time = datetime.datetime.now()
        sports = []

        try:
            self.logger.info("Starting sports scraper.")

            response = self.http.get(SPORTS_URL)
            response.raise_for_status()
            data = response.json()

            raw_sports = data.get("sports", [])

            if raw_sports:
                sports = self._parse_sports(raw_sports)
            else:
                self.logger.info("No sports found in API response.")

            self.logger.info("Finished scraping sports.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed sports scraper: {e}")
            raise e

        return sports


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings sports data."
    )

    args = parser.parse_args()

    scraper = SportScraper()
    result = scraper.scrape()
    print(f"Scraped {len(result)} sports")


if __name__ == "__main__":
    main()
