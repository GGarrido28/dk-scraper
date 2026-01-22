import re
import datetime
import logging
import os
import argparse
import time
import requests
from typing import List, Dict, Any

from marshmallow import ValidationError

from draftkings_scraper.schemas import PlayerSalarySchema
from draftkings_scraper.constants import PLAYER_CSV_URL
from draftkings_scraper.http_handler import HTTPHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PlayerSalaryScraper:
    """
    Scraper for DraftKings player salary data.
    Returns validated player salary data.
    """

    def __init__(self, sport: str):
        self.sport = sport
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

        self.player_salary_schema = PlayerSalarySchema()
        self.player_csv_url = PLAYER_CSV_URL

        # HTTP handler with retry logic
        self.http = HTTPHandler()

    def _fetch_player_salaries(
        self, draft_group_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """Fetch player salary data for given draft group IDs."""
        self.logger.info(f"Collecting player csvs for {self.sport}.")

        players_list = []
        skipped_draft_groups = []

        for dg in draft_group_ids:
            try:
                results = self.http.get(self.player_csv_url % dg)
                results.raise_for_status()
                time.sleep(2)
                headers = []
                write_bool = False

                for line in results.text.split("\n"):
                    line = re.sub("^,+", "", line)
                    line = re.sub(",", ";", line)
                    line = re.sub("\\r", "", line)

                    if "Position" in line:
                        headers = line.split(";")
                        write_bool = True
                        continue

                    if write_bool:
                        player_dict = {}
                        values = line.split(";")

                        if len(values) == len(headers) + 1:
                            game_info_idx = headers.index("Game Info")
                            values[game_info_idx] = (
                                values[game_info_idx] + values[game_info_idx + 1]
                            )
                            del values[game_info_idx + 1]

                        if len(values) == len(headers):
                            for count, header in enumerate(headers):
                                player_dict[header] = values[count]
                            player_dict["DraftGroupId"] = dg
                            players_list.append(player_dict)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    skipped_draft_groups.append(dg)
                    self.logger.info(f"Draft group {dg} not found (404). Skipping.")
                else:
                    self.logger.error(f"Error fetching player CSV for draft group {dg}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error processing player CSV for draft group {dg}: {str(e)}")

        validated_players = []
        validation_errors = []

        for player in players_list:
            try:
                p = {
                    "draft_group_id": player["DraftGroupId"],
                    "position": player["Position"],
                    "name_id": player["Name + ID"],
                    "name": player["Name"],
                    "id": int(player["ID"]),
                    "roster_position": player["Roster Position"],
                    "salary": float(player.get("Salary", 0)),
                    "game_info": player["Game Info"],
                    "team_abbrev": player["TeamAbbrev"],
                    "avg_points_per_game": float(player["AvgPointsPerGame"]),
                }

                validated_player = self.player_salary_schema.load(p)
                validated_players.append(validated_player)

            except (KeyError, ValueError, ValidationError) as e:
                validation_errors.append(
                    {
                        "draft_group_id": player.get("DraftGroupId", "Unknown"),
                        "player": player.get("Name", "Unknown"),
                        "errors": str(e),
                    }
                )

        if validation_errors:
            errors_by_dg = {}
            for err in validation_errors:
                dg = err["draft_group_id"]
                errors_by_dg[dg] = errors_by_dg.get(dg, 0) + 1
            for dg, count in errors_by_dg.items():
                self.logger.warning(f"Draft group {dg}: skipped {count} players due to validation errors.")

        self.logger.info(f"Fetched {len(validated_players)} player salaries for {self.sport}.")

        if skipped_draft_groups:
            self.logger.info(f"Skipped {len(skipped_draft_groups)} draft groups due to 404 errors.")

        return validated_players

    def scrape(self, draft_group_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Main scraping method for player salaries.

        Args:
            draft_group_ids: List of draft group IDs to scrape player salaries for.

        Returns:
            list: List of validated player salary dictionaries.
        """
        start_time = datetime.datetime.now()
        players = []

        try:
            self.logger.info(f"Starting player salary scraper for {self.sport}.")

            if draft_group_ids:
                players = self._fetch_player_salaries(draft_group_ids)
            else:
                self.logger.info("No draft group IDs provided.")

            self.logger.info(f"Finished scraping player salaries for {self.sport}.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed player salary scraper: {e}")
            raise e

        return players


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings player salaries for draft group IDs."
    )
    parser.add_argument("sport", type=str, help="Sport code (e.g., NFL, MLB, MMA)")
    parser.add_argument(
        "--draft-group-ids", type=str, help="Comma-separated draft group IDs"
    )
    args = parser.parse_args()

    draft_group_ids = []
    if args.draft_group_ids:
        draft_group_ids = [
            int(dgid.strip()) for dgid in args.draft_group_ids.split(",")
        ]

    scraper = PlayerSalaryScraper(sport=args.sport)
    result = scraper.scrape(draft_group_ids=draft_group_ids)
    print(f"Scraped {len(result)} player salaries")


if __name__ == "__main__":
    main()
