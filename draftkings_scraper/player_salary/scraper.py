import re
import datetime
import logging
import os
import argparse
import time
import requests
from typing import List, Dict, Any

from marshmallow import ValidationError

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.schemas import PlayerSalarySchema
from draftkings_scraper.constants import PLAYER_CSV_URL
from draftkings_scraper.http_handler import HTTPHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PlayerSalaryScraper:
    """
    Scraper for DraftKings player salary data.
    Populates the draftkings.player_salary table.
    Requires draft_group_ids from DraftGroupsScraper.
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

        self.player_salary_schema = PlayerSalarySchema()
        self.player_csv_url = PLAYER_CSV_URL

        # HTTP handler with retry logic
        self.http = HTTPHandler()

    def _update_player_salaries(
        self, draft_group_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """Update player_salary table for given draft group IDs.

        Returns:
            list: List of validated player salary dictionaries that were inserted.
        """
        msg = f"Collecting player csvs for {self.sport}."
        self.logger.log(level="info", message=msg)

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
                    msg = f"Draft group {dg} not found (404). Skipping."
                    self.logger.log(level="data", message=msg)
                else:
                    msg = f"Error fetching player CSV for draft group {dg}: {str(e)}"
                    self.logger.log(level="error", message=msg)
            except Exception as e:
                msg = f"Error processing player CSV for draft group {dg}: {str(e)}"
                self.logger.log(level="error", message=msg)

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
            # Group errors by draft group for clearer logging
            errors_by_dg = {}
            for err in validation_errors:
                dg = err["draft_group_id"]
                errors_by_dg[dg] = errors_by_dg.get(dg, 0) + 1
            for dg, count in errors_by_dg.items():
                msg = f"Draft group {dg}: skipped {count} players due to validation errors."
                self.logger.log(level="warning", message=msg)

        if validated_players:
            self.draftkings_connection.insert_rows(
                "player_salary",
                validated_players[0].keys(),
                validated_players,
                contains_dicts=True,
                update=True,
            )
            msg = f"Imported {len(validated_players)} player salaries for {self.sport}."
            self.logger.log(level="info", message=msg)

        if skipped_draft_groups:
            msg = f"Skipped {len(skipped_draft_groups)} draft groups due to 404 errors."
            self.logger.log(level="info", message=msg)

        return validated_players

    def scrape(self, draft_group_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Main scraping method for player salaries.

        Args:
            draft_group_ids: List of draft group IDs to scrape player salaries for.

        Returns:
            list: List of validated player salary dictionaries that were inserted.
        """
        start_time = datetime.datetime.now()
        players = []

        try:
            msg = f"Starting player salary scraper for {self.sport}."
            self.logger.log(level="info", message=msg)

            if draft_group_ids:
                players = self._update_player_salaries(draft_group_ids)
            else:
                msg = f"No draft group IDs provided."
                self.logger.log(level="info", message=msg)

            msg = f"Finished scraping player salaries for {self.sport}."
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed player salary scraper: {e}"
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
                    alert_name=f"Error processing player salary data for {self.sport}",
                    alert_description=f"Error processing player salary data: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.player_salary",
                )
            self._close_sql_connections()

        return players

    def _close_sql_connections(self):
        self.logger.close_logger()
        self.draftkings_connection.close()


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
    scraper.scrape(draft_group_ids=draft_group_ids)


if __name__ == "__main__":
   main()