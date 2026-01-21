import datetime
import logging
import os
import argparse
from typing import Dict, Any, List, Optional

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.game_types import GameTypesScraper
from draftkings_scraper.draft_groups import DraftGroupsScraper
from draftkings_scraper.payout import PayoutScraper
from draftkings_scraper.player_salary import PlayerSalaryScraper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DraftKingsOrchestrator:
    """
    Orchestrates the full DraftKings scraping pipeline for a sport.

    Uses the contest_scrape table to determine:
    - Which sports are enabled
    - Which game_type_ids to filter by
    - Which slate_types to filter by (e.g., 'Main', 'Night')

    Pipeline order:
    1. Fetch scrape config from contest_scrape table
    2. Fetch lobby data (shared across scrapers)
    3. Scrape draft groups (filtered by game_type_ids and slate_types)
    4. Scrape contests (filtered by draft_group_ids from step 3)
    5. Scrape game types
    6. Scrape payouts (for contest_ids from step 4)
    7. Scrape player salaries (for draft_group_ids from step 3)
    8. Update contest attributes (is_final, is_cancelled, start_time) for existing contests
    """

    def __init__(self, sport: str):
        """
        Initialize the orchestrator.

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

        # Database connection for fetching config
        self.database = "defaultdb"
        self.schema = "draftkings"
        self.draftkings_connection = PostgresManager(
            "digital_ocean", self.database, self.schema, return_logging=False
        )

    def _get_scrape_config(self) -> Optional[Dict[str, Any]]:
        """
        Fetch scrape configuration from contest_scrape table.

        Returns:
            dict with 'slate_types' and 'game_type_ids', or None if not found/disabled.
        """
        q = f"SELECT * FROM draftkings.contest_scrape WHERE sport = '{self.sport}'"
        results = self.draftkings_connection.execute(q)

        if not results:
            msg = f"No contest_scrape config found for {self.sport}"
            self.logger.log(level="warning", message=msg)
            return None

        row = results[0]
        if not row.get("enabled", False):
            msg = f"Sport {self.sport} is disabled in contest_scrape"
            self.logger.log(level="info", message=msg)
            return None

        # Parse slate_types (comma-separated string)
        slate_types = []
        if row.get("slate_types"):
            slate_types = [s.strip() for s in row["slate_types"].split(",")]

        # Parse game_type_ids (JSON object with 'game_type_ids' key)
        game_type_ids = []
        if row.get("game_type_ids"):
            game_type_ids = row["game_type_ids"].get("game_type_ids", [])

        return {
            "slate_types": slate_types,
            "game_type_ids": game_type_ids,
        }

    def run(
        self,
        skip_contests: bool = False,
        skip_game_types: bool = False,
        skip_draft_groups: bool = False,
        skip_payouts: bool = False,
        skip_player_salaries: bool = False,
        only_update_attributes: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the full scraping pipeline.

        Args:
            skip_contests: Skip contests scraping
            skip_game_types: Skip game types scraping
            skip_draft_groups: Skip draft groups scraping
            skip_payouts: Skip payouts scraping
            skip_player_salaries: Skip player salaries scraping
            only_update_attributes: Only run the update_attributes step, skip everything else

        Returns:
            dict: Results from each scraper stage
        """
        start_time = datetime.datetime.now()
        results = {
            "sport": self.sport,
            "contests": [],
            "game_types": [],
            "draft_groups": [],
            "payouts": [],
            "player_salaries": [],
            "errors": [],
        }

        try:
            msg = f"Starting DraftKings orchestrator for {self.sport}"
            self.logger.log(level="info", message=msg)

            contests_scraper = ContestsScraper(sport=self.sport)

            # If only updating attributes, skip everything else
            if only_update_attributes:
                try:
                    msg = "Updating contest attributes..."
                    self.logger.log(level="info", message=msg)

                    updated = contests_scraper.update_attributes()
                    results["contest_attributes_updated"] = len(updated)

                    msg = f"Updated attributes for {len(updated)} contests"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error updating contest attributes: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append(
                        {"stage": "contest_attributes", "error": str(e)}
                    )
                return results

            # Step 0: Get scrape configuration from contest_scrape table
            scrape_config = self._get_scrape_config()
            if scrape_config is None:
                msg = f"Skipping {self.sport} - not configured or disabled"
                self.logger.log(level="info", message=msg)
                return results

            game_type_ids = scrape_config["game_type_ids"]
            slate_types = scrape_config["slate_types"]

            msg = f"Scrape config: game_type_ids={game_type_ids}, slate_types={slate_types}"
            self.logger.log(level="info", message=msg)

            # Step 1: Fetch lobby data once (shared across scrapers)
            msg = "Fetching lobby data..."
            self.logger.log(level="info", message=msg)

            lobby_data = contests_scraper.fetch_lobby_data()

            if not lobby_data or not lobby_data.get("Contests"):
                msg = (
                    f"No lobby data found for {self.sport}. Sport may be in offseason."
                )
                self.logger.log(level="info", message=msg)
                return results

            msg = f"Lobby data fetched: {len(lobby_data.get('Contests', []))} contests, {len(lobby_data.get('DraftGroups', []))} draft groups"
            self.logger.log(level="info", message=msg)

            # Step 2: Scrape draft groups FIRST (to get filtered draft_group_list)
            draft_group_ids = []
            if not skip_draft_groups:
                try:
                    msg = "Scraping draft groups..."
                    self.logger.log(level="info", message=msg)

                    draft_groups_scraper = DraftGroupsScraper(sport=self.sport)
                    results["draft_groups"] = draft_groups_scraper.scrape(
                        lobby_data=lobby_data,
                        game_type_ids=game_type_ids,
                        slate_types=slate_types,
                    )
                    draft_group_ids = draft_groups_scraper.draft_group_list

                    msg = f"Scraped {len(results['draft_groups'])} draft groups"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error scraping draft groups: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append({"stage": "draft_groups", "error": str(e)})

            # Step 3: Scrape contests (filtered by draft_group_ids)
            contest_ids = []
            if not skip_contests:
                try:
                    msg = "Scraping contests..."
                    self.logger.log(level="info", message=msg)

                    scrape_result = contests_scraper.scrape(
                        lobby_data=lobby_data,
                        draft_group_ids=draft_group_ids,
                    )
                    results["contests"] = scrape_result.get("contests", [])
                    contest_ids = [c["contest_id"] for c in results["contests"]]

                    msg = f"Scraped {len(results['contests'])} contests"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error scraping contests: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append({"stage": "contests", "error": str(e)})

            # Step 4: Scrape game types
            if not skip_game_types:
                try:
                    msg = "Scraping game types..."
                    self.logger.log(level="info", message=msg)

                    game_types_scraper = GameTypesScraper(sport=self.sport)
                    results["game_types"] = game_types_scraper.scrape(
                        lobby_data=lobby_data
                    )

                    msg = f"Scraped {len(results['game_types'])} game types"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error scraping game types: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append({"stage": "game_types", "error": str(e)})

            # Step 5: Scrape payouts (requires contest_ids)
            if not skip_payouts and contest_ids:
                try:
                    msg = f"Scraping payouts for {len(contest_ids)} contests..."
                    self.logger.log(level="info", message=msg)

                    payout_scraper = PayoutScraper(sport=self.sport)
                    results["payouts"] = payout_scraper.scrape(contest_ids=contest_ids)

                    msg = f"Scraped {len(results['payouts'])} payouts"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error scraping payouts: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append({"stage": "payouts", "error": str(e)})

            # Step 6: Scrape player salaries (requires draft_group_ids)
            if not skip_player_salaries and draft_group_ids:
                try:
                    msg = f"Scraping player salaries for {len(draft_group_ids)} draft groups..."
                    self.logger.log(level="info", message=msg)

                    player_salary_scraper = PlayerSalaryScraper(sport=self.sport)
                    results["player_salaries"] = player_salary_scraper.scrape(
                        draft_group_ids=draft_group_ids
                    )

                    msg = f"Scraped {len(results['player_salaries'])} player salaries"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error scraping player salaries: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append(
                        {"stage": "player_salaries", "error": str(e)}
                    )

            # Step 7: Update contest attributes (is_final, is_cancelled, start_time)
            if not skip_contests:
                try:
                    msg = "Updating contest attributes..."
                    self.logger.log(level="info", message=msg)

                    updated = contests_scraper.update_attributes()
                    results["contest_attributes_updated"] = len(updated)

                    msg = f"Updated attributes for {len(updated)} contests"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Error updating contest attributes: {e}"
                    self.logger.log(level="error", message=msg)
                    results["errors"].append(
                        {"stage": "contest_attributes", "error": str(e)}
                    )

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Orchestrator completed for {self.sport} in {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Orchestrator failed for {self.sport}: {e}"
            self.logger.log(level="error", message=msg)
            results["errors"].append({"stage": "orchestrator", "error": str(e)})
            raise e

        finally:
            if self.logger.warning_logs or self.logger.error_logs:
                logs = sorted(
                    list(set(self.logger.warning_logs))
                    + list(set(self.logger.error_logs))
                )
                logs_str = ",".join(logs)
                self.logger.check_alert_log(
                    alert_name=f"Error in DraftKings orchestrator for {self.sport}",
                    alert_description=f"Orchestrator errors: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings",
                )
            self.logger.close_logger()

        return results


def get_enabled_sports() -> List[str]:
    """
    Fetch list of enabled sports from contest_scrape table.

    Returns:
        list: List of enabled sport codes.
    """
    db = PostgresManager("digital_ocean", "defaultdb", "draftkings", return_logging=False)
    try:
        q = "SELECT sport FROM draftkings.contest_scrape WHERE enabled = true"
        results = db.execute(q)
        return [row["sport"] for row in results]
    finally:
        db.close()


def run_all_sports(
    sports: Optional[List[str]] = None,
    skip_contests: bool = False,
    skip_game_types: bool = False,
    skip_draft_groups: bool = False,
    skip_payouts: bool = False,
    skip_player_salaries: bool = False,
    only_update_attributes: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    Run the orchestrator for multiple sports.

    Args:
        sports: List of sport codes. If None, fetches enabled sports from contest_scrape table.
        skip_contests: Skip contests scraping
        skip_game_types: Skip game types scraping
        skip_draft_groups: Skip draft groups scraping
        skip_payouts: Skip payouts scraping
        skip_player_salaries: Skip player salaries scraping
        only_update_attributes: Only run the update_attributes step

    Returns:
        dict: Results keyed by sport code
    """
    # If only updating attributes, run once (not per-sport) since contests table
    # doesn't have a sport column - update_attributes fetches all contests that need updates
    if only_update_attributes:
        logger.info("Updating contest attributes (runs once for all sports)")
        # Use a generic sport just to create the scraper instance
        contests_scraper = ContestsScraper(sport="NFL")
        try:
            updated = contests_scraper.update_attributes()
            logger.info(f"Updated attributes for {len(updated)} contests")
            return {"all": {"contest_attributes_updated": len(updated), "errors": []}}
        except Exception as e:
            logger.error(f"Error updating contest attributes: {e}")
            return {"all": {"contest_attributes_updated": 0, "errors": [{"stage": "contest_attributes", "error": str(e)}]}}

    if sports is None:
        sports = get_enabled_sports()
        logger.info(f"Enabled sports from contest_scrape: {sports}")

    all_results = {}
    for sport in sports:
        logger.info(f"Running orchestrator for {sport}")
        orchestrator = DraftKingsOrchestrator(sport=sport)
        all_results[sport] = orchestrator.run(
            skip_contests=skip_contests,
            skip_game_types=skip_game_types,
            skip_draft_groups=skip_draft_groups,
            skip_payouts=skip_payouts,
            skip_player_salaries=skip_player_salaries,
        )

    return all_results


def main():
    parser = argparse.ArgumentParser(
        description="Run the DraftKings scraping pipeline for one or more sports."
    )
    parser.add_argument(
        "sport",
        type=str,
        nargs="?",
        help="Sport code (e.g., NFL, MLB, MMA). If not provided, runs all sports in SPORTS_WITH_DB.",
    )
    parser.add_argument(
        "--all", action="store_true", help="Run for all sports in SPORTS_WITH_DB"
    )
    parser.add_argument(
        "--skip-contests", action="store_true", help="Skip contests scraping"
    )
    parser.add_argument(
        "--skip-game-types", action="store_true", help="Skip game types scraping"
    )
    parser.add_argument(
        "--skip-draft-groups", action="store_true", help="Skip draft groups scraping"
    )
    parser.add_argument(
        "--skip-payouts", action="store_true", help="Skip payouts scraping"
    )
    parser.add_argument(
        "--skip-player-salaries",
        action="store_true",
        help="Skip player salaries scraping",
    )
    parser.add_argument(
        "--only-update-attributes",
        action="store_true",
        help="Only update contest attributes (is_final, is_cancelled, start_time), skip all other steps",
    )

    args = parser.parse_args()

    if args.all or not args.sport:
        results = run_all_sports(
            skip_contests=args.skip_contests,
            skip_game_types=args.skip_game_types,
            skip_draft_groups=args.skip_draft_groups,
            skip_payouts=args.skip_payouts,
            skip_player_salaries=args.skip_player_salaries,
            only_update_attributes=args.only_update_attributes,
        )
        for key, result in results.items():
            if args.only_update_attributes:
                logger.info(f"Updated {result.get('contest_attributes_updated', 0)} contest attributes")
            else:
                logger.info(f"{key}: Contests={len(result['contests'])}, Game Types={len(result['game_types'])}, Draft Groups={len(result['draft_groups'])}, Payouts={len(result['payouts'])}, Player Salaries={len(result['player_salaries'])}")
            if result["errors"]:
                logger.error(f"{key} Errors: {result['errors']}")
    else:
        orchestrator = DraftKingsOrchestrator(sport=args.sport)
        result = orchestrator.run(
            skip_contests=args.skip_contests,
            skip_game_types=args.skip_game_types,
            skip_draft_groups=args.skip_draft_groups,
            skip_payouts=args.skip_payouts,
            skip_player_salaries=args.skip_player_salaries,
            only_update_attributes=args.only_update_attributes,
        )
        if args.only_update_attributes:
            logger.info(f"{args.sport}: Updated {result.get('contest_attributes_updated', 0)} contest attributes")
        else:
            logger.info(f"{args.sport}: Contests={len(result['contests'])}, Game Types={len(result['game_types'])}, Draft Groups={len(result['draft_groups'])}, Payouts={len(result['payouts'])}, Player Salaries={len(result['player_salaries'])}")
        if result["errors"]:
            logger.error(f"{args.sport} Errors: {result['errors']}")


if __name__ == "__main__":
    main()
