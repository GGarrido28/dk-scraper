import datetime
import logging
import os
import argparse
from typing import Dict, Any, List, Optional

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

    Pipeline order:
    1. Fetch lobby data (shared across scrapers)
    2. Scrape draft groups (filtered by game_type_ids and slate_types)
    3. Scrape contests (filtered by draft_group_ids from step 2)
    4. Scrape game types
    5. Scrape payouts (for contest_ids from step 3)
    6. Scrape player salaries (for draft_group_ids from step 2)
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
        self.logger = logging.getLogger(__name__)

    def run(
        self,
        game_type_ids: Optional[List[int]] = None,
        slate_types: Optional[List[str]] = None,
        skip_contests: bool = False,
        skip_game_types: bool = False,
        skip_draft_groups: bool = False,
        skip_payouts: bool = False,
        skip_player_salaries: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the full scraping pipeline.

        Args:
            game_type_ids: List of game type IDs to filter draft groups by.
            slate_types: List of slate types to filter draft groups by.
            skip_contests: Skip contests scraping
            skip_game_types: Skip game types scraping
            skip_draft_groups: Skip draft groups scraping
            skip_payouts: Skip payouts scraping
            skip_player_salaries: Skip player salaries scraping

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
            self.logger.info(f"Starting DraftKings orchestrator for {self.sport}")

            contests_scraper = ContestsScraper(sport=self.sport)

            self.logger.info("Fetching lobby data...")

            lobby_data = contests_scraper.fetch_lobby_data()

            if not lobby_data or not lobby_data.get("Contests"):
                self.logger.info(f"No lobby data found for {self.sport}. Sport may be in offseason.")
                return results

            self.logger.info(f"Lobby data fetched: {len(lobby_data.get('Contests', []))} contests, {len(lobby_data.get('DraftGroups', []))} draft groups")

            draft_group_ids = []
            if not skip_draft_groups:
                try:
                    self.logger.info("Scraping draft groups...")

                    draft_groups_scraper = DraftGroupsScraper(sport=self.sport)
                    results["draft_groups"] = draft_groups_scraper.scrape(
                        lobby_data=lobby_data,
                        game_type_ids=game_type_ids,
                        slate_types=slate_types,
                    )
                    draft_group_ids = draft_groups_scraper.draft_group_list

                    self.logger.info(f"Scraped {len(results['draft_groups'])} draft groups")
                except Exception as e:
                    self.logger.error(f"Error scraping draft groups: {e}")
                    results["errors"].append({"stage": "draft_groups", "error": str(e)})

            contest_ids = []
            if not skip_contests:
                try:
                    self.logger.info("Scraping contests...")

                    scrape_result = contests_scraper.scrape(
                        lobby_data=lobby_data,
                        draft_group_ids=draft_group_ids,
                    )
                    results["contests"] = scrape_result.get("contests", [])
                    contest_ids = [c["contest_id"] for c in results["contests"]]

                    self.logger.info(f"Scraped {len(results['contests'])} contests")
                except Exception as e:
                    self.logger.error(f"Error scraping contests: {e}")
                    results["errors"].append({"stage": "contests", "error": str(e)})

            if not skip_game_types:
                try:
                    self.logger.info("Scraping game types...")

                    game_types_scraper = GameTypesScraper(sport=self.sport)
                    results["game_types"] = game_types_scraper.scrape(
                        lobby_data=lobby_data
                    )

                    self.logger.info(f"Scraped {len(results['game_types'])} game types")
                except Exception as e:
                    self.logger.error(f"Error scraping game types: {e}")
                    results["errors"].append({"stage": "game_types", "error": str(e)})

            if not skip_payouts and contest_ids:
                try:
                    self.logger.info(f"Scraping payouts for {len(contest_ids)} contests...")

                    payout_scraper = PayoutScraper(sport=self.sport)
                    results["payouts"] = payout_scraper.scrape(contest_ids=contest_ids)

                    self.logger.info(f"Scraped {len(results['payouts'])} payouts")
                except Exception as e:
                    self.logger.error(f"Error scraping payouts: {e}")
                    results["errors"].append({"stage": "payouts", "error": str(e)})

            if not skip_player_salaries and draft_group_ids:
                try:
                    self.logger.info(f"Scraping player salaries for {len(draft_group_ids)} draft groups...")

                    player_salary_scraper = PlayerSalaryScraper(sport=self.sport)
                    results["player_salaries"] = player_salary_scraper.scrape(
                        draft_group_ids=draft_group_ids
                    )

                    self.logger.info(f"Scraped {len(results['player_salaries'])} player salaries")
                except Exception as e:
                    self.logger.error(f"Error scraping player salaries: {e}")
                    results["errors"].append(
                        {"stage": "player_salaries", "error": str(e)}
                    )

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Orchestrator completed for {self.sport} in {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Orchestrator failed for {self.sport}: {e}")
            results["errors"].append({"stage": "orchestrator", "error": str(e)})
            raise e

        return results


def run_all_sports(
    sports: List[str],
    game_type_ids: Optional[List[int]] = None,
    slate_types: Optional[List[str]] = None,
    skip_contests: bool = False,
    skip_game_types: bool = False,
    skip_draft_groups: bool = False,
    skip_payouts: bool = False,
    skip_player_salaries: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    Run the orchestrator for multiple sports.

    Args:
        sports: List of sport codes.
        game_type_ids: List of game type IDs to filter by.
        slate_types: List of slate types to filter by.
        skip_contests: Skip contests scraping
        skip_game_types: Skip game types scraping
        skip_draft_groups: Skip draft groups scraping
        skip_payouts: Skip payouts scraping
        skip_player_salaries: Skip player salaries scraping

    Returns:
        dict: Results keyed by sport code
    """
    all_results = {}
    for sport in sports:
        logger.info(f"Running orchestrator for {sport}")
        orchestrator = DraftKingsOrchestrator(sport=sport)
        all_results[sport] = orchestrator.run(
            game_type_ids=game_type_ids,
            slate_types=slate_types,
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
        help="Sport code (e.g., NFL, MLB, MMA).",
    )
    parser.add_argument(
        "--sports",
        type=str,
        help="Comma-separated list of sport codes to run for multiple sports",
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

    args = parser.parse_args()

    if args.sports:
        sports = [s.strip() for s in args.sports.split(",")]
        results = run_all_sports(
            sports=sports,
            skip_contests=args.skip_contests,
            skip_game_types=args.skip_game_types,
            skip_draft_groups=args.skip_draft_groups,
            skip_payouts=args.skip_payouts,
            skip_player_salaries=args.skip_player_salaries,
        )
        for sport, result in results.items():
            logger.info(f"{sport}: Contests={len(result['contests'])}, Game Types={len(result['game_types'])}, Draft Groups={len(result['draft_groups'])}, Payouts={len(result['payouts'])}, Player Salaries={len(result['player_salaries'])}")
            if result["errors"]:
                logger.error(f"{sport} Errors: {result['errors']}")
    elif args.sport:
        orchestrator = DraftKingsOrchestrator(sport=args.sport)
        result = orchestrator.run(
            skip_contests=args.skip_contests,
            skip_game_types=args.skip_game_types,
            skip_draft_groups=args.skip_draft_groups,
            skip_payouts=args.skip_payouts,
            skip_player_salaries=args.skip_player_salaries,
        )
        logger.info(f"{args.sport}: Contests={len(result['contests'])}, Game Types={len(result['game_types'])}, Draft Groups={len(result['draft_groups'])}, Payouts={len(result['payouts'])}, Player Salaries={len(result['player_salaries'])}")
        if result["errors"]:
            logger.error(f"{args.sport} Errors: {result['errors']}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
