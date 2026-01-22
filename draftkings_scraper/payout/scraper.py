import json
import re
import datetime
import logging
import os
import argparse
import time
import requests
import concurrent.futures
from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup
from marshmallow import ValidationError

from draftkings_scraper.schemas import PayoutSchema
from draftkings_scraper.constants import DRAFT_URL
from draftkings_scraper.http_handler import HTTPHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PayoutScraper:
    """
    Scraper for DraftKings contest payout data.
    Returns validated payout data.
    """

    def __init__(self, sport: str):
        self.sport = sport
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

        self.payout_schema = PayoutSchema()
        self.draft_url = DRAFT_URL

        # HTTP handler with retry logic
        self.http = HTTPHandler()

    def _process_payout_value(self, value_str, payout_type):
        if "ticket" in payout_type.lower():
            return 0
        elif "$" in value_str:
            return float(value_str.replace("$", "").replace(",", ""))
        return value_str

    def _scrape_single_contest_payout(self, contest_id):
        payout_steps = []

        try:
            response = self.http.get(self.draft_url % contest_id)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            script_tags = soup.find_all("script")
            contest_data = None

            for script in script_tags:
                if script.string and "window.mvcVars.contests" in script.string:
                    try:
                        match = re.search(
                            r'window\.mvcVars\.contests\s*=\s*(\{.*?"contestDetail":.*?"errorStatus":\{\}\})',
                            script.string,
                            re.DOTALL,
                        )
                        if match:
                            json_str = match.group(1)
                            detail_match = re.search(
                                r'"contestDetail":(.*?),"errorStatus":',
                                json_str,
                                re.DOTALL,
                            )
                            if detail_match:
                                contest_data = json.loads(
                                    "{"
                                    + f'"contestDetail":{detail_match.group(1)}'
                                    + "}"
                                )
                                break
                    except json.JSONDecodeError:
                        continue

            if not contest_data or "contestDetail" not in contest_data:
                return None

            for payout_step in contest_data["contestDetail"]["payoutSummary"]:
                tiers = payout_step["tierPayoutDescriptions"].items()

                payout_info = {
                    "contest_id": contest_id,
                    "max_position": payout_step["maxPosition"],
                    "min_position": payout_step["minPosition"],
                    "original_tier": payout_step["tierPayoutDescriptions"],
                    "payout_one_type": None,
                    "payout_one_value": None,
                    "payout_two_type": None,
                    "payout_two_value": None,
                }

                tier_items = list(tiers)
                if tier_items:
                    payout_info["payout_one_type"] = tier_items[0][0]
                    payout_info["payout_one_value"] = self._process_payout_value(
                        tier_items[0][1], tier_items[0][0]
                    )

                    if len(tier_items) > 1:
                        payout_info["payout_two_type"] = tier_items[1][0]
                        payout_info["payout_two_value"] = self._process_payout_value(
                            tier_items[1][1], tier_items[1][0]
                        )

                payout_steps.append(payout_info)

            return payout_steps

        except requests.HTTPError as http_err:
            if http_err.response.status_code == 404:
                self.logger.info(f"Contest {contest_id} not found (404)")
            else:
                self.logger.error(f"HTTP error for contest {contest_id}: {str(http_err)}")
            return None

        except Exception as e:
            self.logger.error(f"Error scraping contest {contest_id}: {str(e)}")
            return None

    def _scrape_contest_payouts_batch(self, contest_ids, batch_size=10):
        all_payouts = []

        for i in range(0, len(contest_ids), batch_size):
            batch = contest_ids[i : i + batch_size]
            self.logger.info(f"Processing batch of {len(batch)} contests ({i+1}-{i+len(batch)} of {len(contest_ids)})")

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=batch_size
            ) as executor:
                future_to_contest = {
                    executor.submit(
                        self._scrape_single_contest_payout, contest_id
                    ): contest_id
                    for contest_id in batch
                }

                for future in concurrent.futures.as_completed(future_to_contest):
                    contest_id = future_to_contest[future]
                    try:
                        payouts = future.result()
                        if payouts:
                            all_payouts.extend(payouts)
                    except Exception as e:
                        self.logger.error(f"Error processing contest {contest_id}: {str(e)}")

            if i + batch_size < len(contest_ids):
                time.sleep(1)

        return all_payouts

    def _fetch_payouts(self, contest_ids: List[int]) -> List[Dict[str, Any]]:
        """Fetch payout data for given contest IDs."""
        self.logger.info(f"Fetching payouts for {len(contest_ids)} contests.")

        all_payouts = self._scrape_contest_payouts_batch(contest_ids)

        validated_payouts = []
        validation_errors = []

        for payout in all_payouts:
            try:
                validated_payout = self.payout_schema.load(payout)
                validated_payouts.append(validated_payout)
            except ValidationError as err:
                validation_errors.append(
                    {"contest_id": payout.get("contest_id"), "errors": err.messages}
                )

        if validation_errors:
            self.logger.warning(f"Skipped {len(validation_errors)} payouts due to validation errors.")

        self.logger.info(f"Fetched {len(validated_payouts)} payouts.")

        return validated_payouts

    def scrape(
        self,
        contest_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for payouts.

        Args:
            contest_ids: List of contest IDs to scrape payouts for.

        Returns:
            list: List of validated payout dictionaries.
        """
        start_time = datetime.datetime.now()
        payouts = []

        try:
            self.logger.info(f"Starting payout scraper for {self.sport}.")

            if contest_ids:
                payouts = self._fetch_payouts(contest_ids)
            else:
                self.logger.info("No contest IDs provided.")

            self.logger.info(f"Finished scraping payouts for {self.sport}.")

            elapsed_time = datetime.datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed payout scraper: {e}")
            raise e

        return payouts


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings payouts for contest IDs."
    )
    parser.add_argument("sport", type=str, help="Sport code (e.g., NFL, MLB, MMA)")
    parser.add_argument("--contest-ids", type=str, help="Comma-separated contest IDs")
    args = parser.parse_args()

    contest_ids = None
    if args.contest_ids:
        contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]

    scraper = PayoutScraper(sport=args.sport)
    result = scraper.scrape(contest_ids=contest_ids)
    print(f"Scraped {len(result)} payouts")


if __name__ == "__main__":
    main()
