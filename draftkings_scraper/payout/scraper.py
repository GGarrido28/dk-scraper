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

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.schemas import PayoutSchema
from draftkings_scraper.constants import DRAFT_URL
from draftkings_scraper.http_handler import HTTPHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PayoutScraper:
    """
    Scraper for DraftKings contest payout data.
    Populates the draftkings.payout table.
    Requires contest_ids from ContestsScraper.
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
                msg = f"Contest {contest_id} not found (404)"
                self.logger.log(level="info", message=msg)
            else:
                msg = f"HTTP error for contest {contest_id}: {str(http_err)}"
                self.logger.log(level="error", message=msg)
            return None

        except Exception as e:
            msg = f"Error scraping contest {contest_id}: {str(e)}"
            self.logger.log(level="error", message=msg)
            return None

    def _scrape_contest_payouts_batch(self, contest_ids, batch_size=10):
        all_payouts = []

        for i in range(0, len(contest_ids), batch_size):
            batch = contest_ids[i : i + batch_size]
            msg = f"Processing batch of {len(batch)} contests ({i+1}-{i+len(batch)} of {len(contest_ids)})"
            self.logger.log(level="info", message=msg)

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
                        msg = f"Error processing contest {contest_id}: {str(e)}"
                        self.logger.log(level="error", message=msg)

            if i + batch_size < len(contest_ids):
                time.sleep(1)

        return all_payouts

    def _update_payouts(self, contest_ids: List[int]) -> List[Dict[str, Any]]:
        """Update payout table for given contest IDs.

        Returns:
            list: List of validated payout dictionaries that were inserted.
        """
        msg = f"Checking for contest payouts."
        self.logger.log(level="info", message=msg)

        # Get existing payouts
        existing_payouts = set()
        if contest_ids:
            for i in range(0, len(contest_ids), 1000):
                chunk = contest_ids[i : i + 1000]
                q = """
                SELECT contest_id
                FROM draftkings.payout
                WHERE contest_id IN %(contest_ids)s
                """
                results = self.draftkings_connection.execute(
                    q, params={"contest_ids": tuple(chunk)}
                )
                existing_payouts.update(r["contest_id"] for r in results)

        contests_to_process = [
            cid for cid in contest_ids if cid not in existing_payouts
        ]

        if not contests_to_process:
            msg = f"No new contest payouts to process."
            self.logger.log(level="info", message=msg)
            return []

        msg = f"Processing payouts for {len(contests_to_process)} contests."
        self.logger.log(level="info", message=msg)

        all_payouts = self._scrape_contest_payouts_batch(contests_to_process)

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
            msg = f"Skipped {len(validation_errors)} payouts due to validation errors."
            self.logger.log(level="warning", message=msg)

        if validated_payouts:
            CHUNK_SIZE = 500
            for i in range(0, len(validated_payouts), CHUNK_SIZE):
                chunk = validated_payouts[i : i + CHUNK_SIZE]
                self.draftkings_connection.insert_rows(
                    "payout", chunk[0].keys(), chunk, contains_dicts=True, update=True
                )

            msg = f"Imported {len(validated_payouts)} payouts."
            self.logger.log(level="info", message=msg)

        if existing_payouts:
            msg = f"Skipped {len(existing_payouts)} existing payouts."
            self.logger.log(level="info", message=msg)

        return validated_payouts

    def _get_contest_ids_for_draft_group(self, draft_group_id: int) -> List[int]:
        """Get contest IDs associated with a draft group from the database.

        Args:
            draft_group_id: The draft group ID to look up contests for.

        Returns:
            list: List of contest IDs for the draft group.
        """
        q = """
        SELECT contest_id
        FROM draftkings.contests
        WHERE draft_group_id = %(draft_group_id)s
        """
        results = self.draftkings_connection.execute(
            q, params={"draft_group_id": draft_group_id}
        )
        return [r["contest_id"] for r in results]

    def scrape(
        self,
        contest_ids: Optional[List[int]] = None,
        draft_group_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for payouts.

        Args:
            contest_ids: List of contest IDs to scrape payouts for.
            draft_group_id: Optional draft group ID to look up contests for.

        Returns:
            list: List of validated payout dictionaries that were inserted.
        """
        start_time = datetime.datetime.now()
        payouts = []

        try:
            msg = f"Starting payout scraper for {self.sport}."
            self.logger.log(level="info", message=msg)

            # Get contest IDs from draft_group_id if provided
            if draft_group_id and not contest_ids:
                contest_ids = self._get_contest_ids_for_draft_group(draft_group_id)
                msg = f"Found {len(contest_ids)} contests for draft group {draft_group_id}."
                self.logger.log(level="info", message=msg)

            if contest_ids:
                payouts = self._update_payouts(contest_ids)
            else:
                msg = f"No contest IDs provided or found."
                self.logger.log(level="info", message=msg)

            msg = f"Finished scraping payouts for {self.sport}."
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed payout scraper: {e}"
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
                    alert_name=f"Error processing payout data for {self.sport}",
                    alert_description=f"Error processing payout data: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.payout",
                )
            self._close_sql_connections()

        return payouts

    def _close_sql_connections(self):
        self.logger.close_logger()
        self.draftkings_connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings payouts for contest IDs."
    )
    parser.add_argument("sport", type=str, help="Sport code (e.g., NFL, MLB, MMA)")
    parser.add_argument("--contest-ids", type=str, help="Comma-separated contest IDs")
    parser.add_argument(
        "--draft-group-id", type=int, help="Draft group ID to look up contests for"
    )
    args = parser.parse_args()

    contest_ids = None
    if args.contest_ids:
        contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]

    scraper = PayoutScraper(sport=args.sport)
    scraper.scrape(contest_ids=contest_ids, draft_group_id=args.draft_group_id)


if __name__ == "__main__":
    main()
