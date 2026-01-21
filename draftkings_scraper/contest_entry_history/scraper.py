import csv
import time
import webbrowser
import logging
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from marshmallow import ValidationError

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from draftkings_scraper.schemas import ContestHistorySchema
from draftkings_scraper.constants import SPORTS_WITH_DB, CONTEST_HISTORY_CSV_URL

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ContestEntryHistoryScraper:
    """
    Scraper for DraftKings contest entry history data.
    Populates the draftkings.contest_history table.
    Downloads contest history CSV from DraftKings account.
    """

    def __init__(self, sleep_time: int = 120):
        """
        Initialize the ContestEntryHistoryScraper.

        Args:
            sleep_time: Time to wait for download to complete (seconds).
        """
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = LoggerManager(
            self.script_name,
            self.script_path,
            sport=None,
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
        self.contest_history_schema = ContestHistorySchema()

        # URLs
        self.download_url = CONTEST_HISTORY_CSV_URL

        # DraftKings username from environment
        self.dk_username = os.environ.get("DK_USERNAME", "")

        # File settings
        self.file_name = "draftkings-contest-entry-history.csv"
        self.sleep_time = sleep_time

        # Directories from environment variables
        self.download_directory = Path(os.environ.get("DOWNLOAD_DIRECTORY", ""))
        self.csv_directory = Path(os.environ.get("CSV_DIRECTORY", "downloads"))

        # Chrome path from environment or default
        self.chrome_path = os.environ.get(
            "CHROME_PATH", "C:/Program Files/Google/Chrome/Application/chrome.exe"
        )

        # State tracking
        self.entries: List[Dict[str, Any]] = []
        self.validation_errors: List[Dict[str, Any]] = []

    def _validate_directories(self) -> bool:
        """Validate that required directories exist.

        Returns:
            bool: True if all directories exist, False otherwise.
        """
        if not self.download_directory or not self.download_directory.exists():
            msg = f"Download directory does not exist: {self.download_directory}"
            self.logger.log(level="error", message=msg)
            return False

        if not self.csv_directory.exists():
            msg = f"CSV directory does not exist: {self.csv_directory}"
            self.logger.log(level="error", message=msg)
            return False

        return True

    def _download_csv(self) -> bool:
        """Download contest history CSV from DraftKings.

        Returns:
            bool: True if download was initiated successfully.
        """
        msg = "Downloading DraftKings contest history"
        self.logger.log(level="info", message=msg)

        try:
            webbrowser.register(
                "chrome", None, webbrowser.BackgroundBrowser(self.chrome_path)
            )
            webbrowser.get("chrome").open_new(self.download_url)

            msg = f"Waiting {self.sleep_time} seconds for download to complete"
            self.logger.log(level="info", message=msg)
            time.sleep(self.sleep_time)

            msg = "Download wait completed"
            self.logger.log(level="info", message=msg)
            return True

        except Exception as e:
            msg = f"Error initiating download: {e}"
            self.logger.log(level="error", message=msg)
            return False

    def _move_file(self) -> bool:
        """Move downloaded file from download directory to CSV directory.

        Returns:
            bool: True if file was moved successfully.
        """
        source_file = self.download_directory / self.file_name
        target_file = self.csv_directory / self.file_name

        if not source_file.exists():
            msg = f"Source file not found: {source_file}"
            self.logger.log(level="error", message=msg)
            return False

        msg = f"Moving {self.file_name} from {self.download_directory} to {self.csv_directory}"
        self.logger.log(level="info", message=msg)

        try:
            if target_file.exists():
                target_file.unlink()
            source_file.rename(target_file)
            return True
        except Exception as e:
            msg = f"Error moving file: {e}"
            self.logger.log(level="error", message=msg)
            return False

    def _parse_opponent(self, entry_text: str) -> Optional[str]:
        """Parse opponent name from entry text.

        Args:
            entry_text: The entry text from the CSV.

        Returns:
            str or None: Opponent name if found.
        """
        if not self.dk_username or self.dk_username not in entry_text:
            return None

        if "League" in entry_text:
            return None

        entry = entry_text.split("(")[0]
        person_one = entry.split(" vs.")[0]
        person_two = entry.split(" vs.")[1] if " vs." in entry else ""

        if self.dk_username in person_one:
            return person_two.replace(" ", "").strip()
        else:
            return person_one.split(" ")[-1].replace(" ", "").strip()

    def _read_csv(self) -> List[Dict[str, Any]]:
        """Read and parse contest history CSV.

        Returns:
            list: List of parsed contest history entries.
        """
        csv_path = self.csv_directory / self.file_name

        if not csv_path.exists():
            msg = f"CSV file not found: {csv_path}"
            self.logger.log(level="error", message=msg)
            return []

        entries = []
        skipped_leagues = 0

        with open(csv_path, "r", encoding="utf8") as f:
            csv_reader = csv.DictReader(f)
            next(csv_reader, None)  # Skip header row

            for row in csv_reader:
                # Skip league entries
                if "League" in row.get("Entry", ""):
                    skipped_leagues += 1
                    continue

                opponent = self._parse_opponent(row.get("Entry", ""))

                try:
                    entry = {
                        "sport": row["Sport"],
                        "game_type": row["Game_Type"],
                        "entry_id": row["Entry_Key"],
                        "entry": row["Entry"],
                        "opponent": opponent,
                        "contest_id": row["Contest_Key"],
                        "contest_date_est": row["Contest_Date_EST"],
                        "lineup_rank": int(row["Place"]),
                        "points": float(row["Points"]),
                        "winnings_non_ticket": float(
                            row["Winnings_Non_Ticket"].replace("$", "").replace(",", "")
                        ),
                        "winnings_ticket": float(
                            row["Winnings_Ticket"].replace("$", "").replace(",", "")
                        ),
                        "contest_entries": int(row["Contest_Entries"]),
                        "entry_fee": float(
                            row["Entry_Fee"].replace("$", "").replace(",", "")
                        ),
                        "prize_pool": float(
                            row["Prize_Pool"].replace("$", "").replace(",", "")
                        ),
                        "places_paid": int(row["Places_Paid"]),
                    }

                    # Validate with schema
                    validated_entry = self.contest_history_schema.load(entry)
                    entries.append(validated_entry)

                except ValidationError as err:
                    self.validation_errors.append(
                        {"entry_id": row.get("Entry_Key"), "errors": err.messages}
                    )
                except (KeyError, ValueError) as e:
                    msg = f"Error parsing row: {e}"
                    self.logger.log(level="warning", message=msg)
                    self.validation_errors.append(
                        {"entry_id": row.get("Entry_Key"), "errors": str(e)}
                    )

        if skipped_leagues > 0:
            msg = f"Skipped {skipped_leagues} league entries"
            self.logger.log(level="info", message=msg)

        if self.validation_errors:
            msg = f"Skipped {len(self.validation_errors)} entries due to validation errors"
            self.logger.log(level="warning", message=msg)

        msg = f"Parsed {len(entries)} contest history entries"
        self.logger.log(level="info", message=msg)

        return entries

    def _insert_in_batches(
        self, entries: List[Dict[str, Any]], batch_size: int = 5000
    ) -> None:
        """Insert entries in batches to avoid connection timeouts.

        Args:
            entries: List of entries to insert.
            batch_size: Number of entries per batch.
        """
        total_entries = len(entries)
        if total_entries == 0:
            msg = "No entries to insert"
            self.logger.log(level="info", message=msg)
            return

        msg = f"Inserting {total_entries} entries into contest_history in batches of {batch_size}"
        self.logger.log(level="info", message=msg)

        for i in range(0, total_entries, batch_size):
            batch = entries[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_entries + batch_size - 1) // batch_size

            msg = f"Inserting batch {batch_num}/{total_batches} ({len(batch)} entries)"
            self.logger.log(level="info", message=msg)

            self.draftkings_connection.insert_rows(
                "contest_history",
                batch[0].keys(),
                batch,
                contains_dicts=True,
                update=True,
            )

        msg = f"Successfully inserted all {total_entries} entries"
        self.logger.log(level="info", message=msg)

    def _update_sport_databases(self) -> None:
        """Update sport-specific databases with contest history data."""
        for sport in SPORTS_WITH_DB:
            try:
                msg = f"Updating {sport} database with contest history"
                self.logger.log(level="info", message=msg)

                sport_sql = PostgresManager(
                    "digital_ocean", sport.lower(), "draftkings", return_logging=False
                )

                q = """
                    SELECT
                        c.entry_id,
                        c.contest_id,
                        COALESCE(d.draft_group_id, -1) as draft_group_id,
                        c.game_type,
                        c.entry,
                        c.opponent,
                        c.contest_date_est,
                        c.lineup_rank,
                        c.points,
                        c.winnings_non_ticket,
                        c.winnings_ticket,
                        c.contest_entries,
                        c.entry_fee,
                        c.prize_pool,
                        c.places_paid
                    FROM contest_history c
                    LEFT JOIN contests d ON c.contest_id = d.contest_id
                    WHERE c.sport = %(sport)s
                """
                results = self.draftkings_connection.execute(q, params={"sport": sport})

                if results:
                    sport_sql.insert_rows(
                        "contest_history",
                        results[0].keys(),
                        results,
                        contains_dicts=True,
                        update=True,
                    )
                    msg = f"Updated {len(results)} entries in {sport} database"
                    self.logger.log(level="info", message=msg)
                else:
                    msg = f"No entries found for {sport}"
                    self.logger.log(level="info", message=msg)

                sport_sql.close()

            except Exception as e:
                msg = f"Error updating {sport} database: {e}"
                self.logger.log(level="error", message=msg)

    def _close_sql_connections(self) -> None:
        """Close database connections."""
        self.logger.close_logger()
        self.draftkings_connection.close()

    def scrape(
        self, skip_download: bool = False, skip_sport_update: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for contest entry history.

        Args:
            skip_download: Skip downloading and use existing CSV file.
            skip_sport_update: Skip updating sport-specific databases.

        Returns:
            list: List of validated contest history entries that were inserted.
        """
        start_time = datetime.now()
        self.entries = []
        self.validation_errors = []

        try:
            msg = "Starting contest entry history scraper"
            self.logger.log(level="info", message=msg)

            # Validate directories
            if not self._validate_directories():
                raise SystemExit("Required directories do not exist.")

            # Download CSV
            if not skip_download:
                if not self._download_csv():
                    raise Exception("Failed to download CSV")

                if not self._move_file():
                    raise Exception("Failed to move downloaded file")

            # Read and parse CSV
            self.entries = self._read_csv()

            # Insert into database
            if self.entries:
                self._insert_in_batches(self.entries)

                # Update sport-specific databases
                if not skip_sport_update:
                    self._update_sport_databases()

            msg = "Finished scraping contest entry history"
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed contest entry history scraper: {e}"
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
                    alert_name="Error processing contest entry history",
                    alert_description=f"Error processing contest entry history: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.contest_history",
                )
            self._close_sql_connections()

        return self.entries


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DraftKings contest entry history."
    )
    parser.add_argument(
        "--sleep-time",
        type=int,
        default=120,
        help="Time to wait for download to complete (seconds)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading and use existing CSV file",
    )
    parser.add_argument(
        "--skip-sport-update",
        action="store_true",
        help="Skip updating sport-specific databases",
    )

    args = parser.parse_args()

    scraper = ContestEntryHistoryScraper(sleep_time=args.sleep_time)
    scraper.scrape(
        skip_download=args.skip_download, skip_sport_update=args.skip_sport_update
    )


if __name__ == "__main__":
    main()
