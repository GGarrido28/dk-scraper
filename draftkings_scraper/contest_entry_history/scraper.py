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

from draftkings_scraper.schemas import ContestHistorySchema
from draftkings_scraper.constants import CONTEST_HISTORY_CSV_URL

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ContestEntryHistoryScraper:
    """
    Scraper for DraftKings contest entry history data.
    Returns validated contest history data.
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
        self.logger = logging.getLogger(__name__)

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
        """Validate that required directories exist."""
        if not self.download_directory or not self.download_directory.exists():
            self.logger.error(f"Download directory does not exist: {self.download_directory}")
            return False

        if not self.csv_directory.exists():
            self.logger.error(f"CSV directory does not exist: {self.csv_directory}")
            return False

        return True

    def _download_csv(self) -> bool:
        """Download contest history CSV from DraftKings."""
        self.logger.info("Downloading DraftKings contest history")

        try:
            webbrowser.register(
                "chrome", None, webbrowser.BackgroundBrowser(self.chrome_path)
            )
            webbrowser.get("chrome").open_new(self.download_url)

            self.logger.info(f"Waiting {self.sleep_time} seconds for download to complete")
            time.sleep(self.sleep_time)

            self.logger.info("Download wait completed")
            return True

        except Exception as e:
            self.logger.error(f"Error initiating download: {e}")
            return False

    def _move_file(self) -> bool:
        """Move downloaded file from download directory to CSV directory."""
        source_file = self.download_directory / self.file_name
        target_file = self.csv_directory / self.file_name

        if not source_file.exists():
            self.logger.error(f"Source file not found: {source_file}")
            return False

        self.logger.info(f"Moving {self.file_name} from {self.download_directory} to {self.csv_directory}")

        try:
            if target_file.exists():
                target_file.unlink()
            source_file.rename(target_file)
            return True
        except Exception as e:
            self.logger.error(f"Error moving file: {e}")
            return False

    def _parse_opponent(self, entry_text: str) -> Optional[str]:
        """Parse opponent name from entry text."""
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
        """Read and parse contest history CSV."""
        csv_path = self.csv_directory / self.file_name

        if not csv_path.exists():
            self.logger.error(f"CSV file not found: {csv_path}")
            return []

        entries = []
        skipped_leagues = 0

        with open(csv_path, "r", encoding="utf8") as f:
            csv_reader = csv.DictReader(f)
            next(csv_reader, None)

            for row in csv_reader:
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

                    validated_entry = self.contest_history_schema.load(entry)
                    entries.append(validated_entry)

                except ValidationError as err:
                    self.validation_errors.append(
                        {"entry_id": row.get("Entry_Key"), "errors": err.messages}
                    )
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Error parsing row: {e}")
                    self.validation_errors.append(
                        {"entry_id": row.get("Entry_Key"), "errors": str(e)}
                    )

        if skipped_leagues > 0:
            self.logger.info(f"Skipped {skipped_leagues} league entries")

        if self.validation_errors:
            self.logger.warning(f"Skipped {len(self.validation_errors)} entries due to validation errors")

        self.logger.info(f"Parsed {len(entries)} contest history entries")

        return entries

    def scrape(
        self, skip_download: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Main scraping method for contest entry history.

        Args:
            skip_download: Skip downloading and use existing CSV file.

        Returns:
            list: List of validated contest history entries.
        """
        start_time = datetime.now()
        self.entries = []
        self.validation_errors = []

        try:
            self.logger.info("Starting contest entry history scraper")

            if not self._validate_directories():
                raise SystemExit("Required directories do not exist.")

            if not skip_download:
                if not self._download_csv():
                    raise Exception("Failed to download CSV")

                if not self._move_file():
                    raise Exception("Failed to move downloaded file")

            self.entries = self._read_csv()

            self.logger.info("Finished scraping contest entry history")

            elapsed_time = datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed contest entry history scraper: {e}")
            raise e

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

    args = parser.parse_args()

    scraper = ContestEntryHistoryScraper(sleep_time=args.sleep_time)
    result = scraper.scrape(skip_download=args.skip_download)
    print(f"Scraped {len(result)} contest history entries")


if __name__ == "__main__":
    main()
