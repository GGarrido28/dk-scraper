import time
import os
import webbrowser
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv

from draftkings_scraper.utils.helpers import return_last_folder_item, move_file
from draftkings_scraper.constants import CONTEST_STANDINGS_CSV_URL

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DOWNLOAD_DIRECTORY = os.environ.get("DOWNLOAD_DIRECTORY", "")


class ContestEntriesScraper:
    """
    Scraper for downloading DraftKings contest CSV files.
    Downloads contest standing CSVs and moves them to the download directory.
    """

    def __init__(self):
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

        # URLs
        self.download_url = CONTEST_STANDINGS_CSV_URL

        # Directories from environment variables
        self.download_directory = Path(
            os.environ.get("DOWNLOAD_DIRECTORY", DOWNLOAD_DIRECTORY)
        )
        self.csv_directory = Path(os.environ.get("CSV_DIRECTORY", "downloads"))
        self.csv_download_directory = self.csv_directory / "download"

        # Chrome path (Windows default, configurable via env var)
        self.chrome_path = os.environ.get(
            "CHROME_PATH", "C:/Program Files/Google/Chrome/Application/chrome.exe"
        )

        # State tracking
        self.successful_contests: List[int] = []
        self.failed_contests: List[Dict[str, Any]] = []

    def _validate_directories(self) -> bool:
        """Validate that all required directories exist."""
        directories = [
            self.download_directory,
            self.csv_directory,
            self.csv_download_directory,
        ]

        missing_directories = []
        for directory in directories:
            if not directory.exists():
                missing_directories.append(str(directory))

        if missing_directories:
            self.logger.error(f"The following directories do not exist: {', '.join(missing_directories)}. Please create them before running the scraper.")
            return False

        return True

    def _wait_for_downloads(self, timeout: int = 300) -> bool:
        """Wait for all downloads to complete by checking for .crdownload files."""
        self.logger.info(f"Waiting for downloads to complete (timeout: {timeout}s)")

        start_time = time.time()
        while time.time() - start_time < timeout:
            crdownload_files = [
                f
                for f in os.listdir(self.download_directory)
                if f.endswith(".crdownload")
            ]

            if not crdownload_files:
                self.logger.info("All downloads completed successfully")
                return True

            self.logger.info(f"Found {len(crdownload_files)} incomplete downloads, waiting...")
            time.sleep(5)

        remaining = [
            f for f in os.listdir(self.download_directory) if f.endswith(".crdownload")
        ]
        if remaining:
            self.logger.warning(f"Download timeout: {len(remaining)} files still incomplete: {remaining[:3]}...")

            for file in remaining:
                try:
                    os.remove(os.path.join(self.download_directory, file))
                    self.logger.info(f"Removed incomplete download: {file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove {file}: {e}")

        return False

    def _download_contest_csv(self, contest_ids: List[int]) -> None:
        """Download contest CSV files from DraftKings and move to csv_download_directory."""
        self.logger.info("Starting DraftKings Contest Results download.")

        if len(contest_ids) == 0:
            self.logger.info("No Contests found to download; ending script.")
            return
        else:
            self.logger.info(f"Found {len(contest_ids)} contests to download.")

        self.logger.info("Starting CSV Downloads.")

        # Check for already downloaded files
        if len(os.listdir(self.csv_download_directory)) >= 2:
            for file in os.listdir(self.csv_download_directory):
                if ".csv" in file or "zip" in file:
                    contest_id = file.split("-")[2]
                    contest_id = contest_id.split(".")[0]
                    try:
                        contest_id = contest_id.split(" ")[0]
                    except Exception:
                        pass
                    if int(contest_id) in contest_ids:
                        contest_ids.remove(int(contest_id))

        # Download contests
        webbrowser.register(
            "chrome", None, webbrowser.BackgroundBrowser(self.chrome_path)
        )

        for i, contest_id in enumerate(contest_ids, 1):
            self.logger.info(f"Downloading contest {i}/{len(contest_ids)}: {contest_id}")

            webbrowser.open(self.download_url % contest_id)

            download_found = False
            for attempt in range(30):
                time.sleep(2)
                for file in os.listdir(self.download_directory):
                    if f"contest-standings-{contest_id}" in file and not file.endswith(
                        ".crdownload"
                    ):
                        self.logger.info(f"Contest {contest_id} downloaded successfully")
                        download_found = True
                        break
                if download_found:
                    break

            if not download_found:
                self.logger.warning(f"Contest {contest_id} download may have timed out")

            if i < len(contest_ids):
                time.sleep(3)

        self._wait_for_downloads(timeout=120)

        try:
            subprocess.run(
                ["taskkill", "/im", "chrome.exe", "/f"],
                capture_output=True,
                check=False,
            )
        except FileNotFoundError:
            subprocess.run(["pkill", "-f", "chrome"], capture_output=True, check=False)
        time.sleep(5)

        # Move downloaded files to csv_download_directory
        for contest in contest_ids:
            file_download = return_last_folder_item(self.download_directory, "contest")
            count = 0
            if file_download is None:
                while count < 2:
                    file_download = return_last_folder_item(
                        self.download_directory, "contest"
                    )
                    if file_download is None:
                        count += 1
                        self.logger.info(f"Contest {contest} not found in folder, retry attempt {count}")
                    else:
                        if os.path.exists(self.csv_download_directory / file_download):
                            self.logger.info(f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder")
                            os.remove(self.csv_download_directory / file_download)
                            move_file(
                                file_download,
                                self.download_directory,
                                self.csv_download_directory,
                            )
                            self.successful_contests.append(contest)
                            count = 3
                        else:
                            self.logger.info(f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder")
                            move_file(
                                file_download,
                                self.download_directory,
                                self.csv_download_directory,
                            )
                            self.successful_contests.append(contest)
                            count = 3
                self.logger.warning(f"Contest {contest} not found in folder, retry attempt exceeded.")
                failed_contest = {
                    "contest_id": contest,
                    "reason": "File not found in folder - retry attempt exceeded.",
                }
                self.failed_contests.append(failed_contest)
            else:
                if os.path.exists(self.download_directory / file_download):
                    self.logger.info(f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder")
                    try:
                        os.remove(self.csv_download_directory / file_download)
                    except FileNotFoundError:
                        pass
                    move_file(
                        file_download,
                        self.download_directory,
                        self.csv_download_directory,
                    )
                    self.successful_contests.append(contest)
                else:
                    self.logger.warning(f"Contest {contest} doesnt exist in csv directory, moving from download to CSV Downloaded folder")
                    failed_contest = {
                        "contest_id": contest,
                        "reason": "File doesnt exist in csv directory.",
                    }
                    self.failed_contests.append(failed_contest)
                    move_file(
                        file_download,
                        self.download_directory,
                        self.csv_download_directory,
                    )

    def _import_logs(self) -> None:
        """Log import summary."""
        if len(self.successful_contests) > 0:
            self.logger.info(f"Successfully downloaded {len(self.successful_contests)} contests")

        if len(self.failed_contests) > 0:
            self.logger.warning(f"Failed to download {len(self.failed_contests)} contests")

            reasons: Dict[str, int] = {}
            for contest in self.failed_contests:
                if contest["reason"] not in reasons:
                    reasons[contest["reason"]] = 1
                else:
                    reasons[contest["reason"]] += 1

            for reason in reasons.keys():
                self.logger.warning(f"Reason: {reason} - {reasons[reason]}")

            for contest in self.failed_contests:
                self.logger.warning(f"Contest {contest['contest_id']} was in the failed list with reason: {contest['reason']}")

    def scrape(
        self, contest_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Main method for downloading contest CSVs.

        Args:
            contest_ids: List of contest IDs to download.

        Returns:
            dict: Contains successful_contests and failed_contests lists.
        """
        if not self._validate_directories():
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        start_time = datetime.now()

        try:
            self.logger.info("Starting contest entries downloader.")

            if contest_ids:
                self._download_contest_csv(contest_ids)
            else:
                self.logger.info("No contest IDs provided.")

            self._import_logs()

            self.logger.info("Finished downloading contest entries.")

            elapsed_time = datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed contest entries downloader: {e}")
            raise e

        return {
            "successful_contests": self.successful_contests,
            "failed_contests": self.failed_contests,
        }


def main():
    parser = argparse.ArgumentParser(description="Download DraftKings contest CSVs.")
    parser.add_argument("--contest-ids", type=str, help="Comma-separated contest IDs")
    args = parser.parse_args()

    scraper = ContestEntriesScraper()

    if args.contest_ids:
        contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]
    else:
        contest_ids = []

    if not contest_ids:
        logger.info("No contests provided; ending script.")
        return

    logger.info(f"Downloading {len(contest_ids)} contests.")
    result = scraper.scrape(contest_ids=contest_ids)
    print(f"Downloaded {len(result['successful_contests'])} contests")


if __name__ == "__main__":
    main()
