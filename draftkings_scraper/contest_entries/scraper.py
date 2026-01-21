import csv
import time
import shutil
import os
import zipfile
import webbrowser
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from marshmallow import ValidationError

from draftkings_scraper.utils.helpers import return_last_folder_item, move_file
from draftkings_scraper.schemas import ContestEntrySchema, PlayerResultsSchema

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DOWNLOAD_DIRECTORY = os.environ.get("DOWNLOAD_DIRECTORY", "")


class ContestEntriesScraper:
    """
    Scraper for DraftKings contest entries and player results data.
    Returns validated contest entry and player results data.
    Downloads contest standing CSVs from DraftKings.
    """

    def __init__(self):
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = logging.getLogger(__name__)

        # Schemas for validation
        self.contest_entry_schema = ContestEntrySchema()
        self.player_results_schema = PlayerResultsSchema()

        # URLs
        self.lobby_url = "https://myaccount.draftkings.com/login?returnPath=%2flobby"
        self.download_url = (
            "https://www.draftkings.com/contest/exportfullstandingscsv/%s"
        )

        # Directories from environment variables
        self.download_directory = Path(
            os.environ.get("DOWNLOAD_DIRECTORY", DOWNLOAD_DIRECTORY)
        )
        self.csv_directory = Path(os.environ.get("CSV_DIRECTORY", "downloads"))
        self.csv_download_directory = self.csv_directory / "download"
        self.csv_imported_directory = self.csv_directory / "import"
        self.csv_failed_directory = self.csv_directory / "failed"

        # Credentials from environment variables
        self.user_name = os.environ.get("DK_EMAIL")
        self.password = os.environ.get("DK_PASSWORD")

        # Chrome path (Windows default, configurable via env var)
        self.chrome_path = os.environ.get(
            "CHROME_PATH", "C:/Program Files/Google/Chrome/Application/chrome.exe"
        )

        # CSV column mapping
        self.csv_columns = [
            "rank",
            "entry_id",
            "entry_name",
            "time_remaining",
            "points",
            "lineup",
            "empty_column",
            "player",
            "roster_position",
            "percent_drafted",
            "fpts",
        ]

        # State tracking
        self.contest_entries: List[Dict[str, Any]] = []
        self.player_results: List[Dict[str, Any]] = []
        self.successful_contests: List[int] = []
        self.failed_contests: List[Dict[str, Any]] = []
        self.contest_data: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}

    def _validate_directories(self) -> bool:
        """Validate that all required directories exist."""
        directories = [
            self.download_directory,
            self.csv_directory,
            self.csv_download_directory,
            self.csv_imported_directory,
            self.csv_failed_directory,
        ]

        missing_directories = []
        for directory in directories:
            if not directory.exists():
                missing_directories.append(str(directory))

        if missing_directories:
            self.logger.error(f"The following directories do not exist: {', '.join(missing_directories)}. Please create them before running the scraper.")
            return False

        return True

    def _login_draftkings(self) -> Optional[webdriver.Chrome]:
        """Login to DraftKings using Selenium."""
        site_retry = True
        while site_retry:
            try:
                options = webdriver.ChromeOptions()
                options.add_experimental_option("excludeSwitches", ["enable-logging"])
                driver = webdriver.Chrome(
                    ChromeDriverManager().install(), options=options
                )
                driver.get(self.lobby_url)
                time.sleep(10)

                driver.find_element(By.ID, "login-username-input").send_keys(
                    self.user_name
                )
                driver.find_element(By.ID, "login-password-input").send_keys(
                    self.password
                )
                time.sleep(2)
                driver.find_element(By.ID, "login-submit").click()
                time.sleep(2)
                try:
                    driver.find_element(By.ID, "login-submit").click()
                except Exception:
                    pass

                time.sleep(10)
                site_retry = False
                return driver
            except Exception as e:
                self.logger.warning(f"Login attempt failed: {e}. Retrying in 360s...")
                time.sleep(360)

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
        """Download contest CSV files from DraftKings."""
        self.logger.info("Starting DraftKings Contest Results download.")

        # Clean up directories
        for file in os.listdir(self.csv_imported_directory):
            if ".csv" in str(file) or ".zip" in str(file):
                os.remove(self.csv_imported_directory / file)

        for file in os.listdir(self.csv_failed_directory):
            if ".csv" in str(file) or ".zip" in str(file):
                os.remove(self.csv_failed_directory / file)

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
                            count = 3
                        else:
                            self.logger.info(f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder")
                            move_file(
                                file_download,
                                self.download_directory,
                                self.csv_download_directory,
                            )
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

    def _unzip_files(self) -> None:
        """Unzip downloaded contest files."""
        self.logger.info("Preparing to unzip all the files.")

        for file in os.listdir(self.csv_download_directory):
            if zipfile.is_zipfile(self.csv_download_directory / file):
                with zipfile.ZipFile(self.csv_download_directory / file) as item:
                    item.extractall(self.csv_download_directory)

        self.logger.info("Unzipped zip files.")

    def _clean_entry(self, row: Dict[str, str], contest_id: int) -> None:
        """Clean and validate a single contest entry row."""
        if row["entry_id"] != "":
            if "(" in row["entry_name"]:
                user = row["entry_name"].split("(")[0].strip()
                entry = row["entry_name"].split("(")[1].split("/")[0].strip()
                total = (
                    row["entry_name"].split("(")[1].split("/")[1].split(")")[0].strip()
                )
            else:
                user = row["entry_name"]
                entry = 1
                total = 1
            try:
                contest_entry = {
                    "contest_id": contest_id,
                    "entry_id": int(float(row["entry_id"])),
                    "entry_name": user,
                    "entry": int(entry),
                    "total_entries": int(total),
                    "lineup_rank": int(float(row["rank"])),
                    "points": float(row["points"]),
                    "lineup": row["lineup"] if row["lineup"] != "" else None,
                }

                validated_entry = self.contest_entry_schema.load(contest_entry)
                self.contest_entries.append(validated_entry)

            except ValidationError as err:
                self.logger.warning(f"Validation error for contest {contest_id} entry {row['entry_id']}: {err.messages}")
                failed_contest = {
                    "contest_id": contest_id,
                    "reason": f"Validation error: {err.messages}",
                }
                self.failed_contests.append(failed_contest)
            except Exception as e:
                self.logger.warning(f"Error with contest {contest_id} and entry {row['entry_id']}: {e}")
                failed_contest = {
                    "contest_id": contest_id,
                    "reason": "Error with cleaning contest entry.",
                }
                self.failed_contests.append(failed_contest)

        try:
            if row["player"] != "":
                player_result = {
                    "contest_id": contest_id,
                    "player": row["player"].strip(),
                    "roster_position": row["roster_position"].strip(),
                    "percent_drafted": float(row["percent_drafted"].strip("%")),
                    "fpts": float(row["fpts"]),
                }

                validated_result = self.player_results_schema.load(player_result)
                self.player_results.append(validated_result)

        except ValidationError as err:
            self.logger.warning(f"Validation error for contest {contest_id} player: {err.messages}")
        except Exception as e:
            self.logger.warning(f"Error with contest {contest_id} and player cleaning: {e}")
            failed_contest = {
                "contest_id": contest_id,
                "reason": "Error with cleaning player results.",
            }
            self.failed_contests.append(failed_contest)

        if row["empty_column"] != "":
            self.logger.warning(f"Contest {contest_id} and entry {row['entry_id']} has an empty column filled {row['empty_column']}")
            failed_contest = {
                "contest_id": contest_id,
                "reason": "Contest has an empty column filled.",
            }
            self.failed_contests.append(failed_contest)

    def _process_contest_results(self, contest_id: int) -> None:
        """Process contest entries and player results."""
        self.contest_data[contest_id] = {
            "entries": list(self.contest_entries),
            "player_results": list(self.player_results),
        }

        if len(self.contest_entries) > 0:
            self.logger.info(f"Processed {len(self.contest_entries)} entries for contest {contest_id}")

        if len(self.player_results) > 0:
            self.logger.info(f"Processed {len(self.player_results)} player results for contest {contest_id}")

    def _move_csv(self, file: str, contest_id: int) -> None:
        """Move processed CSV to imported directory."""
        move_file(file, self.csv_download_directory, self.csv_imported_directory)
        self.logger.info(f"Contest {contest_id} moved to CSV Imported folder.")
        self.successful_contests.append(contest_id)

    def _import_logs(self) -> None:
        """Log import summary."""
        if len(self.successful_contests) > 0:
            self.logger.info(f"Successfully processed {len(self.successful_contests)} contests")

        if len(self.failed_contests) > 0:
            self.logger.warning(f"Failed to process {len(self.failed_contests)} contests")

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

    def _clean_csv(self) -> None:
        """Process downloaded CSV files."""
        for file in os.listdir(self.csv_download_directory):
            if ".csv" in file and "contest-standings" in file:
                contest_id = file.split("-")[-1]
                contest_id = contest_id.replace(".csv", "")
                try:
                    contest_id = contest_id.split(" ")[0]
                except Exception:
                    pass

                contest_id = int(contest_id)
                try:
                    is_empty = False
                    with open(self.csv_download_directory / str(file), mode="r") as f:
                        csv_reader = csv.DictReader(f, fieldnames=self.csv_columns)
                        try:
                            next(csv_reader)
                            contest_data = [row for row in csv_reader]
                        except StopIteration:
                            is_empty = True
                            f.close()
                        if is_empty:
                            self.logger.warning(f"Contest {contest_id} is empty.")
                            shutil.move(
                                self.csv_download_directory / file,
                                self.csv_failed_directory / file,
                            )
                            self.logger.info(f"Contest {contest_id} moved to CSV Failed folder.")
                            failed_contest = {
                                "contest_id": contest_id,
                                "reason": f"Contest csv is empty.",
                            }
                            self.failed_contests.append(failed_contest)
                            continue
                except Exception as e:
                    failed_contest = {
                        "contest_id": contest_id,
                        "reason": f"Error with cleaning contest: {e}",
                    }
                    self.failed_contests.append(failed_contest)
                    self.logger.warning(f"Error with contest {contest_id}: {e}")
                    shutil.move(
                        self.csv_download_directory / file,
                        self.csv_failed_directory / file,
                    )
                    self.logger.info(f"Contest {contest_id} moved to CSV Failed folder.")
                    continue

                if (
                    len(contest_data) == 0
                    or type(contest_data) is None
                    or type(contest_data) == str
                ):
                    self.logger.warning(f"Contest {contest_id} is unfilled")
                else:
                    self.contest_entries = []
                    self.player_results = []
                    for row in contest_data:
                        self._clean_entry(row, contest_id)
                    self._process_contest_results(contest_id)
                    self._move_csv(file, contest_id)

    def _remove_downloads(self) -> None:
        """Remove downloaded files."""
        self.logger.info("Removing downloads.")

        for file in os.listdir(self.csv_download_directory):
            if ".csv" in str(file) or ".zip" in str(file):
                os.remove(self.csv_download_directory / file)

    def crash_recovery(self) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
        """Recover from a crash by processing already downloaded files."""
        if not self._validate_directories():
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        self.logger.info("Starting crash recovery.")

        processed_contest_ids: set = set()

        files_to_process = []
        for file in os.listdir(self.download_directory):
            if (
                ".csv" in str(file) or ".zip" in str(file)
            ) and "contest-standings" in file:
                files_to_process.append(file)

        files_to_process.sort(key=lambda x: len(x))

        for file in files_to_process:
            contest_id_part = file.split("-")[-1].split(".")[0].strip()

            if "(" in contest_id_part:
                contest_id = contest_id_part.split("(")[0].strip()
            else:
                contest_id = contest_id_part

            try:
                contest_id = int(contest_id)

                if contest_id in processed_contest_ids:
                    self.logger.info(f"Skipping duplicate file for contest {contest_id}: {file}")
                    continue

                processed_contest_ids.add(contest_id)

                self.logger.info(f"Processing contest {contest_id} file: {file}")

                try:
                    if os.path.exists(self.csv_download_directory / file):
                        os.remove(self.csv_download_directory / file)
                    move_file(
                        file, self.download_directory, self.csv_download_directory
                    )
                except Exception as e:
                    self.logger.warning(f"Error moving file {file}: {e}")
                    failed_contest = {
                        "contest_id": contest_id,
                        "reason": f"Error moving file during crash recovery: {e}",
                    }
                    self.failed_contests.append(failed_contest)

            except ValueError as e:
                self.logger.warning(f"Could not parse contest ID from filename {file}: {e}")
                continue

        try:
            self._unzip_files()
            self._clean_csv()
            self._import_logs()
            self._remove_downloads()

            self.logger.info("Crash recovery completed successfully.")
        except Exception as e:
            self.logger.error(f"Error during crash recovery: {e}")
            raise e

        return self.contest_data

    def reprocess_imports(self) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
        """Reprocess files from the imports directory."""
        if not self._validate_directories():
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        self.logger.info("Starting reprocess of imports directory.")

        files_to_process = []
        for file in os.listdir(self.csv_imported_directory):
            if (
                ".csv" in str(file) or ".zip" in str(file)
            ) and "contest-standings" in file:
                files_to_process.append(file)

        if not files_to_process:
            self.logger.info("No files found in imports directory to reprocess.")
            return self.contest_data

        self.logger.info(f"Found {len(files_to_process)} files to reprocess.")

        for file in files_to_process:
            try:
                if os.path.exists(self.csv_download_directory / file):
                    os.remove(self.csv_download_directory / file)
                move_file(file, self.csv_imported_directory, self.csv_download_directory)
                self.logger.info(f"Moved {file} to download directory for reprocessing.")
            except Exception as e:
                self.logger.warning(f"Error moving file {file}: {e}")

        try:
            self._unzip_files()
            self._clean_csv()
            self._import_logs()
            self._remove_downloads()

            self.logger.info("Reprocess imports completed successfully.")
        except Exception as e:
            self.logger.error(f"Error during reprocess imports: {e}")
            raise e

        return self.contest_data

    def scrape(
        self, contest_ids: Optional[List[int]] = None
    ) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
        """Main scraping method for contest entries."""
        if not self._validate_directories():
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        start_time = datetime.now()

        try:
            self.logger.info("Starting contest entries scraper.")

            self._download_contest_csv(contest_ids)
            self._unzip_files()
            self._clean_csv()
            self._import_logs()
            self._remove_downloads()

            self.logger.info("Finished scraping contest entries.")

            elapsed_time = datetime.now() - start_time
            self.logger.info(f"Total time elapsed: {elapsed_time}")

        except Exception as e:
            self.logger.error(f"Failed contest entries scraper: {e}")
            raise e

        return self.contest_data


def main():
    parser = argparse.ArgumentParser(description="Scrape DraftKings contest entries.")
    parser.add_argument("--contest-ids", type=str, help="Comma-separated contest IDs")
    parser.add_argument(
        "--crash-recovery", action="store_true", help="Run crash recovery mode"
    )
    parser.add_argument(
        "--reprocess-imports",
        action="store_true",
        help="Reprocess files from the imports directory",
    )
    args = parser.parse_args()

    scraper = ContestEntriesScraper()

    if args.crash_recovery:
        result = scraper.crash_recovery()
        print(f"Recovered {len(result)} contests")
        return

    if args.reprocess_imports:
        result = scraper.reprocess_imports()
        print(f"Reprocessed {len(result)} contests")
        return

    if args.contest_ids:
        contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]
    else:
        contest_ids = []

    if not contest_ids:
        logger.info("No contests provided; ending script.")
        return

    logger.info(f"Processing {len(contest_ids)} contests.")
    result = scraper.scrape(contest_ids=contest_ids)
    print(f"Scraped {len(result)} contests")


if __name__ == "__main__":
    main()
