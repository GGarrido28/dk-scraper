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

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager
from mg.utils.settings import DOWNLOAD_DIRECTORY
from mg.utils.utils import return_last_folder_item, move_file

from draftkings_scraper.schemas import ContestEntrySchema, PlayerResultsSchema

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ContestEntriesScraper:
    """
    Scraper for DraftKings contest entries and player results data.
    Populates the draftkings.contest and draftkings.player_results tables.
    Downloads contest standing CSVs from DraftKings.
    """

    def __init__(self):
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
        """Validate that all required directories exist.

        Returns:
            bool: True if all directories exist, False otherwise.
        """
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
            msg = f"The following directories do not exist: {', '.join(missing_directories)}. Please create them before running the scraper."
            self.logger.log(level="error", message=msg)
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
                logger.warning(f"Login attempt failed: {e}. Retrying in 360s...")
                time.sleep(360)

    def _wait_for_downloads(self, timeout: int = 300) -> bool:
        """Wait for all downloads to complete by checking for .crdownload files."""
        msg = f"Waiting for downloads to complete (timeout: {timeout}s)"
        self.logger.log(level="info", message=msg)

        start_time = time.time()
        while time.time() - start_time < timeout:
            crdownload_files = [
                f
                for f in os.listdir(self.download_directory)
                if f.endswith(".crdownload")
            ]

            if not crdownload_files:
                msg = f"All downloads completed successfully"
                self.logger.log(level="info", message=msg)
                return True

            msg = f"Found {len(crdownload_files)} incomplete downloads, waiting..."
            self.logger.log(level="info", message=msg)
            time.sleep(5)

        remaining = [
            f for f in os.listdir(self.download_directory) if f.endswith(".crdownload")
        ]
        if remaining:
            msg = f"Download timeout: {len(remaining)} files still incomplete: {remaining[:3]}..."
            self.logger.log(level="warning", message=msg)

            for file in remaining:
                try:
                    os.remove(os.path.join(self.download_directory, file))
                    msg = f"Removed incomplete download: {file}"
                    self.logger.log(level="info", message=msg)
                except Exception as e:
                    msg = f"Failed to remove {file}: {e}"
                    self.logger.log(level="warning", message=msg)

        return False

    def _download_contest_csv(self, contest_ids: List[int]) -> None:
        """Download contest CSV files from DraftKings."""
        msg = f"Starting DraftKings Contest Results download."
        self.logger.log(level="info", message=msg)

        # Clean up directories
        for file in os.listdir(self.csv_imported_directory):
            if ".csv" in str(file) or ".zip" in str(file):
                os.remove(self.csv_imported_directory / file)

        for file in os.listdir(self.csv_failed_directory):
            if ".csv" in str(file) or ".zip" in str(file):
                os.remove(self.csv_failed_directory / file)

        if len(contest_ids) == 0:
            msg = f"No Contests found to download; ending script."
            self.logger.log(level="info", message=msg)
            return
        else:
            msg = f"Found {len(contest_ids)} contests to download."
            self.logger.log(level="info", message=msg)

        msg = f"Starting CSV Downloads."
        self.logger.log(level="info", message=msg)

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
            msg = f"Downloading contest {i}/{len(contest_ids)}: {contest_id}"
            self.logger.log(level="info", message=msg)

            webbrowser.open(self.download_url % contest_id)

            download_found = False
            for attempt in range(30):
                time.sleep(2)
                for file in os.listdir(self.download_directory):
                    if f"contest-standings-{contest_id}" in file and not file.endswith(
                        ".crdownload"
                    ):
                        msg = f"Contest {contest_id} downloaded successfully"
                        self.logger.log(level="info", message=msg)
                        download_found = True
                        break
                if download_found:
                    break

            if not download_found:
                msg = f"Contest {contest_id} download may have timed out"
                self.logger.log(level="warning", message=msg)

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
            # taskkill not available (non-Windows), try pkill
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
                        msg = f"Contest {contest} not found in folder, retry attempt {count}"
                        self.logger.log(level="info", message=msg)
                    else:
                        if os.path.exists(self.csv_download_directory / file_download):
                            msg = f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder"
                            self.logger.log(level="info", message=msg)
                            os.remove(self.csv_download_directory / file_download)
                            move_file(
                                file_download,
                                self.download_directory,
                                self.csv_download_directory,
                            )
                            count = 3
                        else:
                            msg = f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder"
                            self.logger.log(level="info", message=msg)
                            move_file(
                                file_download,
                                self.download_directory,
                                self.csv_download_directory,
                            )
                            count = 3
                msg = f"Contest {contest} not found in folder, retry attempt exceeded."
                self.logger.log(level="warning", message=msg)
                failed_contest = {
                    "contest_id": contest,
                    "reason": "File not found in folder - retry attempt exceeded.",
                }
                self.failed_contests.append(failed_contest)
            else:
                if os.path.exists(self.download_directory / file_download):
                    msg = f"Contest {contest} downloaded successfully, moving to CSV Downloaded folder"
                    self.logger.log(level="info", message=msg)
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
                    msg = f"Contest {contest} doesnt exist in csv directory, moving from download to CSV Downloaded folder"
                    self.logger.log(level="warning", message=msg)
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
        msg = f"Preparing to unzip all the files."
        self.logger.log(level="info", message=msg)

        for file in os.listdir(self.csv_download_directory):
            if zipfile.is_zipfile(self.csv_download_directory / file):
                with zipfile.ZipFile(self.csv_download_directory / file) as item:
                    item.extractall(self.csv_download_directory)

        msg = f"Unzipped zip files."
        self.logger.log(level="info", message=msg)

    def _clean_entry(self, row: Dict[str, str], contest_id: int) -> None:
        """Clean and validate a single contest entry row."""
        # Contest Entries
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

                # Validate with schema
                validated_entry = self.contest_entry_schema.load(contest_entry)
                self.contest_entries.append(validated_entry)

            except ValidationError as err:
                msg = f"Validation error for contest {contest_id} entry {row['entry_id']}: {err.messages}"
                self.logger.log(level="warning", message=msg)
                failed_contest = {
                    "contest_id": contest_id,
                    "reason": f"Validation error: {err.messages}",
                }
                self.failed_contests.append(failed_contest)
            except Exception as e:
                msg = (
                    f"Error with contest {contest_id} and entry {row['entry_id']}: {e}"
                )
                self.logger.log(level="warning", message=msg)
                failed_contest = {
                    "contest_id": contest_id,
                    "reason": "Error with cleaning contest entry.",
                }
                self.failed_contests.append(failed_contest)

        # Player Results
        try:
            if row["player"] != "":
                player_result = {
                    "contest_id": contest_id,
                    "player": row["player"].strip(),
                    "roster_position": row["roster_position"].strip(),
                    "percent_drafted": float(row["percent_drafted"].strip("%")),
                    "fpts": float(row["fpts"]),
                }

                # Validate with schema
                validated_result = self.player_results_schema.load(player_result)
                self.player_results.append(validated_result)

        except ValidationError as err:
            msg = f"Validation error for contest {contest_id} player: {err.messages}"
            self.logger.log(level="warning", message=msg)
        except Exception as e:
            msg = f"Error with contest {contest_id} and player cleaning: {e}"
            self.logger.log(level="warning", message=msg)
            failed_contest = {
                "contest_id": contest_id,
                "reason": "Error with cleaning player results.",
            }
            self.failed_contests.append(failed_contest)

        # Player Name overflow check
        if row["empty_column"] != "":
            msg = f"Contest {contest_id} and entry {row['entry_id']} has an empty column filled {row['empty_column']}"
            self.logger.log(level="warning", message=msg)
            failed_contest = {
                "contest_id": contest_id,
                "reason": "Contest has an empty column filled.",
            }
            self.failed_contests.append(failed_contest)

    def _import_contest_results(self, contest_id: int) -> None:
        """Import contest entries and player results to database.

        Args:
            contest_id: The contest ID being processed.
        """
        # Store data in contest_data for return
        self.contest_data[contest_id] = {
            "entries": list(self.contest_entries),
            "player_results": list(self.player_results),
        }

        if len(self.contest_entries) > 0:
            start_timer = time.time()
            msg = f"Inserting {len(self.contest_entries)} rows to contest_entries"
            self.logger.log(level="info", message=msg)
            self.draftkings_connection.insert_rows(
                "contest",
                self.contest_entries[0].keys(),
                self.contest_entries,
                contains_dicts=True,
                update=True,
            )
            end_timer = time.time()
            msg = f"Inserting {len(self.contest_entries)} rows to contest_entries took {end_timer - start_timer:.2f} seconds"
            self.logger.log(level="info", message=msg)

        if len(self.player_results) > 0:
            msg = f"Inserting {len(self.player_results)} rows to player_results"
            self.logger.log(level="info", message=msg)
            self.draftkings_connection.insert_rows(
                "player_results",
                self.player_results[0].keys(),
                self.player_results,
                contains_dicts=True,
                update=True,
            )

    def _update_contest_status(
        self, contest_id: int, is_downloaded: bool, is_csv_empty: bool = False
    ) -> None:
        """Update contest download status in database."""
        q = f"""UPDATE draftkings.contests
                SET is_downloaded = {is_downloaded}, updated_at = NOW(), is_empty = {is_csv_empty}
                WHERE contest_id = {contest_id}
                """
        self.draftkings_connection.execute(q)
        self.draftkings_connection.connection.commit()
        msg = f"Contest {contest_id} updated to is_downloaded = {is_downloaded}"
        self.logger.log(level="info", message=msg)

    def _move_csv(self, file: str, contest_id: int) -> None:
        """Move processed CSV to imported directory."""
        move_file(file, self.csv_download_directory, self.csv_imported_directory)
        msg = f"Contest {contest_id} moved to CSV Imported folder."
        self.logger.log(level="info", message=msg)
        self.successful_contests.append(contest_id)

    def _import_logs(self) -> None:
        """Log import summary."""
        if len(self.successful_contests) > 0:
            msg = f"Successfully downloaded {len(self.successful_contests)} contests"
            self.logger.log(level="info", message=msg)

        if len(self.failed_contests) > 0:
            msg = f"Failed to download {len(self.failed_contests)} contests"
            self.logger.log(level="warning", message=msg)

            reasons: Dict[str, int] = {}
            for contest in self.failed_contests:
                if contest["reason"] not in reasons:
                    reasons[contest["reason"]] = 1
                else:
                    reasons[contest["reason"]] += 1

            for reason in reasons.keys():
                msg = f"Reason: {reason} - {reasons[reason]}"
                self.logger.log(level="warning", message=msg)

            for contest in self.failed_contests:
                msg = f"Contest {contest['contest_id']} was in the failed list with reason: {contest['reason']}"
                self.logger.log(level="warning", message=msg)

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
                            self.logger.log(
                                level="warning",
                                message=f"Contest {contest_id} is empty.",
                            )
                            shutil.move(
                                self.csv_download_directory / file,
                                self.csv_failed_directory / file,
                            )
                            msg = f"Contest {contest_id} moved to CSV Failed folder."
                            self.logger.log(level="info", message=msg)
                            failed_contest = {
                                "contest_id": contest_id,
                                "reason": f"Contest csv is empty.",
                            }
                            self.failed_contests.append(failed_contest)
                            self._update_contest_status(
                                contest_id, is_downloaded=True, is_csv_empty=True
                            )
                            continue
                except Exception as e:
                    msg = f"Error with contest {contest_id}: {e}"
                    failed_contest = {
                        "contest_id": contest_id,
                        "reason": f"Error with cleaning contest: {e}",
                    }
                    self.failed_contests.append(failed_contest)
                    self.logger.log(level="warning", message=msg)
                    self._update_contest_status(contest_id, is_downloaded=False)
                    shutil.move(
                        self.csv_download_directory / file,
                        self.csv_failed_directory / file,
                    )
                    msg = f"Contest {contest_id} moved to CSV Failed folder."
                    self.logger.log(level="info", message=msg)
                    continue

                if (
                    len(contest_data) == 0
                    or type(contest_data) is None
                    or type(contest_data) == str
                ):
                    msg = f"Contest {contest_id} is unfilled"
                    self.logger.log(level="warning", message=msg)
                else:
                    self.contest_entries = []
                    self.player_results = []
                    for row in contest_data:
                        self._clean_entry(row, contest_id)
                    self._import_contest_results(contest_id)
                    self._update_contest_status(contest_id, is_downloaded=True)
                    self._move_csv(file, contest_id)

    def _remove_downloads(self) -> None:
        """Remove downloaded files."""
        msg = f"Removing downloads."
        self.logger.log(level="info", message=msg)

        for file in os.listdir(self.csv_download_directory):
            if ".csv" in str(file) or ".zip" in str(file):
                os.remove(self.csv_download_directory / file)

    def _close_sql_connections(self) -> None:
        """Close database connections."""
        self.logger.close_logger()
        self.draftkings_connection.close()

    def crash_recovery(self) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
        """
        Recover from a crash by processing already downloaded files.

        Returns:
            dict: Contest data organized by contest_id, e.g.:
                {contest_id: {"entries": [...], "player_results": [...]}, ...}
        """
        # Validate directories exist before starting
        if not self._validate_directories():
            self._close_sql_connections()
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        msg = f"Starting crash recovery."
        self.logger.log(level="info", message=msg)

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
                    msg = f"Skipping duplicate file for contest {contest_id}: {file}"
                    self.logger.log(level="info", message=msg)
                    continue

                processed_contest_ids.add(contest_id)

                msg = f"Processing contest {contest_id} file: {file}"
                self.logger.log(level="info", message=msg)

                try:
                    if os.path.exists(self.csv_download_directory / file):
                        os.remove(self.csv_download_directory / file)
                    move_file(
                        file, self.download_directory, self.csv_download_directory
                    )
                except Exception as e:
                    msg = f"Error moving file {file}: {e}"
                    self.logger.log(level="warning", message=msg)
                    failed_contest = {
                        "contest_id": contest_id,
                        "reason": f"Error moving file during crash recovery: {e}",
                    }
                    self.failed_contests.append(failed_contest)

            except ValueError as e:
                msg = f"Could not parse contest ID from filename {file}: {e}"
                self.logger.log(level="warning", message=msg)
                continue

        try:
            self._unzip_files()
            self._clean_csv()
            self._import_logs()
            self._remove_downloads()

            msg = f"Crash recovery completed successfully."
            self.logger.log(level="info", message=msg)
        except Exception as e:
            msg = f"Error during crash recovery: {e}"
            self.logger.log(level="error", message=msg)
            raise e
        finally:
            if self.logger.warning_logs or self.logger.error_logs:
                logs = sorted(
                    list(set(self.logger.warning_logs))
                    + list(set(self.logger.error_logs))
                )
                self.logger.check_alert_log(
                    alert_name=f"Error in crash recovery for DraftKings entries data",
                    alert_description=f"Error during crash recovery: {logs}",
                    review_script=self.script_name,
                    review_table="contest",
                )
            self._close_sql_connections()

        return self.contest_data

    def reprocess_imports(self) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
        """
        Reprocess files from the imports directory.

        Returns:
            dict: Contest data organized by contest_id, e.g.:
                {contest_id: {"entries": [...], "player_results": [...]}, ...}
        """
        # Validate directories exist before starting
        if not self._validate_directories():
            self._close_sql_connections()
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        msg = "Starting reprocess of imports directory."
        self.logger.log(level="info", message=msg)

        # Move files from imported directory back to download directory for processing
        files_to_process = []
        for file in os.listdir(self.csv_imported_directory):
            if (
                ".csv" in str(file) or ".zip" in str(file)
            ) and "contest-standings" in file:
                files_to_process.append(file)

        if not files_to_process:
            msg = "No files found in imports directory to reprocess."
            self.logger.log(level="info", message=msg)
            self._close_sql_connections()
            return self.contest_data

        msg = f"Found {len(files_to_process)} files to reprocess."
        self.logger.log(level="info", message=msg)

        for file in files_to_process:
            try:
                if os.path.exists(self.csv_download_directory / file):
                    os.remove(self.csv_download_directory / file)
                move_file(file, self.csv_imported_directory, self.csv_download_directory)
                msg = f"Moved {file} to download directory for reprocessing."
                self.logger.log(level="info", message=msg)
            except Exception as e:
                msg = f"Error moving file {file}: {e}"
                self.logger.log(level="warning", message=msg)

        try:
            self._unzip_files()
            self._clean_csv()
            self._import_logs()
            self._remove_downloads()

            msg = "Reprocess imports completed successfully."
            self.logger.log(level="info", message=msg)
        except Exception as e:
            msg = f"Error during reprocess imports: {e}"
            self.logger.log(level="error", message=msg)
            raise e
        finally:
            if self.logger.warning_logs or self.logger.error_logs:
                logs = sorted(
                    list(set(self.logger.warning_logs))
                    + list(set(self.logger.error_logs))
                )
                self.logger.check_alert_log(
                    alert_name="Error in reprocess imports for DraftKings entries data",
                    alert_description=f"Error during reprocess imports: {logs}",
                    review_script=self.script_name,
                    review_table="contest",
                )
            self._close_sql_connections()

        return self.contest_data

    def scrape(
        self, contest_ids: Optional[List[int]] = None
    ) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
        """
        Main scraping method for contest entries.

        Args:
            contest_ids: Optional list of contest IDs to download. If None, fetches from database.

        Returns:
            dict: Contest data organized by contest_id, e.g.:
                {contest_id: {"entries": [...], "player_results": [...]}, ...}
        """
        # Validate directories exist before starting
        if not self._validate_directories():
            self._close_sql_connections()
            raise SystemExit(
                "Required directories do not exist. Please create them and try again."
            )

        start_time = datetime.now()

        try:
            msg = f"Starting contest entries scraper."
            self.logger.log(level="info", message=msg)

            self._download_contest_csv(contest_ids)
            self._unzip_files()
            self._clean_csv()
            self._import_logs()
            self._remove_downloads()

            msg = f"Finished scraping contest entries."
            self.logger.log(level="info", message=msg)

            elapsed_time = datetime.now() - start_time
            msg = f"Total time elapsed: {elapsed_time}"
            self.logger.log(level="info", message=msg)

        except Exception as e:
            msg = f"Failed contest entries scraper: {e}"
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
                    alert_name=f"Error processing DraftKings entries data",
                    alert_description=f"Error processing DraftKings entries data: {logs_str}",
                    review_script=self.script_name,
                    review_table="draftkings.contest",
                )
            self._close_sql_connections()

        return self.contest_data


def get_contests_to_download() -> List[int]:
    """Query the database for contest IDs that need to be downloaded."""
    from mg.db.postgres_manager import PostgresManager

    connection = PostgresManager(
        "digital_ocean", "defaultdb", "draftkings", return_logging=False
    )
    try:
        q = """
            SELECT contest_id
            FROM draftkings.contests
            WHERE is_final = TRUE
            AND COALESCE(is_cancelled, FALSE) = FALSE
            AND COALESCE(is_downloaded, FALSE) = FALSE
        """
        results = connection.execute(q)
        return [x["contest_id"] for x in results]
    finally:
        connection.close()


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

    # Crash recovery mode processes existing downloaded files, no contest IDs needed
    if args.crash_recovery:
        scraper.crash_recovery()
        return

    # Reprocess files from the imports directory
    if args.reprocess_imports:
        scraper.reprocess_imports()
        return

    # Get contest IDs: from args, or query the database
    if args.contest_ids:
        contest_ids = [int(cid.strip()) for cid in args.contest_ids.split(",")]
    else:
        contest_ids = get_contests_to_download()

    if not contest_ids:
        logger.info("No contests found to download; ending script.")
        return

    logger.info(f"Found {len(contest_ids)} contests to download.")
    scraper.scrape(contest_ids=contest_ids)


if __name__ == "__main__":
    main()
