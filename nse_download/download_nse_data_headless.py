#!/usr/bin/env python3
"""
NSE Bhavcopy Downloader
Downloads Full Bhavcopy zip files from NSE from Feb 1, 2025 to today,
extracts them, organizes by month, and tracks failures.
"""

import argparse
import csv
import json
import logging
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

BASE_NSE_RAWDATA_DIR = "./data"
BASE_LOG_DIR = "./logs"

# Create log directory
Path(BASE_LOG_DIR).mkdir(exist_ok=True)

# Configure logging to output to both console and file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Console handler
        logging.FileHandler(
            f"{BASE_LOG_DIR}/nse_download-{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        ),
    ],
)


class StatusLogger:
    """Handles logging download statuses to CSV"""

    def __init__(self):
        self.base_dir = Path(BASE_LOG_DIR)
        self.base_dir.mkdir(exist_ok=True)
        self.statuses = []

    def add_status(self, date, status, reason, file_path=None, file_size=None, file_shape=None):
        """Add a download status entry"""
        self.statuses.append(
            {
                "date": date,
                "status": status,
                "reason": reason,
                "file_path": str(file_path) if file_path else "",
                "file_size": file_size or 0,
                "file_shape": str(file_shape) if file_shape else "",
            }
        )

    def write_csv(self):
        """Write all statuses to a CSV file"""
        status_csv = (
            self.base_dir / f"download_status-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        with open(status_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["date", "status", "reason", "file_path", "file_size", "file_shape"]
            )
            writer.writeheader()
            writer.writerows(self.statuses)
        logging.info("[LOG] Download status logged to: %s", status_csv)

    def write_failed_json(self, failed_dates):
        """Write failed download dates to a JSON file"""
        failed_log = (
            self.base_dir / f"failed_downloads-{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(failed_log, "w", encoding="utf-8") as f:
            json.dump(failed_dates, f, indent=2)
        logging.info("\n[LOG] Failed downloads logged to: %s", failed_log)


class NSEBhavcopyDownloader:
    """Downloads and organizes NSE Bhavcopy data"""

    BASE_URL = "https://www.nseindia.com/api/reports"
    HTTP_STATUS_NOT_FOUND = 404
    HTTP_STATUS_OK = 200
    DATE_FORMAT = "%d-%b-%Y"
    WEEKEND_START = 5

    # NSE requires these headers to avoid 403 errors
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/report-detail/eq_security",
        "X-Requested-With": "XMLHttpRequest",
    }

    COOKIE_REFRESH_INTERVAL = 300  # seconds

    def __init__(self, output_dir=BASE_NSE_RAWDATA_DIR, existing_dir=None):
        """
        Initialize downloader

        Args:
            base_dir: Base directory to store downloaded data
            existing_dir: Directory where existing documents are kept
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.existing_dir = Path(existing_dir) if existing_dir else None
        self.failed_dates = []
        self.skipped_dates = []
        self.status_logger = StatusLogger()
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._last_cookie_time = 0  # Initialize cookie time

    def _get_cookie(self):
        """Get session cookie from NSE homepage"""
        try:
            response = self.session.get("https://www.nseindia.com", timeout=10)
            return response.status_code == NSEBhavcopyDownloader.HTTP_STATUS_OK
        except Exception as e:
            logging.warning("[WARN] Could not get session cookie: %s", e)
            return False

    def _check_file_exists(self, date):
        """Check if the expected CSV file already exists"""
        month_folder = self._get_month_folder(date)
        expected_csv = month_folder / f"sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"
        return expected_csv.exists(), expected_csv

    def _refresh_session_if_needed(self):
        """Refresh session cookie if needed"""
        current_time = time.time()
        if (
            not hasattr(self, "_last_cookie_time")
            or (current_time - self._last_cookie_time) > self.COOKIE_REFRESH_INTERVAL
        ):
            self._get_cookie()
            self._last_cookie_time = current_time
            time.sleep(1)

    def _build_download_url(self, date):
        """Build NSE API URL for given date"""
        date_str = date.strftime(self.DATE_FORMAT)
        archives = (
            '[{"name":"Full Bhavcopy and Security Deliverable data",'
            '"type":"daily-reports",'
            '"category":"capital-market",'
            '"section":"equities"}]'
        )
        archives_encoded = quote(archives)
        url = f"{self.BASE_URL}?archives={archives_encoded}&date={date_str}&type=Archives"
        return url

    def _download_zip_content(self, url):
        """Download zip content from URL, handling both direct zip and JSON responses"""
        response = self.session.get(url, timeout=30)
        if response.status_code == self.HTTP_STATUS_NOT_FOUND:
            return None, "No data available (404)"
        if response.status_code != self.HTTP_STATUS_OK:
            return None, f"HTTP {response.status_code}"

        content_type = response.headers.get("Content-Type", "")
        if "application/zip" in content_type or response.content[:2] == b"PK":
            return response.content, None

        # Try JSON response
        try:
            data = response.json()
            if not data or len(data) == 0:
                return None, "No data in response"

            download_url = None
            for item in data:
                if "file" in item and item["file"].endswith(".zip"):
                    download_url = f"https://www.nseindia.com{item['file']}"
                    break

            if not download_url:
                return None, "No zip file in response"

            zip_response = self.session.get(download_url, timeout=60)
            if zip_response.status_code != NSEBhavcopyDownloader.HTTP_STATUS_OK:
                return None, f"Zip download HTTP {zip_response.status_code}"

            return zip_response.content, None

        except ValueError:
            return None, "Invalid response format (not JSON or ZIP)"

    def _extract_and_cleanup(self, zip_content, zip_path, month_folder):
        """Extract zip content and clean up"""
        with open(zip_path, "wb") as f:
            f.write(zip_content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(month_folder)

        zip_path.unlink()

    def _build_url(self, date):
        """
        Build NSE API URL for given date

        Args:
            date: datetime object

        Returns:
            Full URL string
        """
        # Format date as DD-Mon-YYYY (e.g., 12-Feb-2025)
        date_str = date.strftime(self.DATE_FORMAT)

        # Build the archives parameter (already URL encoded in the sample)
        archives = (
            '[{"name":"Full Bhavcopy and Security Deliverable data",'
            '"type":"daily-reports",'
            '"category":"capital-market",'
            '"section":"equities"}]'
        )
        archives_encoded = quote(archives)

        url = f"{self.BASE_URL}?archives={archives_encoded}&date={date_str}&type=Archives"
        return url

    def _get_month_folder(self, date):
        """
        Get month folder path (YYYYMM format)

        Args:
            date: datetime object

        Returns:
            Path object for month folder
        """
        month_str = date.strftime("%Y%m")
        month_folder = self.output_dir / month_str
        month_folder.mkdir(exist_ok=True)
        return month_folder

    def _get_csv_shape(self, csv_path):
        """Get the shape (rows, columns) of a CSV file"""
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:
                    return (0, 0)
                rows = list(reader)
                return (len(rows), len(header))
        except Exception:
            return (0, 0)

    def download_and_extract(self, date):
        """
        Download and extract bhavcopy for a specific date

        Args:
            date: datetime object

        Returns:
            bool: True if successful, False otherwise
        """
        date_str = date.strftime(self.DATE_FORMAT)
        logging.info("[DOWNLOAD] Processing %s...", date_str)

        zip_path = None
        try:
            # Check if file already exists FIRST (before any network/delay operations)
            month_folder = self._get_month_folder(date)
            expected_csv = month_folder / f"sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"

            # Check if file already exists in existing_dir or output_dir
            if expected_csv.exists():
                logging.info("[SKIP] Already exists, skipping")
                self.skipped_dates.append(date_str)
                return True
            # Refresh session cookie periodically (only if we need to download)
            if (
                not hasattr(self, "_last_cookie_time")
                or (time.time() - self._last_cookie_time) > self.COOKIE_REFRESH_INTERVAL
            ):
                self._get_cookie()
                self._last_cookie_time = time.time()
                time.sleep(1)

            # Build URL
            url = self._build_url(date)

            # Download the response (could be JSON or ZIP directly)
            response = self.session.get(url, timeout=30)
            if response.status_code == self.HTTP_STATUS_NOT_FOUND:
                logging.error("[ERROR] No data (404)")
                self.failed_dates.append({"date": date_str, "reason": "No data available (404)"})
                return False

            if response.status_code != self.HTTP_STATUS_OK:
                logging.error("[ERROR] HTTP %s", response.status_code)
                self.failed_dates.append(
                    {"date": date_str, "reason": f"HTTP {response.status_code}"}
                )
                return False

            # Check content type - NSE might return zip directly or JSON with links
            content_type = response.headers.get("Content-Type", "")

            if "application/zip" in content_type or response.content[:2] == b"PK":
                # Direct zip file download
                zip_content = response.content
            else:
                # Try to parse as JSON (old API format)
                try:
                    data = response.json()

                    if not data or len(data) == 0:
                        logging.error("[ERROR] No data available")
                        self.failed_dates.append(
                            {"date": date_str, "reason": "No data in response"}
                        )
                        return False

                    # Find the zip file URL
                    download_url = None
                    for item in data:
                        if "file" in item:
                            file_url = item["file"]
                            if file_url.endswith(".zip"):
                                download_url = f"https://www.nseindia.com{file_url}"
                                break

                    if not download_url:
                        logging.error("[ERROR] No zip file found")
                        self.failed_dates.append(
                            {"date": date_str, "reason": "No zip file in response"}
                        )
                        return False

                    # Download the zip file
                    zip_response = self.session.get(download_url, timeout=60)

                    if zip_response.status_code != self.HTTP_STATUS_OK:
                        logging.error(
                            "[ERROR] Zip download failed HTTP %s", zip_response.status_code
                        )
                        self.failed_dates.append(
                            {
                                "date": date_str,
                                "reason": f"Zip download HTTP {zip_response.status_code}",
                            }
                        )
                        return False

                    zip_content = zip_response.content

                except ValueError:
                    logging.error("[ERROR] Invalid response format")
                    self.failed_dates.append(
                        {"date": date_str, "reason": "Invalid response format (not JSON or ZIP)"}
                    )
                    return False

            # Get month folder (already retrieved above for existence check)
            # month_folder = self._get_month_folder(date)

            # Save zip temporarily
            zip_filename = f"bhavcopy_{date.strftime('%Y%m%d')}.zip"
            zip_path = month_folder / zip_filename

            with open(zip_path, "wb") as f:
                f.write(zip_content)

            # Extract zip
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(month_folder)

            # Delete zip file
            zip_path.unlink()

            logging.info("[OK]")
            return True

        except requests.exceptions.RequestException as e:
            logging.error("[ERROR] Network error: %s", e)
            self.failed_dates.append({"date": date_str, "reason": f"Network error: {str(e)}"})
            return False

        except zipfile.BadZipFile as e:
            logging.error("[ERROR] Bad zip file: %s", e)
            self.failed_dates.append({"date": date_str, "reason": "Invalid zip file"})
            # Try to clean up bad zip
            try:
                if zip_path is not None and zip_path.exists():
                    zip_path.unlink()
            except Exception:
                pass
            return False

        except Exception as e:
            logging.error("[ERROR] Error: %s", e)
            self.failed_dates.append({"date": date_str, "reason": str(e)})
            return False

    def download_range(self, start_date, end_date):
        """
        Download bhavcopy files for a date range

        Args:
            start_date: datetime object (start date)
            end_date: datetime object (end date)
        """
        logging.info("[START] Starting NSE Bhavcopy Download")
        logging.info(
            "[DATE] Date Range: %s to %s",
            start_date.strftime(self.DATE_FORMAT),
            end_date.strftime(self.DATE_FORMAT),
        )
        logging.info("[DIR] Output Directory: %s", self.output_dir.absolute())

        # Get initial session cookie
        logging.info("[COOKIE] Getting session cookie...")
        if self._get_cookie():
            logging.info("[OK]")
        else:
            logging.warning("[WARN] (Continuing anyway)")

        current_date = start_date
        success_count = 0

        while current_date <= end_date:
            date_str = current_date.strftime(self.DATE_FORMAT)

            # Skip Saturdays and Sundays (market closed)
            if current_date.weekday() >= self.WEEKEND_START:
                logging.info("[SKIP] Skipping %s (Weekend)", date_str)
                self.status_logger.add_status(
                    date_str, "skipped_weekend", "Market closed on weekends"
                )
                current_date += timedelta(days=1)
                continue

            # Check if file exists in existing directory
            if self.existing_dir:
                month_folder_rel = current_date.strftime("%Y%m")
                expected_name = f"sec_bhavdata_full_{current_date.strftime('%d%m%Y')}.csv"
                existing_file = self.existing_dir / month_folder_rel / expected_name
                if existing_file.exists():
                    logging.info("[SKIP] %s already exists in existing directory", date_str)
                    self.status_logger.add_status(
                        date_str, "skipped_existing", "File already exists in existing directory"
                    )
                    current_date += timedelta(days=1)
                    continue

            if self.download_and_extract(current_date):
                if date_str in self.skipped_dates:
                    status = "skipped_existing"
                    reason = "File already exists"
                    file_path = None
                    file_size = 0
                    file_shape = (0, 0)
                elif any(f["date"] == date_str for f in self.failed_dates):
                    status = "failed"
                    reason = next(f["reason"] for f in self.failed_dates if f["date"] == date_str)
                    file_path = None
                    file_size = 0
                    file_shape = (0, 0)
                else:
                    status = "success"
                    reason = ""
                    success_count += 1
                    # Get file info
                    month_folder = self._get_month_folder(current_date)
                    expected_csv = (
                        month_folder / f"sec_bhavdata_full_{current_date.strftime('%d%m%Y')}.csv"
                    )
                    file_path = expected_csv
                    file_size = expected_csv.stat().st_size
                    file_shape = self._get_csv_shape(expected_csv)
            else:
                status = "failed"
                reason = next(
                    (f["reason"] for f in self.failed_dates if f["date"] == date_str),
                    "Unknown error",
                )
                file_path = None
                file_size = 0
                file_shape = (0, 0)

            self.status_logger.add_status(
                date_str, status, reason, file_path, file_size, file_shape
            )

            current_date += timedelta(days=1)

            # Be respectful to NSE servers - small delay between downloads
            if status in ["success", "failed"]:
                time.sleep(2)

        # Write CSV status file
        self.status_logger.write_csv()

        # Summary
        logging.info("\n%s", "=" * 60)
        logging.info("[SUMMARY] Download Summary")
        logging.info("%s", "=" * 60)
        logging.info("[OK] Successful: %s", success_count)
        logging.info(
            "[SKIP] Skipped (existing or weekend): %s",
            len([s for s in self.status_logger.statuses if s["status"].startswith("skipped")]),
        )
        logging.info(
            "[ERROR] Failed: %s",
            len([s for s in self.status_logger.statuses if s["status"] == "failed"]),
        )

        if self.failed_dates:
            logging.warning("\n[WARN] Failed Downloads:")
            for failure in self.failed_dates:
                logging.warning("  - %s: %s", failure["date"], failure["reason"])

            # Save failed dates to JSON
            self.status_logger.write_failed_json(self.failed_dates)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Download NSE Bhavcopy data for a date range")
    parser.add_argument(
        "--start-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Start date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data",
        help="Output directory for downloaded data (default: ./data)",
    )
    parser.add_argument(
        "--existing-dir",
        type=str,
        required=True,
        help="Directory where existing documents are kept",
    )

    args = parser.parse_args()

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        logging.error("Error parsing dates: %s", e)
        logging.error("Please use YYYY-MM-DD format")
        return

    # Validate date range
    if start_date > end_date:
        logging.error("Error: Start date must be before or equal to end date")
        return

    if end_date > datetime.now():
        logging.error("Error: End date must be in the past or today")
        return

    if start_date < datetime(2010, 2, 1):
        logging.error("Error: Start date must be on or after February 1, 2010")
        return

    # Log argument values
    logging.info("[ARGS] Start Date: %s", start_date.strftime("%Y-%m-%d"))
    logging.info("[ARGS] End Date: %s", end_date.strftime("%Y-%m-%d"))
    logging.info("[ARGS] Output Directory: %s", args.output_dir)
    logging.info("[ARGS] Existing Directory: %s", args.existing_dir)

    # Create folders if not exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    # Initialize downloader
    downloader = NSEBhavcopyDownloader(output_dir=args.output_dir, existing_dir=args.existing_dir)

    # Download all files in range
    downloader.download_range(start_date, end_date)


if __name__ == "__main__":
    try:
        logging.info("[START] NSE Bhavcopy download process started.")
        main()
        logging.info("[COMPLETE] NSE Bhavcopy download process completed.")
    except KeyboardInterrupt:
        logging.error("[ERROR] Interrupted by user")
    except Exception as e:
        logging.error("[ERROR] Unexpected error occurred: %s", e)
