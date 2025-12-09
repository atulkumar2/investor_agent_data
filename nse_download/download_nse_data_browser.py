"""
NSE Bhavcopy Data Downloader
Downloads full bhavcopy and security deliverable data from NSE India website
for specified date range with logging and browser agent rotation.
"""

import argparse
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from indian_holidays import is_public_holiday
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Configuration
BASE_URL = "https://www.nseindia.com/all-reports#cr_equity_archives"
SLEEP_MIN = 3  # Minimum sleep seconds between downloads
SLEEP_MAX = 7  # Maximum sleep seconds between downloads

# User agents for rotation
USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ),
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    ),
]


def setup_logging(log_file):
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )


def create_driver(user_agent, data_folder):
    """Create a Chrome WebDriver with specified user agent"""
    chrome_options = Options()
    chrome_options.add_argument(f"user-agent={user_agent}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    # Set download preferences
    prefs = {
        "download.default_directory": str(data_folder.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def perform_initial_setup(driver):
    """Perform initial page setup: navigate, search, and select checkbox"""
    try:
        # Navigate directly to archives page
        archives_url = "https://www.nseindia.com/all-reports#cr_equity_archives"
        logging.info("Navigating to archives page: %s", archives_url)
        driver.get(archives_url)

        # Wait for page to load
        wait = WebDriverWait(driver, 30)
        time.sleep(5)  # Let page fully load and scripts execute

        # Type "full" in the search box to filter reports
        logging.info("Typing 'full' in search box...")
        search_box_selectors = [
            "//input[@id='crEquityArchivesSearch']",
            "//input[@placeholder='Enter a keyword']",
            "//input[contains(@class, 'searchby_input')]",
        ]

        search_box = None
        for selector in search_box_selectors:
            try:
                search_box = wait.until(EC.presence_of_element_located((By.XPATH, selector)))
                if search_box:
                    logging.info("Found search box with selector: %s", selector)
                    break
            except Exception:
                continue

        if search_box:
            search_box.clear()
            search_box.send_keys("full")
            logging.info("Typed 'full' in search box")
            time.sleep(1)  # Wait for search filter to apply
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
        else:
            logging.warning("Could not find search box, continuing anyway...")

        # Look for the "Full Bhavcopy and Security Deliverable data" checkmark
        logging.info("Looking for Full Bhavcopy checkmark...")
        checkmark_selectors = [
            "//div[@class='card-body']//label[contains(., 'Full Bhavcopy')]//span[@class='checkmark']",
            "//label[@class='chk_container'][contains(., 'Full Bhavcopy')]//span[@class='checkmark']",
            "//label[@class='chk_container']//span[@class='checkmark']",
            "//span[@class='checkmark']",
        ]

        checkmark = None
        for selector in checkmark_selectors:
            try:
                checkmark = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                if checkmark:
                    logging.info("Found checkmark with selector: %s", selector)
                    break
            except Exception:
                continue

        if checkmark:
            logging.info("Clicking checkmark for Full Bhavcopy")
            checkmark.click()
            time.sleep(0.5)
            return True
        else:
            logging.warning("Could not find Full Bhavcopy checkmark")
            return False

    except Exception as e:
        logging.error("Error in initial setup: %s", str(e))
        return False


def select_date_in_calendar(driver, target_date):
    """Navigate calendar and select the target date"""
    try:
        wait = WebDriverWait(driver, 30)

        # Scroll to top of page to find date field
        logging.info("Scrolling to top of page...")
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)

        # Click on the calendar icon to open date picker
        logging.info("Looking for calendar icon button...")
        calendar_button_selectors = [
            "//button[@aria-label='Datepicker button']",
            "//button[contains(@class, 'btn') and .//i[contains(@class, 'fa-calendar')]]",
            "//span[@role='button']//button[.//i[@class='fa fa-calendar']]",
            "//button[@type='button'][.//i[contains(@class, 'calendar')]]",
        ]

        calendar_button = None
        for selector in calendar_button_selectors:
            try:
                calendar_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                if calendar_button:
                    logging.info("Found calendar button with selector: %s", selector)
                    break
            except Exception:
                continue

        if not calendar_button:
            logging.warning("Could not find calendar button")
            return False

        # Click the calendar button to open date picker
        logging.info("Clicking calendar button to open date picker...")
        calendar_button.click()
        time.sleep(1)  # Wait for calendar to open

        # Navigate to correct month using calendar navigation
        target_month_year = target_date.strftime("%B %Y")
        logging.info("Navigating to target month: %s", target_month_year)

        max_navigation_attempts = 24  # Limit to 2 years of navigation
        for attempt in range(max_navigation_attempts):
            try:
                # Find the current period displayed in calendar
                period_element = driver.find_element(By.XPATH, "//div[@data-role='period']")
                current_period = period_element.text.strip()
                logging.info("Current calendar period: %s", current_period)

                if current_period == target_month_year:
                    logging.info("Found target month/year: %s", target_month_year)
                    break

                # Determine if we need to go forward or backward
                current_date_obj = datetime.strptime(current_period, "%B %Y")
                target_date_obj = datetime.strptime(target_month_year, "%B %Y")

                if target_date_obj > current_date_obj:
                    # Click right chevron to go forward
                    nav_button = driver.find_element(
                        By.XPATH,
                        "//div[@data-role='navigator']//i[@class='fa fa-chevron-right']/parent::div",
                    )
                    nav_button.click()
                    logging.info("Clicked right chevron to go forward")
                else:
                    # Click left chevron to go backward
                    nav_button = driver.find_element(
                        By.XPATH,
                        "//div[@data-role='navigator']//i[@class='fa fa-chevron-left']/parent::div",
                    )
                    nav_button.click()
                    logging.info("Clicked left chevron to go backward")

                time.sleep(0.5)  # Wait for calendar to update
            except ValueError as ve:
                logging.warning("Error parsing dates: %s", str(ve))
                break
            except Exception as nav_error:
                logging.warning(
                    "Error during calendar navigation (attempt %d): %s", attempt + 1, str(nav_error)
                )
                break

        # Now select the date from the calendar popup
        logging.info("Selecting date from calendar: %s", target_date.strftime("%d-%b-%Y"))

        # Date in calendar is without leading zero (1, 2, 3... not 01, 02, 03)
        day_number = str(target_date.day)  # Remove leading zero

        # Prioritize the selector that works based on logs
        date_cell_selectors = [
            f"//div[contains(@class, 'gj-picker')]//div[text()='{day_number}']",
            f"//div[contains(@class, 'calendar')]//td[text()='{day_number}']",
            f"//div[contains(@class, 'datepicker')]//td[text()='{day_number}']",
            f"//table[@role='grid']//td[text()='{day_number}']",
        ]

        date_cell = None
        for selector in date_cell_selectors:
            try:
                # Reduce wait time from 30 to 5 seconds per selector
                date_cell = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                if date_cell:
                    logging.info("Found date cell with selector: %s", selector)
                    break
            except Exception:
                continue

        if date_cell:
            logging.info("Clicking date: %s", day_number)
            date_cell.click()
            time.sleep(2)  # Wait for date selection to be processed
            return True

        logging.warning("Could not find date cell in calendar")
        return False

    except Exception as e:
        logging.warning("Error selecting date: %s", str(e))
        return False


def click_download_button(driver):
    """Find and click the download button"""
    try:
        # Wait a bit more for the page to update after date selection
        time.sleep(1)

        # Find the download icon in the same card-body container
        download_icon_selectors = [
            "//div[@class='card-body']//span[@class='reportDownloadIcon']//a[@aria-label='Download File']",
            "//div[@class='card-body']//span[@class='reportDownloadIcon']//a",
            "//label[contains(., 'Full Bhavcopy')]/..//span[@class='reportDownloadIcon']//a",
            "//span[@class='reportDownloadIcon']//a[@class='pdf-download-link']",
            "//a[@onclick=\"SingledownloadReports('#cr_equity_archives', 'equities', this)\"]",
            "//a[@aria-label='Download File'][@class='pdf-download-link']",
            "//span[@class='reportDownloadIcon']//a",
        ]

        download_icon = None
        for selector in download_icon_selectors:
            try:
                # Scroll the element into view before clicking
                elements = driver.find_elements(By.XPATH, selector)
                if elements:
                    for elem in elements:
                        if elem.is_displayed() and elem.is_enabled():
                            download_icon = elem
                            logging.info("Found download icon with selector: %s", selector)
                            break
                if download_icon:
                    break
            except Exception:
                continue

        if download_icon:
            logging.info("Clicking download icon")
            # Use JavaScript click to avoid click interception
            driver.execute_script("arguments[0].click();", download_icon)
            logging.info("Download icon clicked, waiting for file...")
            return True
        else:
            logging.warning("Could not find download icon")
            return False

    except Exception as e:
        logging.error("Error clicking download icon: %s", str(e))
        return False


def wait_for_file_download(filepath, max_wait_time=60, check_interval=2):
    """Wait for file to download and verify it's complete"""
    elapsed_time = 0
    while elapsed_time < max_wait_time:
        if filepath.exists():
            # Verify file is not empty and download is complete
            file_size = filepath.stat().st_size
            if file_size > 0:
                logging.info("[SUCCESS] Downloaded: %s (%.2f KB)", filepath.name, file_size / 1024)
                return True, file_size

        time.sleep(check_interval)
        elapsed_time += check_interval

    logging.warning("[FAILED] File not found after %d seconds: %s", max_wait_time, filepath.name)
    return False, 0


def download_bhavcopy_for_date(
    target_date, user_agent, data_folder, driver=None, is_batch=False, is_first_of_week=False
):
    """
    Download bhavcopy for a specific date
    Returns: (success: bool, filename: str or None, error_message: str or None, file_size: int)
    If is_batch=True, driver will be reused and not quit at the end
    If is_first_of_week=True, performs initial setup (navigate, search, checkbox)
    """
    driver_created = False
    try:
        logging.info(
            "Starting download for %s (%s)",
            target_date.strftime("%Y-%m-%d"),
            target_date.strftime("%A"),
        )
        logging.info("Using User Agent: %s...", user_agent[:50])

        if driver is None:
            driver = create_driver(user_agent, data_folder)
            driver_created = True

        # Only do initial setup on first download of the week
        if is_first_of_week or driver_created:
            if not perform_initial_setup(driver):
                return False, None, "Initial setup failed", 0
        else:
            # For subsequent downloads in the same week, just need a WebDriverWait
            logging.info("Reusing existing page (skipping navigation and search)")

        # Select the date in calendar
        if not select_date_in_calendar(driver, target_date):
            return False, None, "Date selection failed", 0

        # Click the download button
        if not click_download_button(driver):
            return False, None, "Download button click failed", 0

        # Generate expected filename
        filename = "sec_bhavdata_full_%s.csv" % target_date.strftime("%d%m%Y")
        filepath = data_folder / filename

        # Wait for file to download
        success, file_size = wait_for_file_download(filepath)
        if success:
            return True, filename, None, file_size
        else:
            return False, None, "File not found after download", 0

    except TimeoutException as e:
        error_msg = f"Timeout waiting for element: {e}"
        logging.error("[ERROR] %s", error_msg)
        return False, None, error_msg, 0
    except NoSuchElementException as e:
        error_msg = f"Element not found: {e}"
        logging.error("[ERROR] %s", error_msg)
        return False, None, error_msg, 0
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logging.error("[ERROR] %s", error_msg)
        return False, None, error_msg, 0
    finally:
        if driver and driver_created and not is_batch:
            driver.quit()


def should_skip_date(current_date):
    """Check if date should be skipped (weekend or holiday)"""
    weekday = current_date.weekday()

    # Skip weekends
    if weekday == 5:  # Saturday
        return True, "Saturday", "Weekend - Market Closed"
    elif weekday == 6:  # Sunday
        return True, "Sunday", "Weekend - Market Closed"

    # Skip public holidays
    if is_public_holiday(current_date):
        return True, current_date.strftime("%A"), "Public Holiday - Market Closed"

    return False, None, None


def manage_browser_session(current_date, driver, user_agent, data_folder):
    """Manage browser session lifecycle"""
    weekday = current_date.weekday()
    is_first_of_week = False

    # Start new browser session on Monday or if no driver exists
    if weekday == 0 or driver is None:  # Monday
        if driver:
            logging.info("Closing previous week's browser session...")
            driver.quit()

        user_agent = random.choice(USER_AGENTS)
        driver = create_driver(user_agent, data_folder)
        is_first_of_week = True
        logging.info(
            "Started new browser session for week starting %s",
            current_date.strftime("%Y-%m-%d"),
        )

    return driver, user_agent, is_first_of_week


def process_download_results(results, data_folder):
    """Analyze downloaded files and update results with file info"""
    logging.info("=" * 80)
    logging.info("Analyzing downloaded files...")
    logging.info("=" * 80)

    for result in results:
        if result["Status"] == "Success" and result["Filename"] != "N/A":
            filepath = data_folder / result["Filename"]
            try:
                df = pd.read_csv(filepath)
                rows, cols = df.shape
                result["Rows"] = rows
                result["Columns"] = cols
                logging.info(
                    "File: %s - Shape: (%d rows, %d columns)", result["Filename"], rows, cols
                )
            except Exception as e:
                logging.warning("Could not analyze file %s: %s", result["Filename"], str(e))
                result["Rows"] = -1
                result["Columns"] = -1


def generate_summary_report(results, summary_file, log_file):
    """Generate and display download summary"""
    # Create summary table
    df = pd.DataFrame(results)
    df.to_csv(summary_file, index=False)

    logging.info("=" * 80)
    logging.info("Download Summary:")
    logging.info("=" * 80)
    print("\n" + df.to_string(index=False))

    # Statistics
    total = len(results)
    successful = sum(1 for r in results if r["Status"] == "Success")
    failed = total - successful

    logging.info("=" * 80)
    logging.info("Total downloads attempted: %d", total)
    logging.info("Successful downloads: %d", successful)
    logging.info("Failed downloads: %d", failed)

    if failed > 0:
        logging.info("\nFailed downloads:")
        for r in results:
            if r["Status"] == "Failed":
                logging.info("  - %s (%s): %s", r["Date"], r["Weekday"], r["Error"])

    logging.info("=" * 80)
    logging.info("Summary saved to: %s", summary_file)
    logging.info("Log saved to: %s", log_file)
    logging.info("NSE Bhavcopy Download Script Completed")
    logging.info("=" * 80)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Download NSE bhavcopy data for a specified date range"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2025-07-01",
        help="Start date in YYYY-MM-DD format (default: 2025-07-01)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-07-10",
        help="End date in YYYY-MM-DD format (default: 2025-07-10)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: data/YYYYMM based on start date)",
    )
    return parser.parse_args()


def main():
    """Main function to download bhavcopy data for the date range"""
    args = parse_arguments()

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing dates: {e}")
        print("Please use YYYY-MM-DD format")
        return

    # Validate date range
    if start_date > end_date:
        print("Error: Start date must be before or equal to end date")
        return

    # Setup output directory
    if args.output_dir:
        data_folder = Path(args.output_dir)
    else:
        year_month = start_date.strftime("%Y%m")
        data_folder = Path(f"data/{year_month}")

    data_folder.mkdir(parents=True, exist_ok=True)

    # Setup logging directory and files with date range in filename
    logs_folder = Path("logs")
    logs_folder.mkdir(parents=True, exist_ok=True)

    date_range_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    log_file = logs_folder / f"download_log_{date_range_str}.txt"
    summary_file = logs_folder / f"download_summary_{date_range_str}.csv"
    setup_logging(log_file)

    logging.info("=" * 80)
    logging.info("NSE Bhavcopy Download Script Started")
    logging.info(
        "Date Range: %s to %s", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
    logging.info("Output Directory: %s", data_folder)
    logging.info("=" * 80)

    results = []
    current_date = start_date
    driver = None
    user_agent = None

    while current_date <= end_date:
        # Check if date should be skipped
        skip, weekday_name, skip_reason = should_skip_date(current_date)
        if skip:
            logging.info(
                "Skipping %s: %s",
                weekday_name.lower() if weekday_name else "unknown",
                current_date.strftime("%Y-%m-%d"),
            )
            result = {
                "Date": current_date.strftime("%Y-%m-%d"),
                "Weekday": weekday_name,
                "Status": "Skipped",
                "Filename": "N/A",
                "File_Size_KB": 0,
                "Rows": 0,
                "Columns": 0,
                "Error": skip_reason,
            }
            results.append(result)
            current_date += timedelta(days=1)
            continue

        # Manage browser session
        driver, user_agent, is_first_of_week = manage_browser_session(
            current_date, driver, user_agent, data_folder
        )

        # Download for current date using existing driver
        success, filename, error, file_size = download_bhavcopy_for_date(
            current_date,
            user_agent,
            data_folder,
            driver=driver,
            is_batch=True,
            is_first_of_week=is_first_of_week,
        )

        # Record result
        result = {
            "Date": current_date.strftime("%Y-%m-%d"),
            "Weekday": current_date.strftime("%A"),
            "Status": "Success" if success else "Failed",
            "Filename": filename if filename else "N/A",
            "File_Size_KB": round(file_size / 1024, 2) if file_size > 0 else 0,
            "Rows": 0,
            "Columns": 0,
            "Error": error if error else "N/A",
        }
        results.append(result)

        # Move to next date
        current_date += timedelta(days=1)

        # Close browser on Friday or if it's the last date
        next_weekday = current_date.weekday() if current_date <= end_date else -1
        if next_weekday == 5 or current_date > end_date:  # Saturday or past end
            if driver:
                logging.info("Closing browser session at end of week...")
                driver.quit()
                driver = None

        # Sleep between downloads (except for the last one)
        if current_date <= end_date:
            sleep_time = random.uniform(SLEEP_MIN, SLEEP_MAX)
            logging.info("Sleeping for %.2f seconds before next download...", sleep_time)
            time.sleep(sleep_time)

    # Cleanup any remaining driver
    if driver:
        driver.quit()

    # Process download results
    process_download_results(results, data_folder)

    # Generate summary report
    generate_summary_report(results, summary_file, log_file)


if __name__ == "__main__":
    main()
