#!/usr/bin/env python3
"""
NSE Bhavcopy Downloader
Downloads Full Bhavcopy zip files from NSE from Feb 1, 2025 to today,
extracts them, organizes by month, and tracks failures.
"""

import os
import requests
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
import time
import json
from urllib.parse import quote


class NSEBhavcopyDownloader:
    """Downloads and organizes NSE Bhavcopy data"""
    
    BASE_URL = "https://www.nseindia.com/api/reports"
    
    # NSE requires these headers to avoid 403 errors
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.nseindia.com/report-detail/eq_security',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    def __init__(self, base_dir="investor_agent/data/NSE_RawData"):
        """
        Initialize downloader
        
        Args:
            base_dir: Base directory to store downloaded data
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.failed_dates = []
        self.skipped_dates = []
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        
    def _get_cookie(self):
        """Get session cookie from NSE homepage"""
        try:
            response = self.session.get(
                "https://www.nseindia.com",
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            print(f"⚠️  Warning: Could not get session cookie: {e}")
            return False
    
    def _build_url(self, date):
        """
        Build NSE API URL for given date
        
        Args:
            date: datetime object
            
        Returns:
            Full URL string
        """
        # Format date as DD-Mon-YYYY (e.g., 12-Feb-2025)
        date_str = date.strftime("%d-%b-%Y")
        
        # Build the archives parameter (already URL encoded in the sample)
        archives = '[{"name":"Full Bhavcopy and Security Deliverable data","type":"daily-reports","category":"capital-market","section":"equities"}]'
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
        month_folder = self.base_dir / month_str
        month_folder.mkdir(exist_ok=True)
        return month_folder
    
    def download_and_extract(self, date):
        """
        Download and extract bhavcopy for a specific date
        
        Args:
            date: datetime object
            
        Returns:
            bool: True if successful, False otherwise
        """
        date_str = date.strftime("%d-%b-%Y")
        print(f"📥 Processing {date_str}...", end=" ")
        
        try:
            # Check if file already exists FIRST (before any network/delay operations)
            month_folder = self._get_month_folder(date)
            expected_csv = month_folder / f"sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"
            
            if expected_csv.exists():
                print("⏭️  Already exists, skipping")
                self.skipped_dates.append(date_str)
                return True
            
            # Refresh session cookie periodically (only if we need to download)
            if not hasattr(self, '_last_cookie_time') or (time.time() - self._last_cookie_time) > 300:
                self._get_cookie()
                self._last_cookie_time = time.time()
                time.sleep(1)
            
            # Build URL
            url = self._build_url(date)
            
            # Download the response (could be JSON or ZIP directly)
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 404:
                print("❌ No data (404)")
                self.failed_dates.append({
                    "date": date_str,
                    "reason": "No data available (404)"
                })
                return False
            
            if response.status_code != 200:
                print(f"❌ HTTP {response.status_code}")
                self.failed_dates.append({
                    "date": date_str,
                    "reason": f"HTTP {response.status_code}"
                })
                return False
            
            # Check content type - NSE might return zip directly or JSON with links
            content_type = response.headers.get('Content-Type', '')
            
            if 'application/zip' in content_type or response.content[:2] == b'PK':
                # Direct zip file download
                zip_content = response.content
            else:
                # Try to parse as JSON (old API format)
                try:
                    data = response.json()
                    
                    if not data or len(data) == 0:
                        print("❌ No data available")
                        self.failed_dates.append({
                            "date": date_str,
                            "reason": "No data in response"
                        })
                        return False
                    
                    # Find the zip file URL
                    download_url = None
                    for item in data:
                        if 'file' in item:
                            file_url = item['file']
                            if file_url.endswith('.zip'):
                                download_url = f"https://www.nseindia.com{file_url}"
                                break
                    
                    if not download_url:
                        print("❌ No zip file found")
                        self.failed_dates.append({
                            "date": date_str,
                            "reason": "No zip file in response"
                        })
                        return False
                    
                    # Download the zip file
                    zip_response = self.session.get(download_url, timeout=60)
                    
                    if zip_response.status_code != 200:
                        print(f"❌ Zip download failed HTTP {zip_response.status_code}")
                        self.failed_dates.append({
                            "date": date_str,
                            "reason": f"Zip download HTTP {zip_response.status_code}"
                        })
                        return False
                    
                    zip_content = zip_response.content
                    
                except ValueError:
                    print(f"❌ Invalid response format")
                    self.failed_dates.append({
                        "date": date_str,
                        "reason": "Invalid response format (not JSON or ZIP)"
                    })
                    return False
            
            # Get month folder (already retrieved above for existence check)
            # month_folder = self._get_month_folder(date)
            
            # Save zip temporarily
            zip_filename = f"bhavcopy_{date.strftime('%Y%m%d')}.zip"
            zip_path = month_folder / zip_filename
            
            with open(zip_path, 'wb') as f:
                f.write(zip_content)
            
            # Extract zip
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(month_folder)
            
            # Delete zip file
            zip_path.unlink()
            
            print("✅")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            self.failed_dates.append({
                "date": date_str,
                "reason": f"Network error: {str(e)}"
            })
            return False
            
        except zipfile.BadZipFile as e:
            print(f"❌ Bad zip file: {e}")
            self.failed_dates.append({
                "date": date_str,
                "reason": "Invalid zip file"
            })
            # Try to clean up bad zip
            try:
                zip_path.unlink()
            except:
                pass
            return False
            
        except Exception as e:
            print(f"❌ Error: {e}")
            self.failed_dates.append({
                "date": date_str,
                "reason": str(e)
            })
            return False
    
    def download_range(self, start_date, end_date):
        """
        Download bhavcopy files for a date range
        
        Args:
            start_date: datetime object (start date)
            end_date: datetime object (end date)
        """
        print(f"🚀 Starting NSE Bhavcopy Download")
        print(f"📅 Date Range: {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
        print(f"📁 Output Directory: {self.base_dir.absolute()}\n")
        
        # Get initial session cookie
        print("🔐 Getting session cookie...", end=" ")
        if self._get_cookie():
            print("✅\n")
        else:
            print("⚠️  (Continuing anyway)\n")
        
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        success_count = 0
        
        while current_date <= end_date:
            # Skip Saturdays and Sundays (market closed)
            if current_date.weekday() >= 5:
                print(f"⏭️  Skipping {current_date.strftime('%d-%b-%Y')} (Weekend)")
                current_date += timedelta(days=1)
                continue
            
            date_str_before = current_date.strftime("%d-%b-%Y")
            skipped_count_before = len(self.skipped_dates)
            
            if self.download_and_extract(current_date):
                success_count += 1
            
            # Check if this date was skipped (by comparing counts)
            was_skipped = len(self.skipped_dates) > skipped_count_before
            
            current_date += timedelta(days=1)
            
            # Be respectful to NSE servers - small delay between ACTUAL downloads only
            if not was_skipped:
                time.sleep(2)
        
        # Summary
        print(f"\n{'='*60}")
        print(f"📊 Download Summary")
        print(f"{'='*60}")
        print(f"✅ Successful: {success_count}")
        print(f"⏭️  Skipped (already exist): {len(self.skipped_dates)}")
        print(f"❌ Failed: {len(self.failed_dates)}")
        
        if self.failed_dates:
            print(f"\n⚠️  Failed Downloads:")
            for failure in self.failed_dates:
                print(f"  • {failure['date']}: {failure['reason']}")
            
            # Save failed dates to JSON
            failed_log = self.base_dir / "failed_downloads.json"
            with open(failed_log, 'w') as f:
                json.dump(self.failed_dates, f, indent=2)
            print(f"\n📝 Failed downloads logged to: {failed_log}")


def main():
    """Main entry point"""
    # Date range: Oct 1, 2019 to today
    start_date = datetime(2019, 10, 1)
    end_date = datetime.now()

    # Create folders if not exist
    Path("investor_agent/data/NSE_RawData").mkdir(parents=True, exist_ok=True)
    
    # Initialize downloader
    downloader = NSEBhavcopyDownloader(base_dir="investor_agent/data/NSE_RawData")
    
    # Download all files in range
    downloader.download_range(start_date, end_date)


if __name__ == "__main__":
    main()
