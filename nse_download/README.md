# NSE Download Scripts

This folder contains Python scripts for downloading and analyzing NSE (National Stock Exchange of India) bhavcopy data.

## Scripts Overview

### 1. `download_nse_data_headless.py` - Main Bhavcopy Downloader

Downloads full bhavcopy zip files from NSE India website, extracts them, and organizes by month with comprehensive status tracking.

**Features:**

- Downloads NSE bhavcopy data for specified date ranges
- Automatic extraction and organization by year/month structure
- Skips weekends and Indian public holidays
- Comprehensive logging and status tracking via `StatusLogger` class
- CSV status reports with file details (size, shape, download status)
- JSON tracking of failed downloads
- All logs organized in `./logs` directory
- All output organized in `./data` directory
- Optimized HTTP requests without browser dependencies

**Usage:**

```bash
python download_nse_data_headless.py --start-date 2025-01-01 --end-date 2025-01-31 --existing-dir "path of existing dir"
```

**Arguments:**

- `--start-date`: Start date in YYYY-MM-DD format (default: 2025-02-01)
- `--end-date`: End date in YYYY-MM-DD format (default: today)
- `--existing-dir`: Path pf existing directory

### 2. `download_nse_data_browser.py` - Browser-Based Downloader

Alternative implementation using Selenium browser automation to download NSE bhavcopy files with rotating user agents.

**Features:**

- Browser-based navigation through NSE website
- Rotating user agents for each download
- Weekly batching for improved performance
- Intelligent skipping of weekends and holidays
- Requires Chrome browser and ChromeDriver

### 3. `analyze_existing_files.py` - File Analyzer

Analyzes existing NSE bhavcopy files in a directory and generates reports on file status and missing dates.

**Features:**

- Scans directory for existing bhavcopy CSV files
- Generates summary CSV with file details (date, size, shape)
- Identifies potentially missing dates
- Useful for auditing downloaded data

**Usage:**

```bash
python analyze_existing_files.py --directory ./data --output-dir ./analysis
```

### 4. `indian_holidays.py` - Holiday Configuration Module

Shared module containing NSE market holidays configuration.

**Features:**

- Loads actual NSE holiday dates from CSV file (1990-2024+)
- Fallback to basic recurring holidays if file unavailable
- Used by all downloader scripts to skip market-closed days

## Directory Structure

```bash
nse_download/
├── download_nse_data_headless.py  # Main HTTP-based downloader
├── download_nse_data_browser.py   # Browser automation downloader
├── analyze_existing_files.py      # File analysis tool
├── indian_holidays.py             # Holiday configuration
├── logs/                          # Log files and status reports
│   ├── nse_download-*.log         # Application logs
│   └── download_status-*.csv      # Download status reports
├── data/                          # Downloaded and extracted data
│   └── output/
│       ├── raw/                   # Raw zip files
│       └── curated/               # Extracted CSV files
└── README.md                      # This file
```

## Key Classes

### StatusLogger Class

Located in `download_nse_data_headless.py`, this class handles all logging and status tracking:

- **CSV Status Reports**: Tracks download status, file paths, sizes, and data shapes
- **Failed Downloads JSON**: Maintains a record of failed download attempts
- **Comprehensive Logging**: File and console logging with different levels
- **Organized Outputs**: All logs and reports stored in `./logs` directory

## Dependencies

- Python 3.8+
- requests (for HTTP-based downloader)
- pandas (for file analysis)
- selenium (for browser-based downloader)
- webdriver-manager (for automatic ChromeDriver management)
- Standard library modules: argparse, csv, json, logging, pathlib, zipfile

## Output Files

All scripts generate organized outputs in the `./logs` directory:

- **Log files**: `nse_download-YYYYMMDD_HHMMSS.log` - Comprehensive application logging
- **Status CSV**: `download_status-YYYYMMDD_HHMMSS.csv` - Download status with file details
- **Data files**: Organized in `./data/output/` with raw zips and extracted CSVs

## Installation & Setup

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **For browser-based downloader, ensure Chrome is installed**

3. **Run the scripts from the project root directory**

## Usage Examples

### Download recent data (default range)

```bash
python download_nse_data_headless.py
```

### Download specific month

```bash
python download_nse_data_headless.py --start-date 2025-01-01 --end-date 2025-01-31
```

### Analyze existing files

```bash
python analyze_existing_files.py --directory ./data/output/curated --output-dir ./logs
```

## Notes

- Scripts automatically skip weekends and NSE holidays using `indian_holidays.py`
- Status tracking helps identify and retry failed downloads
- The headless downloader is faster and more reliable for bulk downloads
- Browser-based downloader may be needed if NSE changes their website structure
- All logging and status files are timestamped for easy tracking</content>
  <parameter name="filePath">e:\ws-ip\investor_agent_data\nse_download\README.md
