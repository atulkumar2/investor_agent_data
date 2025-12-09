# NSE Bhavcopy Data Downloader

Automated script to download full bhavcopy and security deliverable data from NSE India website.

## Scripts

### 1. download_nse_bhavcopy.py - Data Downloader

Downloads NSE bhavcopy files for specified date ranges

### 2. analyze_existing_files.py - File Analyzer

Analyzes existing NSE bhavcopy files in a directory and identifies missing dates

### 3. indian_holidays.py - Public Holiday Configuration

Shared module containing NSE market holidays. Loads actual holiday dates from a
comprehensive CSV file (667+ holidays from 1990-2024+), with fallback to basic
recurring holidays if the file is unavailable. Both the downloader and analyzer
use this module to skip market closed days.

## Features

- ✅ Automated navigation through NSE website with calendar-based date selection
- ✅ Configurable date range via command-line arguments
- ✅ Weekly browser session batching (Monday-Friday) for improved performance
- ✅ Intelligent operation skipping - full navigation only on first day of week
- ✅ Automatic weekend and public holiday skipping with logged entries
- ✅ Shared public holiday configuration across all scripts
- ✅ Rotating browser user agents for each download
- ✅ Optimized wait times (5 seconds per selector)
- ✅ 60-second download wait with retry mechanism
- ✅ Random sleep intervals between downloads (3-7 seconds)
- ✅ Comprehensive logging to file and console (stored in `logs/` folder)
- ✅ CSV summary table with download status, file size, and pandas shape analysis
- ✅ Tracks failed downloads with weekday information

## Prerequisites

- Python 3.8 or higher
- Google Chrome browser installed
- ChromeDriver (will be managed automatically by selenium)

## Installation

### Option 1: Using uv (recommended)

Create or refresh `requirements.txt` from `pyproject.toml`:

```powershell
uv pip compile pyproject.toml -o requirements.txt
```

Then install dependencies into your environment:

```powershell
uv sync
```

### Option 2: Using plain pip (without uv)

If you prefer not to use `uv`, you can use `requirements.txt` directly with `pip`:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Code Quality and Formatting

### Pre-commit Hooks

This repository uses pre-commit hooks to automatically fix basic linting issues before commits. The hooks handle:

- Removing trailing whitespace
- Ensuring files end with a newline
- Formatting Python code with Black
- Sorting imports with isort
- Linting with flake8
- Formatting Markdown files

#### Installing Pre-commit Hooks

1. Install pre-commit:

```powershell
pip install pre-commit
```

Or using uv:

```powershell
uv add --dev pre-commit
```

2. Install the hooks:

```powershell
pre-commit install
```

3. (Optional) Run on all files:

```powershell
pre-commit run --all-files
```

Now, pre-commit will automatically run on every commit, fixing issues where possible.

### Editor Configuration for On-Save Formatting

To automatically fix linting issues when saving files (before committing), configure your editor to format on save. This complements the pre-commit hooks and provides immediate feedback.

#### VS Code Configuration

1. Install the following extensions:
   - Python (Microsoft)
   - Pylint (Microsoft) or Flake8
   - Black Formatter
   - isort
   - Prettier - Code formatter (for Markdown)

2. Update your `settings.json` (User or Workspace):

```json
{
    "python.formatting.provider": "black",
    "python.formatting.blackArgs": ["--line-length", "88"],
    "python.sortImports.args": ["--profile", "black"],
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    },
    "files.trimTrailingWhitespace": true,
    "files.insertFinalNewline": true,
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter"
    },
    "[markdown]": {
        "editor.defaultFormatter": "esbenp.prettier-vscode"
    }
}
```

3. For Markdown formatting, ensure Prettier is configured for `.md` files.

This setup will automatically format Python code with Black, sort imports with isort, remove trailing whitespace, ensure files end with newlines, and format Markdown files on save.

## Usage

### Basic Usage (with defaults)

```powershell
python download_nse_bhavcopy.py
```

This will download data from July 1-10, 2025 to `data/202507/`

### Custom Date Range

```powershell
python download_nse_bhavcopy.py --start-date 2025-06-01 --end-date 2025-06-15
```

### Custom Output Directory

```powershell
python download_nse_bhavcopy.py --start-date 2025-07-01 --end-date 2025-07-10 --output-dir data/custom_folder
```

### Using uv

```powershell
uv run python download_nse_bhavcopy.py --start-date 2025-07-01 --end-date 2025-07-10
```

### Command-line Arguments

- `--start-date`: Start date in YYYY-MM-DD format (default: 2025-07-01)
- `--end-date`: End date in YYYY-MM-DD format (default: 2025-07-10)
- `--output-dir`: Output directory (default: data/YYYYMM based on start date)

### Examples

Download data for entire June 2025:

```powershell
python download_nse_bhavcopy.py --start-date 2025-06-01 --end-date 2025-06-30
```

Download single day:

```powershell
python download_nse_bhavcopy.py --start-date 2025-07-15 --end-date 2025-07-15
```

## Analyzing Existing Files

Use `analyze_existing_files.py` to scan a directory containing NSE bhavcopy files:

```powershell
uv run python analyze_existing_files.py --input-dir "C:\path\to\nse\data" --output-dir analysis
```

This will generate:

1. `existing_files_summary.csv` - Details of all found files with size and shape
2. `missing_files.csv` - List of missing weekday dates (excluding weekends and public holidays)

### Analysis Options

- `--input-dir`: Directory containing NSE CSV files (required)
- `--output-dir`: Where to save analysis results (default: analysis)
- `--no-recursive`: Search only in the specified directory, not subdirectories

## Public Holidays

The `indian_holidays.py` module manages NSE market holidays:

**Primary Source**: Loads from comprehensive CSV file with 667+ actual NSE holidays (1990-2024+)

- Path: `nse_holidays.csv` (in repository root)
- Includes all festival holidays, national holidays, and special market closures

**Fallback**: If CSV file is unavailable, uses basic recurring holidays:

- **Republic Day** - January 26
- **Labour Day** - May 1  
- **Independence Day** - August 15
- **Gandhi Jayanti** - October 2
- **Christmas** - December 25

Both scripts automatically skip these dates as the market is closed.

## Output

The script creates the following:

### In the output directory (e.g., `data/202507/`)

1. **Downloaded CSV files**: `sec_bhavdata_full_DDMMYYYY.csv`

### In the `logs/` folder

1. **download_log_YYYYMMDD_YYYYMMDD.txt**: Detailed log of all operations
2. **download_summary_YYYYMMDD_YYYYMMDD.csv**: Summary table with columns:
   - Date
   - Weekday
   - Status (Success/Failed/Skipped)
   - Filename
   - Error (if any)
   - File_Size_KB
   - Rows (from pandas shape analysis)
   - Columns (from pandas shape analysis)

## Configuration

You can modify the following variables in the script:

- `SLEEP_MIN` and `SLEEP_MAX`: Sleep interval range between downloads (default: 3-7 seconds)
- `USER_AGENTS`: List of browser user agents to rotate

## Notes

- The script uses Selenium WebDriver with Chrome to automate browser interactions
- Direct URL navigation to archives page: `https://www.nseindia.com/all-reports#cr_equity_archives`
- Calendar interaction using `gj-picker` class for date selection
- Weekly browser session batching: new session starts Monday, reused through Friday
- First day of week performs full navigation, search, and checkbox selection
- Subsequent days in the same week skip directly to date selection for speed
- Weekends (Saturday/Sunday) are automatically skipped with logged entries
- Public holidays are automatically skipped with logged entries (see indian_holidays.py)
- User agents are rotated for each weekly session
- Sleep intervals are randomized to avoid triggering rate limits
- Failed downloads are logged with their weekday for reference
- Download waits up to 60 seconds with 2-second retry interval
- File analysis performed after all downloads complete using pandas
