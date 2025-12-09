"""
Analyze existing NSE bhavcopy files in a directory.

This script scans a directory for NSE bhavcopy CSV files and generates:
1. A summary CSV with file details (date, size, shape)
2. A missing files CSV listing potentially missing dates
"""

import argparse
import csv
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from indian_holidays import is_public_holiday

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def parse_filename_date(filename):
    """
    Extract date from NSE bhavcopy filename.

    Expected format: sec_bhavdata_full_DDMMYYYY.csv

    Args:
        filename: Name of the file

    Returns:
        datetime object or None if date cannot be parsed
    """
    pattern = r'sec_bhavdata_full_(\d{2})(\d{2})(\d{4})\.csv'
    match = re.search(pattern, filename)

    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            logging.warning("Invalid date in filename: %s", filename)
            return None
    return None


def get_file_size_kb(filepath):
    """Get file size in KB."""
    return os.path.getsize(filepath) / 1024


def get_csv_shape(filepath):
    """
    Get shape of CSV file using pandas.

    Args:
        filepath: Path to CSV file

    Returns:
        Tuple of (rows, columns) or (None, None) if error
    """
    try:
        df = pd.read_csv(filepath)
        return df.shape
    except Exception as e:
        logging.error("Error reading %s: %s", filepath, e)
        return None, None


def analyze_directory(directory_path, output_dir, recursive=True):
    """
    Analyze all NSE bhavcopy files in directory.

    Args:
        directory_path: Path to directory containing CSV files
        output_dir: Directory to save analysis results
        recursive: If True, search subdirectories recursively

    Returns:
        List of file information dictionaries
    """
    directory = Path(directory_path)
    if not directory.exists():
        logging.error("Directory does not exist: %s", directory_path)
        return []

    files_info = []

    # Find all matching CSV files
    pattern = "sec_bhavdata_full_*.csv"
    if recursive:
        csv_files = list(directory.rglob(pattern))
    else:
        csv_files = list(directory.glob(pattern))

    logging.info("Found %d files matching pattern: %s", len(csv_files), pattern)

    for csv_file in csv_files:
        filename = csv_file.name
        file_date = parse_filename_date(filename)

        if file_date is None:
            logging.warning("Skipping file with unparseable date: %s", filename)
            continue

        # Get file details
        file_size_kb = get_file_size_kb(csv_file)
        rows, columns = get_csv_shape(csv_file)

        file_info = {
            'Filename': filename,
            'Date': file_date.strftime('%Y-%m-%d'),
            'Weekday': file_date.strftime('%A'),
            'File_Size_KB': f"{file_size_kb:.2f}",
            'Rows': rows if rows is not None else 'Error',
            'Columns': columns if columns is not None else 'Error',
            'Full_Path': str(csv_file)
        }

        files_info.append(file_info)
        logging.info(
            "Processed: %s - Date: %s, Size: %.2f KB, Shape: (%s, %s)",
            filename,
            file_info['Date'],
            file_size_kb,
            rows,
            columns
        )

    # Sort by date
    files_info.sort(key=lambda x: x['Date'])

    return files_info


def find_missing_dates(files_info, output_dir):
    """
    Find potentially missing dates between min and max dates in files.

    Args:
        files_info: List of file information dictionaries
        output_dir: Directory to save missing files report

    Returns:
        List of missing date information dictionaries
    """
    if not files_info:
        logging.warning("No files to analyze for missing dates")
        return []

    # Parse dates
    dates = [datetime.strptime(f['Date'], '%Y-%m-%d') for f in files_info]
    min_date = min(dates)
    max_date = max(dates)

    logging.info(
        "Date range: %s to %s",
        min_date.strftime('%Y-%m-%d'),
        max_date.strftime('%Y-%m-%d')
    )

    # Create set of existing dates
    existing_dates = set(dates)

    # Find missing dates (excluding weekends and public holidays)
    missing_info = []
    current_date = min_date

    while current_date <= max_date:
        weekday = current_date.weekday()

        # Skip weekends entirely
        if weekday >= 5:
            current_date += timedelta(days=1)
            continue

        # Skip public holidays
        if is_public_holiday(current_date):
            current_date += timedelta(days=1)
            continue

        if current_date not in existing_dates:
            missing_info.append({
                'Date': current_date.strftime('%Y-%m-%d'),
                'Weekday': current_date.strftime('%A'),
                'Expected_Filename': f"sec_bhavdata_full_{current_date.strftime('%d%m%Y')}.csv"
            })

        current_date += timedelta(days=1)

    logging.info(
        "Found %d missing weekday dates (excluding public holidays)",
        len(missing_info)
    )

    return missing_info


def save_results(files_info, missing_info, output_dir):
    """
    Save analysis results to CSV files.

    Args:
        files_info: List of file information dictionaries
        missing_info: List of missing date information dictionaries
        output_dir: Directory to save results
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Save files summary
    if files_info:
        summary_file = output_path / 'existing_files_summary.csv'
        fieldnames = ['Filename', 'Date', 'Weekday', 'File_Size_KB', 'Rows', 'Columns', 'Full_Path']

        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(files_info)

        logging.info("Saved files summary to: %s", summary_file)

    # Save missing dates
    if missing_info:
        missing_file = output_path / 'missing_files.csv'
        fieldnames = ['Date', 'Weekday', 'Expected_Filename']

        with open(missing_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(missing_info)

        logging.info("Saved missing files report to: %s", missing_file)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Analyze existing NSE bhavcopy files and find missing dates'
    )

    parser.add_argument(
        '--input-dir',
        type=str,
        required=True,
        help='Directory containing NSE bhavcopy CSV files'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='analysis',
        help='Directory to save analysis results (default: analysis)'
    )

    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='Do not search subdirectories recursively'
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()

    logging.info("Starting analysis of directory: %s", args.input_dir)

    # Analyze existing files
    files_info = analyze_directory(
        args.input_dir,
        args.output_dir,
        recursive=not args.no_recursive
    )

    if not files_info:
        logging.error("No valid files found to analyze")
        return

    logging.info("Successfully analyzed %d files", len(files_info))

    # Find missing dates
    missing_info = find_missing_dates(files_info, args.output_dir)

    # Save results
    save_results(files_info, missing_info, args.output_dir)

    # Print summary
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    print(f"Total files found: {len(files_info)}")
    print(f"Date range: {files_info[0]['Date']} to {files_info[-1]['Date']}")
    print(f"Missing weekday dates: {len(missing_info)}")

    if missing_info:
        print("\nMissing weekday dates:")
        for m in missing_info[:10]:  # Show first 10
            print(f"  - {m['Date']} ({m['Weekday']})")
        if len(missing_info) > 10:
            print(f"  ... and {len(missing_info) - 10} more")

    print(f"\nResults saved to: {args.output_dir}/")
    print("="*60)


if __name__ == '__main__':
    main()
