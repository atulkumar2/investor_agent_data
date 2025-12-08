"""
Create a local folder structure mimicking GCS and convert NSE CSV files to daily Parquet files.
"""

import os
import argparse
import csv
from enum import Enum
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger("build_daily_parquet")
SCRIPT_START_DATETIME = datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
LOG_FOLDER = Path("logs")
LOG_FILENAME = f"build_daily_parquet_{SCRIPT_START_DATETIME}.log"


def _get_file_datetime_from_name(filepath: Path) -> datetime:
    """Get the datetime of the input file."""
    stem = filepath.stem  # e.g. "sec_bhavdata_full_23082019"
    date_str = stem[-8:]  # "23082019"

    try:
        trade_date = datetime.strptime(date_str, "%d%m%Y").date()
        return trade_date
    except ValueError:
        logger.error("Cannot parse date from filename: %s", str(filepath))
        return None

class ProcessingTracker:
    """Tracks processing status and metadata for each file."""

    STATUS_FILENAME = "file_processing_status.csv"

    class Status(Enum):
        """Defines possible processing statuses."""
        SUCCESS = "Success"
        SKIPPED = "Skipped"
        ERROR = "Error"

    class StatusRecord(Enum):
        """Defines the fields for a status record."""
        FILE_NAME = 'File name'
        PROCESSING_STATUS = 'Processing status'
        OUTPUT_FILE_NAME = 'Output file name'
        FILE_DATE = 'File date'
        WEEKDAY = 'Weekday'
        INPUT_FILE_SIZE = 'Input file size'
        OUTPUT_FILE_SIZE = 'Output file size'
        INPUT_FILE_SHAPE = 'Input file shape'
        INPUT_FILE_PATH = 'Input file path'
        OUTPUT_FILE_PATH = 'Output file path'
        COPIED_INPUT_FILE_PATH = 'Copied input file path'

    def __init__(self, input_root_path: Path, output_root_path: Path):
        self.records = []
        self.stats = {
            ProcessingTracker.Status.SUCCESS: 0,
            ProcessingTracker.Status.SKIPPED: 0,
            ProcessingTracker.Status.ERROR: 0
        }
        self._input_root_path = input_root_path
        self._output_root_path = output_root_path

    def add_record(
        self,
        input_filepath: Path,
        status: Status,
        output_filepath: Path = None,
        input_file_shape: int = 0,
        copied_input_filepath: Path = None
    ) -> None:
        """Add a processing status record, extracting metadata from file paths."""
        input_filesize = input_filepath.stat().st_size
        output_size = output_filepath.stat(
          ).st_size if output_filepath and output_filepath.exists() else 0
        output_filename = output_filepath.name if output_filepath else ''

        # Calculate relative paths
        input_relative_path = str(input_filepath.relative_to(self._input_root_path))
        output_relative_path = str(output_filepath.relative_to(
            self._output_root_path)) if output_filepath else ''
        copied_input_relative_path = str(copied_input_filepath.relative_to(
          self._output_root_path)) if copied_input_filepath else ''

        # Use trade_date from filename
        date_for_display = _get_file_datetime_from_name(input_filepath)
        if date_for_display:
            file_date_str = date_for_display.strftime("%Y-%m-%d")
            weekday = date_for_display.strftime("%A")
        else:
            file_date_str = "N/A"
            weekday = "N/A"

        self.records.append({
            ProcessingTracker.StatusRecord.FILE_NAME.value: input_filepath.name,
            ProcessingTracker.StatusRecord.PROCESSING_STATUS.value: status.value,
            ProcessingTracker.StatusRecord.OUTPUT_FILE_NAME.value: output_filename,
            ProcessingTracker.StatusRecord.FILE_DATE.value: file_date_str,
            ProcessingTracker.StatusRecord.WEEKDAY.value: weekday,
            ProcessingTracker.StatusRecord.INPUT_FILE_SIZE.value: input_filesize,
            ProcessingTracker.StatusRecord.OUTPUT_FILE_SIZE.value: output_size,
            ProcessingTracker.StatusRecord.INPUT_FILE_SHAPE.value: input_file_shape,
            ProcessingTracker.StatusRecord.INPUT_FILE_PATH.value: input_relative_path,
            ProcessingTracker.StatusRecord.OUTPUT_FILE_PATH.value: output_relative_path,
            ProcessingTracker.StatusRecord.COPIED_INPUT_FILE_PATH.value: copied_input_relative_path
        })

        # Update statistics
        if status in self.stats:
            self.stats[status] += 1

        logger.debug("Recorded %s status for %s (input: %d bytes, output: %d bytes)",
                     status, input_filepath.name, input_filesize, output_size)

    def save_to_csv(self) -> None:
        """Write status records to CSV file."""
        status_csv_path = (LOG_FOLDER / ProcessingTracker.STATUS_FILENAME.replace(
            ".csv", ("-" + SCRIPT_START_DATETIME + ".csv"))).resolve()
        if os.path.exists(status_csv_path):
            logger.warning("Status report file already exists: %s", status_csv_path)
            return
        try:
            with open(status_csv_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = [
                    ProcessingTracker.StatusRecord.FILE_NAME.value,
                    ProcessingTracker.StatusRecord.PROCESSING_STATUS.value,
                    ProcessingTracker.StatusRecord.OUTPUT_FILE_NAME.value,
                    ProcessingTracker.StatusRecord.FILE_DATE.value,
                    ProcessingTracker.StatusRecord.WEEKDAY.value,
                    ProcessingTracker.StatusRecord.INPUT_FILE_SIZE.value,
                    ProcessingTracker.StatusRecord.OUTPUT_FILE_SIZE.value,
                    ProcessingTracker.StatusRecord.INPUT_FILE_SHAPE.value,
                    ProcessingTracker.StatusRecord.INPUT_FILE_PATH.value,
                    ProcessingTracker.StatusRecord.OUTPUT_FILE_PATH.value,
                    ProcessingTracker.StatusRecord.COPIED_INPUT_FILE_PATH.value
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.records)
            logger.info("Status report saved to: %s (%d records)",
                        status_csv_path, len(self.records))
        except Exception as e:
            logger.error("Failed to save status report to %s: %s", status_csv_path, e)
            raise

    def print_summary(self) -> None:
        """Print processing summary statistics."""
        total_files = sum(self.stats.values())
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info("Total Files Processed: %d", total_files)
        logger.info("  - Success: %d", self.stats[ProcessingTracker.Status.SUCCESS])
        logger.info("  - Skipped: %d", self.stats[ProcessingTracker.Status.SKIPPED])
        logger.info("  - Error: %d", self.stats[ProcessingTracker.Status.ERROR])
        logger.info("=" * 60)


def _build_daily_parquet(
    raw_root: str,
    output_root: str,
    pattern: str = "sec_bhavdata_full_*.csv",
    force: bool = False,
) -> None:
    """
    Scan raw NSE CSV files under raw_root, create a GCS-like folder structure
    under output_root, and write one Parquet file per day.

    Input example:
        raw_root   = "data/NSE_RawData"
        file path  = data/NSE_RawData/201908/sec_bhavdata_full_23082019.csv

    Output example:
        output_root = "data/NSE_Parquet"
        parquet     = data/NSE_Parquet/curated/cm/year=2019/month=08/day=23.parquet

    Assumes filename ends with DDMMYYYY before .csv
    e.g. sec_bhavdata_full_23082019.csv
    """
    raw_root_path = Path(raw_root)
    output_root_path = Path(output_root)

    input_csv_files = list(raw_root_path.rglob(pattern))
    if not input_csv_files:
        logger.warning(
            "No CSV files matching '%s' found under %s", pattern, raw_root_path
        )
        return

    logger.info("Found %d CSV files under %s", len(input_csv_files), raw_root_path)

    tracker = ProcessingTracker(raw_root_path, output_root_path)

    for input_csv_path in input_csv_files:
        trade_date = _get_file_datetime_from_name(input_csv_path)
        if not trade_date:
            logger.error("[ERROR] Cannot parse date from filename: %s",
                         str(input_csv_path.relative_to(raw_root_path)))
            tracker.add_record(input_csv_path, ProcessingTracker.Status.ERROR)
            continue

        # Create raw directory structure and copy input CSV
        raw_out_dir = (
            output_root_path
            / "raw"
            / "cm"
            / f"year={trade_date.year}"
            / f"month={trade_date.month:02d}"
        )
        raw_out_dir.mkdir(parents=True, exist_ok=True)
        copied_input_path = raw_out_dir / input_csv_path.name
        if force or not copied_input_path.exists():
            try:
                shutil.copy2(input_csv_path, copied_input_path)
                logger.info("[COPY] Copied input file to: %s",
                            str(copied_input_path.relative_to(output_root_path)))
            except Exception as e:
                logger.error("[ERROR] Failed to copy input file %s to %s: %s",
                      str(input_csv_path.relative_to(raw_root_path)),
                      str(copied_input_path.relative_to(output_root_path)), e)
                tracker.add_record(input_csv_path,
                      ProcessingTracker.Status.ERROR)
                continue
        else:
            logger.info("[SKIP] Input file already exists at %s",
                        str(copied_input_path.relative_to(output_root_path)))

        # Build curated output directory and file path
        curated_out_dir = (
            output_root_path
            / "curated"
            / "cm"
            / f"year={trade_date.year}"
            / f"month={trade_date.month:02d}"
        )
        curated_out_dir.mkdir(parents=True, exist_ok=True)

        out_file_parquet = curated_out_dir / f"day={trade_date.day:02d}.parquet"

        if not force and out_file_parquet.exists():
            logger.info(
                "[SKIP] Parquet already exists for %s: %s",
                trade_date, str(out_file_parquet.relative_to(output_root_path))
            )
            tracker.add_record(input_csv_path,
                ProcessingTracker.Status.SKIPPED,
                out_file_parquet,
                copied_input_filepath=copied_input_path
            )
            continue

        try:
            logger.info("[PROCESS] %s -> %s",
                str(input_csv_path.relative_to(raw_root_path)), out_file_parquet)

            # Read CSV and write Parquet
            df = pd.read_csv(input_csv_path)
            df.to_parquet(out_file_parquet, engine="pyarrow", index=False)

            tracker.add_record(input_csv_path,
              ProcessingTracker.Status.SUCCESS,
              out_file_parquet, df.shape, copied_input_filepath=copied_input_path)

        except Exception as e:
            logger.error("[ERROR] Failed to process %s: %s",
                str(input_csv_path.relative_to(raw_root_path)), e)
            tracker.add_record(
              input_csv_path,
              ProcessingTracker.Status.ERROR,
              copied_input_filepath=copied_input_path)

    # Write status CSV
    tracker.save_to_csv()

    # Print summary
    tracker.print_summary()

    logger.info("Done building daily Parquet files.")


if __name__ == "__main__":

    LOG_FOLDER.mkdir(parents=True, exist_ok=True)
    log_file = LOG_FOLDER / LOG_FILENAME

    # Configure logging with both file and console handlers
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger.info("Log file created at: %s", log_file)

    parser = argparse.ArgumentParser(
        description=(
            "Create local GCS-like folder structure and convert NSE CSV files "
            "to daily Parquet files."
        )
    )

    parser.add_argument(
        "raw_root",
        help="Root folder containing raw NSE CSV files (input)",
    )
    parser.add_argument(
        "output_root",
        help="Root folder where Parquet files will be written (output)",
    )
    parser.add_argument(
        "--pattern",
        default="sec_bhavdata_full_*.csv",
        help="Glob pattern to match CSV files (default: sec_bhavdata_full_*.csv)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force copying of input CSV files even if they already exist in the raw directory",
    )

    args = parser.parse_args()

    logger.info(
      "Starting build_daily_parquet with raw_root=%s, output_root=%s, pattern=%s, force=%s",
      args.raw_root, args.output_root, args.pattern, args.force)

    _build_daily_parquet(
        raw_root=args.raw_root,
        output_root=args.output_root,
        pattern=args.pattern,
        force=args.force
    )

    logger.info("Script completed.")
    logger.info("Log file saved to: %s", log_file)
