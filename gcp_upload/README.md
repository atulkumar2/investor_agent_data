# NSE Data Processing and GCS Upload

This folder contains scripts to process NSE CSV files, create a local folder structure mimicking Google Cloud Storage (GCS), convert CSV files to Parquet format, and generate a processing status report.

## Prerequisites

- Python 3.x
- pandas
- pyarrow
- Google Cloud SDK (for GCS upload)

## Usage

Run the script to process NSE CSV files:

```bash
python create_local_structure.py <input_directory> <output_directory> [--pattern <glob_pattern>] [--force]
```

### Arguments

- `raw_root`: Root folder containing raw NSE CSV files (input)
- `output_root`: Root folder where Parquet files and raw copies will be written (output)
- `--pattern`: Glob pattern to match CSV files (default: `sec_bhavdata_full_*.csv`)
- `--force`: Force copying of input CSV files even if they already exist in the raw directory

### Example

```bash
python create_local_structure.py ./data/input ./data/output --force
```

## Output Structure

The script creates the following structure in the output directory:

```bash
output/
├── raw/
│   └── cm/
│       └── year=YYYY/
│           └── month=MM/
│               └── sec_bhavdata_full_DDMMYYYY.csv
├── curated/
│   └── cm/
│       └── year=YYYY/
│           └── month=MM/
│               └── day=DD.parquet
└── logs/
    ├── build_daily_parquet_YYYYMMDD_HHMMSS.log
    └── file_processing_status-YYYYMMDD_HHMMSS.csv
```

## Processing Status Report

A CSV file `file_processing_status-YYYYMMDD_HHMMSS.csv` is generated in the `logs/` directory with the following columns:

- File name
- Processing status (Success/Skipped/Error)
- Output file name
- File date
- Weekday
- Input file size
- Output file size
- Input file shape
- Input file path
- Output file path
- Copied input file path

## Uploading to Google Cloud Storage

After processing, upload the generated files to GCS using `gcloud storage rsync`. Refer to the [Google Cloud Storage documentation](https://docs.cloud.google.com/storage/docs/working-with-big-data) for more details on working with big data.

### Sync Output Directory to GCS Bucket

```bash
gcloud storage rsync ./data/output gs://your-bucket-name/nse-data --recursive
```

Replace `your-bucket-name` with your actual GCS bucket name. The `--recursive` flag ensures all subdirectories are synced.

### Additional Options

- To delete files in the destination that are not in the source: `--delete-unmatched-destination-objects`
- To preview changes without syncing: `--dry-run`

Example with dry run:

```bash
gcloud storage rsync ./data/output gs://your-bucket-name/nse-data --recursive --dry-run
```

## Logs

All processing logs are saved to `logs/build_daily_parquet_YYYYMMDD_HHMMSS.log` with timestamps and detailed information about each file processed.
