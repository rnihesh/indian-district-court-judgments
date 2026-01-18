# Indian District Court Judgments Scraper

This project scrapes and archives Indian District Court judgments from services.ecourts.gov.in.

## Project Structure

```
.
├── download.py              # Main entry point for scraping
├── archive_manager.py       # S3 archive management (TAR format)
├── process_metadata.py      # Metadata processing and parquet generation
├── sync_s3.py               # S3 sync module for incremental updates
├── sync_s3_fill.py          # Gap-filling for historical data
├── scrape_courts.py         # Court hierarchy scraper
├── courts.csv               # All 3,567 court complexes (generated)
├── src/
│   ├── captcha_solver/      # CAPTCHA solving module
│   │   ├── main.py          # CAPTCHA solver entry point
│   │   ├── tokenizer_base.py # Tokenizer for CAPTCHA
│   │   └── captcha.onnx     # ONNX model for CAPTCHA
│   └── utils/
│       ├── court_utils.py   # Court data structures and CSV loading
│       └── html_utils.py    # HTML parsing utilities
```

## S3 Structure

All archives use uncompressed TAR format (`.tar`).

```
indian-district-court-judgments-test/
├── data/
│   └── tar/
│       └── year=YYYY/state=XX/district=YY/complex=ZZ/
│           ├── orders.tar
│           └── orders.index.json
└── metadata/
    ├── tar/
    │   └── year=YYYY/state=XX/district=YY/complex=ZZ/
    │       ├── metadata.tar
    │       └── metadata.index.json
    └── parquet/
        └── year=YYYY/state=XX/
            └── metadata.parquet
```

## Configuration

Key constants in `download.py`:

- `S3_BUCKET`: Target S3 bucket (default: `indian-district-court-judgments-test`)
- `S3_PREFIX`: S3 prefix (default: empty)
- `LOCAL_DIR`: Local directory for temporary files (default: `./local_dc_judgments_data`)
- `BASE_URL`: eCourts API base URL (`https://services.ecourts.gov.in/ecourtindia_v6/`)

## Usage

### Prerequisites

```bash
# Install dependencies
uv sync

# For S3 access (requires AWS credentials)
export AWS_PROFILE=your-profile
```

### Scrape Data for a Date Range

```bash
# Scrape judgments for a specific state and date range
python download.py --state_code 29 --start_date 2025-01-01 --end_date 2025-01-07

# Scrape with multiple workers
python download.py --state_code 29 --start_date 2025-01-01 --end_date 2025-01-07 --max_workers 10
```

### Sync with S3

```bash
# Sync and download new data since last update
python download.py --sync-s3

# Fill historical gaps (processes in 5-year chunks per run)
python download.py --sync-s3-fill --timeout-hours 5.5
```

### Generate Parquet Files

```bash
# Process all metadata to parquet
python process_metadata.py

# Process specific year/state
python process_metadata.py --year 2025 --state 29
```

### Command Line Arguments

| Argument          | Description                     | Default |
| ----------------- | ------------------------------- | ------- |
| `--start_date`    | Start date (YYYY-MM-DD)         | None    |
| `--end_date`      | End date (YYYY-MM-DD)           | None    |
| `--day_step`      | Days per chunk                  | 1       |
| `--max_workers`   | Parallel workers                | 5       |
| `--state_code`    | Filter by state code            | None    |
| `--district_code` | Filter by district code         | None    |
| `--complex_code`  | Filter by complex code          | None    |
| `--sync-s3`       | Sync mode (incremental updates) | False   |
| `--sync-s3-fill`  | Gap-filling mode                | False   |
| `--timeout-hours` | Max runtime hours               | 5.5     |

## Index File Format (V2)

Each archive has an accompanying `.index.json` file:

```json
{
  "year": 2025,
  "state_code": "29",
  "district_code": "22",
  "complex_code": "1290148",
  "archive_type": "orders",
  "file_count": 578,
  "total_size": 85000000,
  "total_size_human": "81.01 MB",
  "created_at": "2025-01-14T19:25:47+05:30",
  "updated_at": "2025-01-14T19:25:47+05:30",
  "parts": [
    {
      "name": "orders.tar",
      "files": ["file1.pdf", "file2.pdf", ...],
      "file_count": 578,
      "size": 85000000,
      "size_human": "81.01 MB",
      "created_at": "2025-01-14T19:25:47+05:30"
    }
  ]
}
```

## Archive Types

- **orders**: PDF judgment/order documents
- **metadata**: JSON metadata files with case details

## Court Hierarchy

- **36 States/UTs**
- **~700+ Districts**
- **3,567 Court Complexes**

Data in `courts.csv` with columns:

- `state_code`, `state_name`
- `district_code`, `district_name`
- `complex_code`, `complex_name`
- `court_numbers`, `flag`

## Data Flow

1. **Scraping**: `download.py` scrapes the eCourts website
2. **Local Storage**: Files saved to `./local_dc_judgments_data/{year}/{state}/{district}/{complex}/`
3. **TAR Packaging**: `archive_manager.py` creates TAR archives
4. **S3 Upload**: Archives uploaded to S3 with indexes
5. **Parquet**: `process_metadata.py` converts metadata to parquet

## Key Components

### archive_manager.py

Manages TAR archives in S3 with:

- Multi-part support for large archives (>1GB splits)
- Index file tracking (V2 format with parts array)
- Immediate upload mode for crash recovery
- File existence checking against S3
- 3-level hierarchy: state/district/complex

### process_metadata.py

Metadata processing:

- Reads metadata TAR files from S3
- Extracts structured data from raw HTML
- Parses order dates, case numbers
- Generates parquet files for analytics
- Aggregates by year/state

### API Endpoints

| Endpoint                        | Purpose                            |
| ------------------------------- | ---------------------------------- |
| `?p=casestatus/fillDistrict`    | Get districts for a state          |
| `?p=casestatus/fillcomplex`     | Get court complexes for a district |
| `?p=casestatus/set_data`        | Set session court context          |
| `?p=courtorder/submitOrderDate` | Search orders by date              |
| `?p=home/display_pdf`           | Get PDF download URL               |

### Session Management

- **Cookies**: `SERVICES_SESSID`, `JSESSION`
- **app_token**: Changes with EVERY API call (must chain from response to next request)
- **CAPTCHA**: 6-character alphanumeric, solved via ONNX model

## Notes

- All archives use uncompressed TAR format for faster read/write
- IST timezone (UTC+5:30) used for timestamps
- CAPTCHA solving uses ONNX model (src/captcha_solver/)
- No `__init__.py` files in src/ (direct imports used)
- Retry logic with exponential backoff for network errors
- 1GB maximum archive size before splitting into parts
