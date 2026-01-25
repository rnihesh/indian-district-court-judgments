"""
eCourts Mobile API Scraper with S3 Sync.

Full scraper for district court judgments using the mobile API.
Supports PDF downloads, S3 archival, and robust retry logic.

FEATURES:
- No CAPTCHA required (mobile API advantage)
- Downloads PDFs directly
- Syncs to S3 using archive_manager
- Robust retry logic with exponential backoff
- Resume capability via S3 index files
- Parquet metadata generation

USAGE:
    # Scrape specific district
    uv run python scraper.py --state 29 --district 22 --start-year 1950 --end-year 2025

    # Dry run (no S3 upload)
    uv run python scraper.py --state 29 --district 22 --local-only

    # Generate parquet from scraped data
    uv run python scraper.py --generate-parquet --state 29
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

import urllib3

# Suppress SSL warnings - eCourts API uses certificate that doesn't verify
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from archive_manager import S3ArchiveManager, format_size
from api_client import MobileAPIClient, State, District, CourtComplex, CaseType, Case, Order
from crypto import decrypt_url_param
from gs import check_ghostscript_available, compress_pdf_bytes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

# Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "indian-district-court-judgments-test")
LOCAL_DIR = Path("./local_mobile_data")
DEFAULT_DELAY = 0.3  # Seconds between API calls

# Check if Ghostscript is available for PDF compression
COMPRESSION_AVAILABLE = check_ghostscript_available()
if COMPRESSION_AVAILABLE:
    logger.info("PDF compression enabled (Ghostscript found)")
else:
    logger.warning("PDF compression not available (Ghostscript not found)")


class MobileScraper:
    """
    Enhanced scraper for eCourts Mobile API with S3 sync.
    """

    def __init__(
        self,
        s3_bucket: str = S3_BUCKET,
        local_dir: Path = LOCAL_DIR,
        delay: float = DEFAULT_DELAY,
        max_retries: int = 5,
        local_only: bool = False,
        immediate_upload: bool = True,
        compress_pdfs: bool = True,
    ):
        self.client = MobileAPIClient()
        self.s3_bucket = s3_bucket
        self.local_dir = Path(local_dir)
        self.delay = delay
        self.max_retries = max_retries
        self.local_only = local_only
        self.immediate_upload = immediate_upload
        self.compress_pdfs = compress_pdfs and COMPRESSION_AVAILABLE

        self.archive_manager: Optional[S3ArchiveManager] = None
        self._interrupted = False

        self.stats = {
            "states_processed": 0,
            "districts_processed": 0,
            "complexes_processed": 0,
            "case_types_processed": 0,
            "cases_found": 0,
            "cases_processed": 0,
            "cases_skipped": 0,
            "pdfs_downloaded": 0,
            "pdfs_compressed": 0,
            "pdfs_failed": 0,
            "bytes_saved": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None,
        }

        # Set up signal handler for clean shutdown
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)

    def _handle_interrupt(self, signum, frame):
        """Handle interrupt signal for clean shutdown."""
        if self._interrupted:
            logger.warning("Forced exit - archives may be incomplete!")
            sys.exit(1)

        logger.info("\nInterrupt received - finishing current operation and saving...")
        self._interrupted = True

    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry."""
        for attempt in range(self.max_retries):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")

            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) + (0.1 * attempt)
                time.sleep(wait_time)

        return None

    def _ensure_session(self) -> bool:
        """Ensure API client has valid session."""
        if not self.client._initialized:
            return self._retry_with_backoff(self.client.initialize_session)
        return True

    def _extract_pdf_filename(self, pdf_url: str) -> Optional[str]:
        """Extract PDF filename from encrypted URL params."""
        try:
            parsed = urlparse(pdf_url)
            query = parse_qs(parsed.query)
            params_enc = query.get("params", [""])[0]
            if params_enc:
                params = decrypt_url_param(params_enc)
                if isinstance(params, dict):
                    return params.get("filename", "")
        except Exception:
            pass
        return None

    def _build_case_metadata(
        self,
        case: Case,
        state: State,
        district: District,
        complex_: CourtComplex,
        history: dict,
        final_orders: list,
        interim_orders: list,
    ) -> dict:
        """Build metadata dictionary for a case."""
        def order_to_dict(o: Order) -> dict:
            data = {
                "order_number": o.order_number,
                "order_date": o.order_date,
                "order_type": o.order_type,
                "pdf_url": o.pdf_url,
                "is_final": o.is_final,
            }
            if o.pdf_url:
                filename = self._extract_pdf_filename(o.pdf_url)
                if filename:
                    data["pdf_filename"] = filename
            return data

        return {
            "case_summary": {
                "case_no": case.case_no,
                "cino": case.cino,
                "case_type": case.case_type,
                "case_number": case.case_number,
                "reg_year": case.reg_year,
                "petitioner": case.petitioner,
                "court_code": case.court_code,
            },
            "location": {
                "state_code": state.code,
                "state_name": state.name,
                "district_code": district.code,
                "district_name": district.name,
                "complex_code": complex_.code,
                "complex_name": complex_.name,
            },
            "orders": {
                "final_orders": [order_to_dict(o) for o in final_orders],
                "interim_orders": [order_to_dict(o) for o in interim_orders],
                "has_pdf": bool(final_orders or interim_orders),
            },
            "history": history,
            "scraped_at": datetime.now(IST).isoformat(),
            "source": "mobile_api",
        }

    def _get_year_from_case(self, case: Case) -> int:
        """Extract year from case data."""
        try:
            if case.reg_year:
                return int(case.reg_year)
        except (ValueError, TypeError):
            pass
        return datetime.now().year

    def scrape_case(
        self,
        case: Case,
        state: State,
        district: District,
        complex_: CourtComplex,
        year: int,
    ) -> bool:
        """
        Scrape a single case: get history, download PDFs, save metadata.

        Returns True if case was processed successfully.
        """
        # Check if already processed
        metadata_filename = f"{case.case_no}.json"
        if self.archive_manager.file_exists(
            year=year,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            archive_type="metadata",
            filename=metadata_filename,
        ):
            self.stats["cases_skipped"] += 1
            return True

        time.sleep(self.delay)

        # Get case history
        history = self._retry_with_backoff(
            self.client.get_case_history,
            state_code=state.code,
            dist_code=district.code,
            court_code=case.court_code,
            case_no=case.case_no,
        )

        if not history:
            logger.warning(f"Failed to get history for case {case.case_no}")
            self.stats["errors"] += 1
            return False

        # Extract orders
        final_orders, interim_orders = self.client.get_orders_from_history(history)

        # Build metadata
        metadata = self._build_case_metadata(
            case, state, district, complex_, history, final_orders, interim_orders
        )

        # Download PDFs
        all_orders = final_orders + interim_orders
        for order in all_orders:
            if order.pdf_url:
                pdf_filename = self._extract_pdf_filename(order.pdf_url)
                if not pdf_filename:
                    continue

                # Clean filename for archive
                clean_filename = pdf_filename.lstrip("/").replace("/", "_")

                # Check if PDF already exists
                if self.archive_manager.file_exists(
                    year=year,
                    state_code=str(state.code),
                    district_code=str(district.code),
                    complex_code=str(complex_.code),
                    archive_type="orders",
                    filename=clean_filename,
                ):
                    continue

                # Download PDF
                temp_path = self.local_dir / "temp" / clean_filename
                temp_path.parent.mkdir(parents=True, exist_ok=True)

                time.sleep(self.delay)
                success = self._retry_with_backoff(
                    self.client.download_pdf,
                    pdf_url=order.pdf_url,
                    output_path=str(temp_path),
                )

                if success and temp_path.exists():
                    # Read PDF content
                    with open(temp_path, "rb") as f:
                        pdf_content = f.read()
                    original_size = len(pdf_content)

                    # Compress PDF if enabled
                    if self.compress_pdfs:
                        compressed_content = compress_pdf_bytes(
                            pdf_content, self.local_dir / "temp"
                        )
                        if len(compressed_content) < original_size:
                            saved = original_size - len(compressed_content)
                            self.stats["bytes_saved"] += saved
                            self.stats["pdfs_compressed"] += 1
                            pdf_content = compressed_content

                    # Add to archive
                    self.archive_manager.add_to_archive(
                        year=year,
                        state_code=str(state.code),
                        district_code=str(district.code),
                        complex_code=str(complex_.code),
                        archive_type="orders",
                        filename=clean_filename,
                        content=pdf_content,
                    )
                    self.stats["pdfs_downloaded"] += 1

                    # Clean up temp file
                    temp_path.unlink()
                else:
                    logger.warning(f"Failed to download PDF: {pdf_filename}")
                    self.stats["pdfs_failed"] += 1

        # Save metadata to archive
        metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2)
        self.archive_manager.add_to_archive(
            year=year,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            archive_type="metadata",
            filename=metadata_filename,
            content=metadata_json,
        )

        self.stats["cases_processed"] += 1
        return True

    def scrape_complex(
        self,
        state: State,
        district: District,
        complex_: CourtComplex,
        years: list[int],
        pending_disposed: str = "Both",
    ) -> int:
        """
        Scrape all cases from a court complex.

        Returns number of cases processed.
        """
        processed = 0

        # Get case types
        time.sleep(self.delay)
        case_types = self._retry_with_backoff(
            self.client.get_case_types,
            state_code=state.code,
            dist_code=district.code,
            court_code=complex_.njdg_est_code,
        )

        if not case_types:
            logger.warning(f"No case types found for {complex_.name}")
            return 0

        logger.info(f"    Found {len(case_types)} case types")

        for ct in case_types:
            self.stats["case_types_processed"] += 1

            for year in years:
                # Search for cases
                time.sleep(self.delay)

                # Try both Pending and Disposed if "Both"
                statuses = ["Pending", "Disposed"] if pending_disposed == "Both" else [pending_disposed]

                for status in statuses:
                    cases = self._retry_with_backoff(
                        self.client.search_cases_by_type,
                        state_code=state.code,
                        dist_code=district.code,
                        court_code=complex_.njdg_est_code,
                        case_type=ct.code,
                        year=year,
                        pending_disposed=status,
                    )

                    if not cases:
                        continue

                    logger.info(f"      {ct.name[:30]} / {year} / {status}: {len(cases)} cases")
                    self.stats["cases_found"] += len(cases)

                    for case in cases:
                        if self._interrupted:
                            logger.info("Stopping due to interrupt...")
                            return processed

                        try:
                            if self.scrape_case(case, state, district, complex_, year):
                                processed += 1
                        except Exception as e:
                            logger.error(f"Error processing case {case.case_no}: {e}")
                            self.stats["errors"] += 1

        return processed

    def scrape(
        self,
        state_codes: Optional[list[int]] = None,
        district_codes: Optional[list[int]] = None,
        complex_codes: Optional[list[str]] = None,
        start_year: int = 2020,
        end_year: int = 2025,
        pending_disposed: str = "Both",
    ) -> dict:
        """
        Main scraping function.

        Args:
            state_codes: Filter by state codes
            district_codes: Filter by district codes
            complex_codes: Filter by complex codes
            start_year: Start year (inclusive)
            end_year: End year (inclusive)
            pending_disposed: "Pending", "Disposed", or "Both"

        Returns:
            Statistics dictionary
        """
        self.stats["start_time"] = datetime.now(IST).isoformat()
        years = list(range(end_year, start_year - 1, -1))  # Recent years first

        logger.info(f"Starting scrape")
        logger.info(f"  Years: {start_year} to {end_year}")
        logger.info(f"  Filter: {pending_disposed}")
        logger.info(f"  S3 Bucket: {self.s3_bucket}")
        logger.info(f"  Local Only: {self.local_only}")

        # Initialize session
        if not self._ensure_session():
            logger.error("Failed to initialize API session")
            return self.stats

        # Initialize archive manager
        self.archive_manager = S3ArchiveManager(
            s3_bucket=self.s3_bucket,
            s3_prefix="",
            local_dir=self.local_dir,
            immediate_upload=self.immediate_upload,
            local_only=self.local_only,
        )

        with self.archive_manager:
            # Get states
            states = self._retry_with_backoff(self.client.get_states)
            if not states:
                logger.error("Failed to get states")
                return self.stats

            if state_codes:
                states = [s for s in states if s.code in state_codes]

            logger.info(f"Processing {len(states)} states")

            for state in states:
                if self._interrupted:
                    logger.info("Stopping due to interrupt - finalizing archives...")
                    break

                logger.info(f"\n[State] {state.name} ({state.code})")
                self.stats["states_processed"] += 1

                # Get districts
                time.sleep(self.delay)
                districts = self._retry_with_backoff(
                    self.client.get_districts,
                    state_code=state.code,
                )

                if not districts:
                    continue

                if district_codes:
                    districts = [d for d in districts if d.code in district_codes]

                logger.info(f"  Processing {len(districts)} districts")

                for district in districts:
                    if self._interrupted:
                        break

                    logger.info(f"\n  [District] {district.name} ({district.code})")
                    self.stats["districts_processed"] += 1

                    # Get court complexes
                    time.sleep(self.delay)
                    complexes = self._retry_with_backoff(
                        self.client.get_court_complexes,
                        state_code=state.code,
                        dist_code=district.code,
                    )

                    if not complexes:
                        continue

                    if complex_codes:
                        complexes = [c for c in complexes if c.code in complex_codes]

                    logger.info(f"    Processing {len(complexes)} court complexes")

                    for complex_ in complexes:
                        if self._interrupted:
                            break

                        logger.info(f"\n    [Complex] {complex_.name} ({complex_.code})")
                        self.stats["complexes_processed"] += 1

                        try:
                            processed = self.scrape_complex(
                                state=state,
                                district=district,
                                complex_=complex_,
                                years=years,
                                pending_disposed=pending_disposed,
                            )
                            logger.info(f"    Processed {processed} cases")
                        except Exception as e:
                            logger.error(f"Error processing complex {complex_.name}: {e}")
                            self.stats["errors"] += 1

        self.stats["end_time"] = datetime.now(IST).isoformat()

        # Print summary
        self._print_summary()

        return self.stats

    def _print_summary(self):
        """Print scraping summary."""
        print("\n" + "=" * 60)
        if self._interrupted:
            print("SCRAPING INTERRUPTED - Archives saved properly")
        else:
            print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"States processed:     {self.stats['states_processed']}")
        print(f"Districts processed:  {self.stats['districts_processed']}")
        print(f"Complexes processed:  {self.stats['complexes_processed']}")
        print(f"Case types processed: {self.stats['case_types_processed']}")
        print(f"Cases found:          {self.stats['cases_found']}")
        print(f"Cases processed:      {self.stats['cases_processed']}")
        print(f"Cases skipped:        {self.stats['cases_skipped']}")
        print(f"PDFs downloaded:      {self.stats['pdfs_downloaded']}")
        if self.compress_pdfs:
            print(f"PDFs compressed:      {self.stats['pdfs_compressed']}")
            saved_mb = self.stats['bytes_saved'] / (1024 * 1024)
            print(f"Space saved:          {saved_mb:.2f} MB")
        print(f"PDFs failed:          {self.stats['pdfs_failed']}")
        print(f"Errors:               {self.stats['errors']}")

        if self.stats["start_time"] and self.stats["end_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            end = datetime.fromisoformat(self.stats["end_time"])
            duration = end - start
            print(f"Duration:             {duration}")


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="eCourts Mobile API Scraper with S3 Sync"
    )

    parser.add_argument(
        "--state",
        type=int,
        action="append",
        dest="states",
        help="State code to scrape (can be specified multiple times)",
    )
    parser.add_argument(
        "--district",
        type=int,
        action="append",
        dest="districts",
        help="District code to scrape (can be specified multiple times)",
    )
    parser.add_argument(
        "--complex",
        type=str,
        action="append",
        dest="complexes",
        help="Complex code to scrape (can be specified multiple times)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Start year (default: 2020)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="End year (default: 2025)",
    )
    parser.add_argument(
        "--filter",
        choices=["Pending", "Disposed", "Both"],
        default="Both",
        help="Case status filter (default: Both)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Delay between API calls in seconds (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Don't upload to S3, keep files locally",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=S3_BUCKET,
        help=f"S3 bucket name (default: {S3_BUCKET})",
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        default=str(LOCAL_DIR),
        help=f"Local directory for temp files (default: {LOCAL_DIR})",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable PDF compression (Ghostscript)",
    )

    args = parser.parse_args()

    scraper = MobileScraper(
        s3_bucket=args.s3_bucket,
        local_dir=Path(args.local_dir),
        delay=args.delay,
        local_only=args.local_only,
        compress_pdfs=not args.no_compress,
    )

    stats = scraper.scrape(
        state_codes=args.states,
        district_codes=args.districts,
        complex_codes=args.complexes,
        start_year=args.start_year,
        end_year=args.end_year,
        pending_disposed=args.filter,
    )

    # Save stats
    stats_file = Path(args.local_dir) / "scrape_stats.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nStats saved to {stats_file}")


if __name__ == "__main__":
    main()
