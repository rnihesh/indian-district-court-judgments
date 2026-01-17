"""
Indian District Court Judgments Downloader
Scrapes court orders from services.ecourts.gov.in

Usage:
    # Download locally for a date range
    python download.py --start_date 2025-01-01 --end_date 2025-01-03

    # Download for specific state/district/complex
    python download.py --state_code 24 --district_code 10 --complex_code 2400101 --start_date 2025-01-01 --end_date 2025-01-03

    # S3 sync mode (incremental)
    python download.py --sync-s3

    # S3 fill mode (historical backfill)
    python download.py --sync-s3-fill --timeout-hours 5.5
"""

import argparse
import concurrent.futures
import json
import logging
import random
import re
import sys
import threading
import time
import traceback
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, List, Optional

import colorlog
import requests
import urllib3
from bs4 import BeautifulSoup
from PIL import Image
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
from tqdm import tqdm

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from archive_manager import S3ArchiveManager
from src.captcha_solver.main import get_text
from src.utils.court_utils import CourtComplex, load_courts_csv
from src.gs import check_ghostscript_available, compress_pdf_if_enabled

# Configure logging
root_logger = logging.getLogger()
root_logger.setLevel("INFO")

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(
    colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message=".*pin_memory.*not supported on MPS.*")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Check if Ghostscript is available for PDF compression
COMPRESSION_AVAILABLE = check_ghostscript_available()
if not COMPRESSION_AVAILABLE:
    print("WARNING: PDF compression not available (Ghostscript not found)")

# Configuration
BASE_URL = "https://services.ecourts.gov.in/ecourtindia_v6/"
S3_BUCKET = "indian-district-court-judgments"
S3_PREFIX = ""
LOCAL_DIR = Path("./local_dc_judgments_data")
PACKAGES_DIR = Path("./packages")
IST = timezone(timedelta(hours=5, minutes=30))
START_DATE = "1950-01-01"
COMPLETED_TASKS_FILE = Path("./dc_completed_tasks.json")

# Directories for captcha handling
captcha_tmp_dir = Path("./captcha-tmp")
captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)
captcha_failures_dir.mkdir(parents=True, exist_ok=True)

# Thread lock for completed tasks file
completed_tasks_lock = threading.Lock()

# Request headers
HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://services.ecourts.gov.in",
    "Referer": "https://services.ecourts.gov.in/ecourtindia_v6/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}


def get_task_key(task) -> str:
    """Generate a unique key for a task (court + date combination)"""
    return f"{task.state_code}_{task.district_code}_{task.complex_code}_{task.from_date}_{task.to_date}"


def load_completed_tasks() -> set:
    """Load completed tasks from file"""
    if not COMPLETED_TASKS_FILE.exists():
        return set()
    try:
        with open(COMPLETED_TASKS_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("completed", []))
    except (json.JSONDecodeError, IOError):
        return set()


def save_completed_task(task_key: str):
    """Save a completed task to file (thread-safe)"""
    with completed_tasks_lock:
        completed = load_completed_tasks()
        completed.add(task_key)
        with open(COMPLETED_TASKS_FILE, "w") as f:
            json.dump({"completed": list(completed)}, f)


def is_task_completed(task) -> bool:
    """Check if a task has already been completed"""
    task_key = get_task_key(task)
    completed = load_completed_tasks()
    return task_key in completed


@dataclass
class DistrictCourtTask:
    """A task representing a date range to process for a specific court complex"""

    id: str
    state_code: str
    state_name: str
    district_code: str
    district_name: str
    complex_code: str
    complex_name: str
    court_numbers: str
    from_date: str  # DD-MM-YYYY format
    to_date: str  # DD-MM-YYYY format
    order_type: str = "both"  # "interim", "finalorder", or "both"

    def __str__(self):
        return f"Task({self.state_name}/{self.district_name}/{self.complex_name}, {self.from_date} to {self.to_date})"


def format_date_for_api(date_str: str) -> str:
    """Convert YYYY-MM-DD to DD-MM-YYYY format"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d-%m-%Y")


def parse_date_from_api(date_str: str) -> datetime:
    """Convert DD-MM-YYYY to datetime"""
    return datetime.strptime(date_str, "%d-%m-%Y")


def get_date_ranges(
    start_date: str, end_date: str, day_step: int = 1
) -> Generator[tuple[str, str], None, None]:
    """Generate date ranges in YYYY-MM-DD format"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Cap at today
    today = datetime.now().date()
    if end_dt.date() > today:
        end_dt = datetime.combine(today, datetime.min.time())

    current = start_dt
    while current <= end_dt:
        range_end = min(current + timedelta(days=day_step - 1), end_dt)
        yield (current.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
        current = range_end + timedelta(days=1)


def generate_tasks(
    courts: List[CourtComplex],
    start_date: str,
    end_date: str,
    day_step: int = 1,
) -> Generator[DistrictCourtTask, None, None]:
    """Generate tasks for all courts and date ranges"""
    for from_date, to_date in get_date_ranges(start_date, end_date, day_step):
        for court in courts:
            yield DistrictCourtTask(
                id=str(uuid.uuid4()),
                state_code=court.state_code,
                state_name=court.state_name,
                district_code=court.district_code,
                district_name=court.district_name,
                complex_code=court.complex_code,
                complex_name=court.complex_name,
                court_numbers=court.court_numbers,
                from_date=format_date_for_api(from_date),
                to_date=format_date_for_api(to_date),
                order_type="both",  # "interim", "finalorder", or "both"
            )


class Downloader:
    """Downloads court orders from eCourts website"""

    def __init__(
        self,
        task: DistrictCourtTask,
        archive_manager: S3ArchiveManager,
        compress_pdfs: bool = True,
    ):
        self.task = task
        self.archive_manager = archive_manager
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.app_token = None
        self.session_cookie = None
        # PDF compression (enabled by default if Ghostscript is available)
        self.compress_pdfs = compress_pdfs and COMPRESSION_AVAILABLE

    def _extract_app_token(self, html: str) -> Optional[str]:
        """Extract app_token from HTML content"""
        soup = BeautifulSoup(html, "lxml")
        token_input = soup.find("input", {"name": "app_token"})
        if token_input:
            return token_input.get("value", "")

        pattern = r"app_token['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]"
        match = re.search(pattern, html)
        if match:
            return match.group(1)

        pattern2 = r"app_token=([^&'\"]+)"
        match = re.search(pattern2, html)
        if match:
            return match.group(1)

        return None

    def _update_token(self, response_json: dict):
        """Update app_token from API response"""
        if "app_token" in response_json:
            self.app_token = response_json["app_token"]
            logger.debug(f"Updated app_token: {self.app_token[:20]}...")

    def init_session(self):
        """Initialize session and get app_token"""
        logger.debug(f"Initializing session for task: {self.task}")

        # Add small random delay to avoid rate limiting
        time.sleep(random.uniform(0.5, 1.5))

        # Get the court orders page with retry
        url = f"{BASE_URL}?p=courtorder/index"
        response = self._fetch_with_retry("GET", url, timeout=30, verify=False)

        # Handle rate limiting (405 Security Page)
        if response.status_code == 405:
            logger.warning("Rate limited (405). Waiting 30s before retry...")
            time.sleep(30)
            response = self._fetch_with_retry("GET", url, timeout=30, verify=False)

        response.raise_for_status()

        # Extract app_token
        self.app_token = self._extract_app_token(response.text)
        if not self.app_token:
            token_match = re.search(r"app_token=([^&'\"]+)", response.url)
            if token_match:
                self.app_token = token_match.group(1)

        if not self.app_token:
            raise ValueError("Could not extract app_token from page")

        # Get session cookies
        self.session_cookie = response.cookies.get("SERVICES_SESSID")
        if not self.session_cookie:
            self.session_cookie = response.cookies.get("PHPSESSID")

        logger.debug(f"Got app_token: {self.app_token[:20]}...")

    def set_court_data(self):
        """Set the court complex in the session"""
        url = f"{BASE_URL}?p=casestatus/set_data"

        # Format: complex_code@court_numbers@flag
        complex_code_full = f"{self.task.complex_code}@{self.task.court_numbers}@N"

        data = {
            "complex_code": complex_code_full,
            "selected_state_code": self.task.state_code,
            "selected_dist_code": self.task.district_code,
            "selected_est_code": "null",
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        response = self._fetch_with_retry("POST", url, data=data, timeout=30, verify=False)
        response.raise_for_status()

        result = response.json()
        self._update_token(result)

        if result.get("status") != 1:
            logger.warning(f"Failed to set court data: {result}")
            return False

        return True

    def solve_captcha(self, retries: int = 0) -> str:
        """Solve CAPTCHA using ONNX model"""
        if retries > 10:
            raise ValueError("Failed to solve CAPTCHA after 10 attempts")

        # Get captcha image with retry
        captcha_url = f"{BASE_URL}vendor/securimage/securimage_show.php?{uuid.uuid4().hex}"
        response = self._fetch_with_retry("GET", captcha_url, timeout=30, verify=False)

        # Save and process
        unique_id = uuid.uuid4().hex[:8]
        captcha_path = captcha_tmp_dir / f"captcha_dc_{unique_id}.png"
        with open(captcha_path, "wb") as f:
            f.write(response.content)

        try:
            img = Image.open(captcha_path)
            captcha_text = get_text(img).strip()

            # eCourts captcha is 6 characters
            if len(captcha_text) != 6:
                logger.debug(f"Invalid captcha length: {captcha_text}")
                captcha_path.unlink()
                return self.solve_captcha(retries + 1)

            captcha_path.unlink()
            return captcha_text

        except Exception as e:
            logger.error(f"Error solving captcha: {e}")
            # Move to failures dir for debugging (if file exists)
            if captcha_path.exists():
                new_path = captcha_failures_dir / f"{uuid.uuid4().hex[:8]}_{captcha_path.name}"
                try:
                    captcha_path.rename(new_path)
                except Exception:
                    pass  # Ignore if file was already moved/deleted
            return self.solve_captcha(retries + 1)

    def search_orders(self) -> Optional[str]:
        """Search for orders by date range"""
        url = f"{BASE_URL}?p=courtorder/submitOrderDate"

        # Solve captcha
        captcha_code = self.solve_captcha()

        data = {
            "state_code": self.task.state_code,
            "dist_code": self.task.district_code,
            "court_complex": self.task.complex_code,
            "court_complex_arr": self.task.court_numbers,
            "est_code": "",
            "from_date": self.task.from_date,
            "to_date": self.task.to_date,
            "fradorderdt": self.task.order_type,  # "interim", "finalorder", or "both"
            "orderflagvaldate": self.task.order_type,  # "interim", "finalorder", or "both"
            "order_date_captcha_code": captcha_code,  # Correct field name for captcha
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry("POST", url, data=data, timeout=60, verify=False)
            response.raise_for_status()

            # Check if response is JSON with token update
            try:
                result = response.json()
                self._update_token(result)

                # Check for captcha error
                if result.get("errormsg"):
                    logger.warning(f"Search error: {result.get('errormsg')}")
                    if "captcha" in result.get("errormsg", "").lower():
                        return self.search_orders()  # Retry with new captcha
                    return None

                # Check status
                if result.get("status") != 1:
                    logger.debug(f"Search returned non-success status: {result}")
                    return None

                # Check for HTML content in response (court_dt_data field)
                if "court_dt_data" in result:
                    return result["court_dt_data"]
                if "html" in result:
                    return result["html"]

            except json.JSONDecodeError:
                # Response is HTML
                return response.text

            return response.text

        except (ConnectionError, Timeout, ChunkedEncodingError,
                urllib3.exceptions.ProtocolError) as e:
            logger.error(f"Network error searching orders (after retries): {e}")
            return None
        except Exception as e:
            logger.error(f"Error searching orders: {e}")
            return None

    def parse_order_results(self, html: str) -> List[dict]:
        """Parse order search results from HTML"""
        soup = BeautifulSoup(html, "lxml")
        results = []

        # Look for the results table
        table = soup.find("table", {"id": "caseList"})
        if not table:
            tables = soup.find_all("table")
            for t in tables:
                rows = t.find_all("tr")
                if len(rows) > 1:  # Has data rows
                    table = t
                    break

        if not table:
            return results

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue

            order_data = {
                "raw_html": str(row),
            }

            # Extract data from cells
            for idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                if text:
                    order_data[f"cell_{idx}"] = text

                # Look for links/buttons with PDF info
                link = cell.find("a")
                if link:
                    href = link.get("href", "")
                    onclick = link.get("onclick", "")
                    if href:
                        order_data["pdf_href"] = href
                    if onclick:
                        order_data["onclick"] = onclick

                button = cell.find("button")
                if button:
                    onclick = button.get("onclick", "")
                    if onclick:
                        order_data["onclick"] = onclick

            # Try to extract CNR (16-char format)
            cnr_match = re.search(r"\b([A-Z]{4}\d{12})\b", str(row))
            if cnr_match:
                order_data["cnr"] = cnr_match.group(1)
            # If no CNR, try to use case number as unique identifier
            elif order_data.get("cell_1"):
                # Sanitize case number for filename: MVOP/63/2021 -> MVOP_63_2021
                case_no = order_data["cell_1"]
                case_id = re.sub(r"[^\w\d.-]", "_", case_no)
                order_data["cnr"] = case_id

            if order_data.get("onclick") or order_data.get("pdf_href"):
                results.append(order_data)

        return results

    def _fetch_with_retry(self, method: str, url: str, max_retries: int = 3, **kwargs) -> requests.Response:
        """Fetch URL with retry logic for network errors"""
        last_exception = None
        for attempt in range(max_retries + 1):
            try:
                if method == "GET":
                    return self.session.get(url, **kwargs)
                else:
                    return self.session.post(url, **kwargs)
            except (ConnectionError, Timeout, ChunkedEncodingError,
                    urllib3.exceptions.ProtocolError,
                    requests.exceptions.RequestException) as e:
                last_exception = e
                if attempt < max_retries:
                    delay = min(1.0 * (2 ** attempt) + random.uniform(0, 1), 30.0)
                    logger.warning(
                        f"Network error (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    raise
        raise last_exception

    def download_pdf(self, order_data: dict) -> Optional[bytes]:
        """Download PDF for an order using displayPdf parameters"""
        onclick = order_data.get("onclick", "")

        # Extract displayPdf parameters: displayPdf('normal_v','case_val','court_code','filename','appFlag')
        pattern = r"displayPdf\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']*)'\s*\)"
        match = re.search(pattern, onclick)

        if not match:
            logger.debug(f"Could not extract displayPdf parameters from: {onclick[:100]}")
            return None

        normal_v, case_val, court_code, filename, app_flag = match.groups()

        # Call the display_pdf endpoint to get PDF URL
        url = f"{BASE_URL}?p=home/display_pdf"
        data = {
            "normal_v": normal_v,
            "case_val": case_val,
            "court_code": court_code,
            "filename": filename,
            "appFlag": app_flag,
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry("POST", url, data=data, timeout=60, verify=False)
            response.raise_for_status()

            result = response.json()
            self._update_token(result)

            # Get PDF URL from response (presence of 'order' indicates success)
            pdf_path = result.get("order", "")
            if not pdf_path:
                logger.debug(f"No PDF path in response: {result}")
                return None

            # Make URL absolute
            if not pdf_path.startswith("http"):
                pdf_url = f"{BASE_URL}{pdf_path.lstrip('/')}"
            else:
                pdf_url = pdf_path

            # Download the actual PDF with retry
            pdf_response = self._fetch_with_retry("GET", pdf_url, timeout=120, verify=False)
            if pdf_response.status_code == 200 and len(pdf_response.content) > 100:
                # Verify it's a PDF
                if pdf_response.content[:4] == b'%PDF':
                    return pdf_response.content
                else:
                    logger.debug(f"Response is not a PDF: {pdf_response.content[:50]}")
                    return None

        except json.JSONDecodeError:
            logger.debug(f"Non-JSON response from display_pdf: {response.text[:100]}")
        except Exception as e:
            logger.error(f"Error downloading PDF: {e}")

        return None

    def _compress_pdf_bytes(self, pdf_content: bytes) -> bytes:
        """
        Compress PDF content (bytes) using Ghostscript.
        Returns compressed bytes if successful, original bytes otherwise.
        """
        import tempfile

        try:
            # Write to temp file
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_in:
                tmp_in.write(pdf_content)
                tmp_in_path = Path(tmp_in.name)

            original_size = len(pdf_content)

            # Compress the PDF
            compressed_path = compress_pdf_if_enabled(tmp_in_path, COMPRESSION_AVAILABLE)

            # Read the result
            with open(compressed_path, "rb") as f:
                result_content = f.read()

            compressed_size = len(result_content)

            # Clean up temp file
            if tmp_in_path.exists():
                tmp_in_path.unlink()

            # Log compression result
            if compressed_size < original_size:
                reduction = (1 - compressed_size / original_size) * 100
                logger.debug(
                    f"Compressed PDF: {original_size} -> {compressed_size} bytes ({reduction:.1f}% reduction)"
                )

            return result_content

        except Exception as e:
            logger.debug(f"PDF compression failed: {e}")
            return pdf_content

    def process_order(self, order_data: dict) -> bool:
        """Process a single order - download PDF and save metadata"""
        cnr = order_data.get("cnr", "")
        if not cnr:
            # Generate a unique ID from the raw HTML
            cnr = f"UNKNOWN_{uuid.uuid4().hex[:12]}"

        # Extract year from order date or use task date
        try:
            # Try to parse order date from cells
            order_date_str = order_data.get("cell_3", "") or order_data.get("cell_2", "")
            if order_date_str:
                order_date = parse_date_from_api(order_date_str)
                year = order_date.year
            else:
                # Fall back to task from_date
                year = parse_date_from_api(self.task.from_date).year
        except (ValueError, KeyError, AttributeError):
            year = datetime.now().year

        # Check if metadata already exists
        metadata_filename = f"{cnr}.json"
        if not self.archive_manager.file_exists(
            year,
            self.task.state_code,
            self.task.district_code,
            self.task.complex_code,
            "metadata",
            metadata_filename,
        ):
            # Save metadata
            metadata = {
                "cnr": cnr,
                "state_code": self.task.state_code,
                "state_name": self.task.state_name,
                "district_code": self.task.district_code,
                "district_name": self.task.district_name,
                "complex_code": self.task.complex_code,
                "complex_name": self.task.complex_name,
                "raw_html": order_data.get("raw_html", ""),
                "scraped_at": datetime.now(IST).isoformat(),
            }

            # Add cell data
            for key, value in order_data.items():
                if key.startswith("cell_"):
                    metadata[key] = value

            self.archive_manager.add_to_archive(
                year,
                self.task.state_code,
                self.task.district_code,
                self.task.complex_code,
                "metadata",
                metadata_filename,
                json.dumps(metadata, indent=2, ensure_ascii=False),
            )

        # Check if PDF already exists
        pdf_filename = f"{cnr}.pdf"
        if not self.archive_manager.file_exists(
            year,
            self.task.state_code,
            self.task.district_code,
            self.task.complex_code,
            "orders",
            pdf_filename,
        ):
            # Download PDF
            pdf_content = self.download_pdf(order_data)
            if pdf_content:
                # Compress PDF if enabled
                if self.compress_pdfs:
                    pdf_content = self._compress_pdf_bytes(pdf_content)

                self.archive_manager.add_to_archive(
                    year,
                    self.task.state_code,
                    self.task.district_code,
                    self.task.complex_code,
                    "orders",
                    pdf_filename,
                    pdf_content,
                )
                return True

        return False

    def download(self):
        """Process the task - search and download orders"""
        # Check if task already completed (BEFORE hitting the server)
        if is_task_completed(self.task):
            logger.debug(f"Skipping already completed task: {self.task}")
            return

        try:
            self.init_session()

            # Set court data
            if not self.set_court_data():
                logger.error(f"Failed to set court data for task: {self.task}")
                return

            # Search for orders
            html = self.search_orders()
            if not html:
                logger.debug(f"No results for task: {self.task}")
                # Mark as completed even if no results (so we don't retry)
                save_completed_task(get_task_key(self.task))
                return

            # Parse results
            orders = self.parse_order_results(html)
            if not orders:
                logger.debug(f"No orders found for task: {self.task}")
                # Mark as completed even if no orders
                save_completed_task(get_task_key(self.task))
                return

            logger.info(f"Found {len(orders)} orders for task: {self.task}")

            # Process each order with progress bar
            downloaded = 0
            pbar = tqdm(orders, desc=f"PDFs ({self.task.complex_name[:20]})", leave=False, unit="pdf")
            for order in pbar:
                if self.process_order(order):
                    downloaded += 1
                    pbar.set_postfix({"new": downloaded})

            logger.info(f"Downloaded {downloaded} new PDFs out of {len(orders)} orders for task: {self.task}")

            # Mark task as completed
            save_completed_task(get_task_key(self.task))

        except Exception as e:
            logger.error(f"Error processing task {self.task}: {e}")
            traceback.print_exc()


def process_task(
    task: DistrictCourtTask,
    archive_manager: S3ArchiveManager,
    compress_pdfs: bool = True,
):
    """Process a single task"""
    try:
        downloader = Downloader(task, archive_manager, compress_pdfs=compress_pdfs)
        downloader.download()
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        traceback.print_exc()


def run(
    courts: List[CourtComplex],
    start_date: str,
    end_date: str,
    day_step: int = 1,
    max_workers: int = 5,
    archive_manager: Optional[S3ArchiveManager] = None,
    compress_pdfs: bool = True,
):
    """Run the downloader for all courts and date ranges"""
    # Create archive manager if not provided
    if archive_manager is None:
        archive_manager = S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            local_only=True,
        )

    # Generate tasks
    tasks = list(generate_tasks(courts, start_date, end_date, day_step))
    logger.info(f"Generated {len(tasks)} tasks")

    # Process tasks
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_task, task, archive_manager, compress_pdfs)
            for task in tasks
        ]

        for i, future in enumerate(
            tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Processing tasks",
            )
        ):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Task failed: {e}")

    logger.info("All tasks completed")


def main():
    parser = argparse.ArgumentParser(
        description="Download Indian District Court Judgments"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--day_step",
        type=int,
        default=2100, # Large default to minimize chunks & districts has no pagination & ~5yrs data
        help="Number of days per chunk",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=2,
        help="Number of parallel workers (default: 2 to avoid rate limiting)",
    )
    parser.add_argument(
        "--state_code",
        type=str,
        default=None,
        help="Filter by state code",
    )
    parser.add_argument(
        "--district_code",
        type=str,
        default=None,
        help="Filter by district code",
    )
    parser.add_argument(
        "--complex_code",
        type=str,
        default=None,
        help="Filter by complex code",
    )
    parser.add_argument(
        "--courts_csv",
        type=str,
        default="courts.csv",
        help="Path to courts.csv file",
    )
    parser.add_argument(
        "--sync-s3",
        action="store_true",
        default=False,
        help="Sync mode (incremental updates)",
    )
    parser.add_argument(
        "--sync-s3-fill",
        action="store_true",
        default=False,
        help="Gap-filling mode (historical backfill)",
    )
    parser.add_argument(
        "--timeout-hours",
        type=float,
        default=5.5,
        help="Maximum hours to run before graceful exit",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        default=False,
        help="Disable PDF compression (compression is enabled by default)",
    )
    args = parser.parse_args()

    # Handle PDF compression settings
    compress_pdfs = not args.no_compress
    if compress_pdfs:
        if COMPRESSION_AVAILABLE:
            logger.info("PDF compression enabled (using Ghostscript)")
        else:
            logger.warning("PDF compression requested but Ghostscript not available - compression disabled")
            compress_pdfs = False
    else:
        logger.info("PDF compression disabled (--no-compress flag)")

    # Load courts
    courts_path = Path(args.courts_csv)
    if not courts_path.exists():
        logger.error(f"Courts file not found: {courts_path}")
        logger.info("Run 'python scrape_courts.py' first to generate courts.csv")
        sys.exit(1)

    courts = load_courts_csv(courts_path)
    logger.info(f"Loaded {len(courts)} court complexes")

    # Apply filters
    if args.state_code:
        courts = [c for c in courts if c.state_code == args.state_code]
    if args.district_code:
        courts = [c for c in courts if c.district_code == args.district_code]
    if args.complex_code:
        courts = [c for c in courts if c.complex_code == args.complex_code]

    if not courts:
        logger.error("No courts match the specified filters")
        sys.exit(1)

    logger.info(f"Processing {len(courts)} court complexes")

    if args.sync_s3_fill:
        from sync_s3_fill import sync_s3_fill_gaps

        sync_s3_fill_gaps(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            courts=courts,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            timeout_hours=args.timeout_hours,
            compress_pdfs=compress_pdfs,
        )
    elif args.sync_s3:
        from sync_s3 import run_sync_s3

        run_sync_s3(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            courts=courts,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            compress_pdfs=compress_pdfs,
        )
    else:
        # Default: local download mode
        if not args.start_date:
            logger.error("--start_date is required for local download mode")
            sys.exit(1)

        end_date = args.end_date or datetime.now().strftime("%Y-%m-%d")

        with S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            local_only=True,
        ) as archive_manager:
            run(
                courts=courts,
                start_date=args.start_date,
                end_date=end_date,
                day_step=args.day_step,
                max_workers=args.max_workers,
                archive_manager=archive_manager,
                compress_pdfs=compress_pdfs,
            )


if __name__ == "__main__":
    main()
