"""
eCourts Mobile API Scraper.

Bulk data retrieval from the eCourts mobile API.

NOTE: PDF downloads are not currently supported via the mobile API.
The PDF endpoint requires a server-generated Bearer token that is tied
to the mobile app's session and cannot be forged. Use the web scraper
(download.py) for PDF downloads, which handles session management
and CAPTCHA solving.

This scraper is useful for:
- Collecting case metadata without CAPTCHA
- Getting case types and court hierarchies
- Searching cases by type/year
- Getting case history with all metadata
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from api_client import MobileAPIClient, State, District, CourtComplex, CaseType, Case, Order


# Default output directory
DEFAULT_OUTPUT_DIR = "./mobile_data"


class MobileScraper:
    """Scraper for eCourts Mobile API."""

    def __init__(
        self,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        delay: float = 0.5,
        max_retries: int = 3,
        download_pdfs: bool = False  # Disabled by default - mobile API PDF auth not working
    ):
        self.client = MobileAPIClient()
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.max_retries = max_retries
        self.download_pdfs = download_pdfs
        self.stats = {
            "states_processed": 0,
            "districts_processed": 0,
            "complexes_processed": 0,
            "cases_found": 0,
            "cases_downloaded": 0,
            "pdfs_downloaded": 0,
            "pdfs_failed": 0,
            "errors": 0,
        }

        if download_pdfs:
            print("WARNING: PDF downloads via mobile API are experimental and may not work.")
            print("         The PDF endpoint requires server-generated session tokens.")
            print("         Use the web scraper (download.py) for reliable PDF downloads.")

    def _ensure_dir(self, path: Path) -> None:
        """Ensure directory exists."""
        path.mkdir(parents=True, exist_ok=True)

    def _save_json(self, path: Path, data: dict) -> None:
        """Save data as JSON."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _case_exists(self, state_code: int, dist_code: int, complex_code: str, case_no: str) -> bool:
        """Check if case data already exists."""
        case_dir = self.output_dir / f"state={state_code}" / f"district={dist_code}" / f"complex={complex_code}"
        case_file = case_dir / f"{case_no}.json"
        return case_file.exists()

    def scrape_states(self) -> list[State]:
        """Get all states."""
        print("Fetching states...")
        states = self.client.get_states()
        print(f"  Found {len(states)} states")
        return states

    def scrape_districts(self, state_code: int) -> list[District]:
        """Get districts for a state."""
        time.sleep(self.delay)
        districts = self.client.get_districts(state_code)
        return districts

    def scrape_court_complexes(self, state_code: int, dist_code: int) -> list[CourtComplex]:
        """Get court complexes for a district."""
        time.sleep(self.delay)
        complexes = self.client.get_court_complexes(state_code, dist_code)
        return complexes

    def scrape_case_types(self, state_code: int, dist_code: int, court_code: str) -> list[CaseType]:
        """Get case types for a court."""
        time.sleep(self.delay)
        return self.client.get_case_types(state_code, dist_code, court_code)

    def scrape_cases(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_type: int,
        year: int,
        pending_disposed: str = "Both"
    ) -> list[Case]:
        """Search for cases."""
        time.sleep(self.delay)
        return self.client.search_cases_by_type(
            state_code=state_code,
            dist_code=dist_code,
            court_code=court_code,
            case_type=case_type,
            year=year,
            pending_disposed=pending_disposed
        )

    def scrape_case_history(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_no: str
    ) -> Optional[dict]:
        """Get case history."""
        time.sleep(self.delay)
        return self.client.get_case_history(
            state_code=state_code,
            dist_code=dist_code,
            court_code=court_code,
            case_no=case_no
        )

    def scrape_complex(
        self,
        state: State,
        district: District,
        complex_: CourtComplex,
        years: list[int],
        pending_disposed: str = "Both",
        skip_existing: bool = True
    ) -> int:
        """
        Scrape all cases from a court complex.

        Returns:
            Number of cases downloaded
        """
        downloaded = 0
        case_dir = (
            self.output_dir
            / f"state={state.code}"
            / f"district={district.code}"
            / f"complex={complex_.code}"
        )
        self._ensure_dir(case_dir)

        # Get case types
        case_types = self.scrape_case_types(state.code, district.code, complex_.njdg_est_code)
        if not case_types:
            return 0

        print(f"    Found {len(case_types)} case types")

        for ct in case_types:
            for year in years:
                # Search cases
                cases = self.scrape_cases(
                    state_code=state.code,
                    dist_code=district.code,
                    court_code=complex_.njdg_est_code,
                    case_type=ct.code,
                    year=year,
                    pending_disposed=pending_disposed
                )

                if not cases:
                    continue

                print(f"      Type {ct.code} ({ct.name[:20]}), Year {year}: {len(cases)} cases")
                self.stats["cases_found"] += len(cases)

                for case in cases:
                    # Check if already downloaded
                    case_file = case_dir / f"{case.case_no}.json"
                    if skip_existing and case_file.exists():
                        continue

                    # Get case history
                    history = self.scrape_case_history(
                        state_code=state.code,
                        dist_code=district.code,
                        court_code=complex_.njdg_est_code,
                        case_no=case.case_no
                    )

                    if history:
                        # Extract orders with PDF URLs
                        final_orders, interim_orders = self.client.get_orders_from_history(history)

                        # Convert orders to dicts for JSON serialization
                        final_orders_data = [
                            {
                                "order_number": o.order_number,
                                "order_date": o.order_date,
                                "order_type": o.order_type,
                                "pdf_url": o.pdf_url,
                                "is_final": o.is_final,
                            }
                            for o in final_orders
                        ]
                        interim_orders_data = [
                            {
                                "order_number": o.order_number,
                                "order_date": o.order_date,
                                "order_type": o.order_type,
                                "pdf_url": o.pdf_url,
                                "is_final": o.is_final,
                            }
                            for o in interim_orders
                        ]

                        # Add metadata
                        case_data = {
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
                                "final_orders": final_orders_data,
                                "interim_orders": interim_orders_data,
                                "has_pdf": bool(final_orders_data or interim_orders_data),
                            },
                            "history": history,
                            "scraped_at": datetime.now().isoformat(),
                        }

                        self._save_json(case_file, case_data)
                        downloaded += 1
                        self.stats["cases_downloaded"] += 1

                        # Track orders with PDFs
                        if final_orders_data or interim_orders_data:
                            self.stats.setdefault("cases_with_orders", 0)
                            self.stats["cases_with_orders"] += 1

                        # Download PDFs if enabled
                        if self.download_pdfs:
                            pdf_dir = case_dir / "pdfs"
                            all_orders = final_orders + interim_orders

                            for order in all_orders:
                                if order.pdf_url:
                                    order_type = "final" if order.is_final else "interim"
                                    pdf_filename = f"{case.case_no}_{order_type}_{order.order_number}.pdf"
                                    pdf_path = pdf_dir / pdf_filename

                                    if not pdf_path.exists():
                                        self._ensure_dir(pdf_dir)
                                        time.sleep(self.delay)

                                        success = self.client.download_pdf(
                                            order.pdf_url,
                                            str(pdf_path)
                                        )

                                        if success:
                                            self.stats["pdfs_downloaded"] += 1
                                            print(f"        Downloaded PDF: {pdf_filename}")
                                        else:
                                            self.stats["pdfs_failed"] += 1

        return downloaded

    def scrape(
        self,
        state_codes: Optional[list[int]] = None,
        district_codes: Optional[list[int]] = None,
        complex_codes: Optional[list[str]] = None,
        years: Optional[list[int]] = None,
        pending_disposed: str = "Both",
        skip_existing: bool = True,
        limit: Optional[int] = None
    ) -> dict:
        """
        Main scraping function.

        Args:
            state_codes: Filter by state codes (default: all)
            district_codes: Filter by district codes (default: all)
            complex_codes: Filter by complex codes (default: all)
            years: Years to scrape (default: [2024, 2023, 2022])
            pending_disposed: "Pending", "Disposed", or "Both"
            skip_existing: Skip already downloaded cases
            limit: Maximum number of cases to download

        Returns:
            Statistics dictionary
        """
        if years is None:
            years = [2024, 2023, 2022]

        start_time = datetime.now()
        print(f"\nStarting scrape at {start_time.isoformat()}")
        print(f"Output directory: {self.output_dir}")
        print(f"Years: {years}")
        print(f"Filter: {pending_disposed}")
        print()

        # Get states
        states = self.scrape_states()
        if state_codes:
            states = [s for s in states if s.code in state_codes]
            print(f"Filtered to {len(states)} states")

        for state in states:
            print(f"\n[State] {state.name} ({state.code})")

            # Get districts
            districts = self.scrape_districts(state.code)
            if district_codes:
                districts = [d for d in districts if d.code in district_codes]

            print(f"  Found {len(districts)} districts")

            for district in districts:
                print(f"\n  [District] {district.name} ({district.code})")

                # Get court complexes
                complexes = self.scrape_court_complexes(state.code, district.code)
                if complex_codes:
                    complexes = [c for c in complexes if c.code in complex_codes]

                print(f"    Found {len(complexes)} court complexes")

                for complex_ in complexes:
                    print(f"\n    [Complex] {complex_.name} ({complex_.code})")

                    downloaded = self.scrape_complex(
                        state=state,
                        district=district,
                        complex_=complex_,
                        years=years,
                        pending_disposed=pending_disposed,
                        skip_existing=skip_existing
                    )

                    print(f"    Downloaded {downloaded} cases")
                    self.stats["complexes_processed"] += 1

                    # Check limit
                    if limit and self.stats["cases_downloaded"] >= limit:
                        print(f"\nReached limit of {limit} cases")
                        break

                self.stats["districts_processed"] += 1

                if limit and self.stats["cases_downloaded"] >= limit:
                    break

            self.stats["states_processed"] += 1

            if limit and self.stats["cases_downloaded"] >= limit:
                break

        end_time = datetime.now()
        duration = end_time - start_time

        self.stats["start_time"] = start_time.isoformat()
        self.stats["end_time"] = end_time.isoformat()
        self.stats["duration_seconds"] = duration.total_seconds()

        print("\n" + "=" * 60)
        print("Scraping Complete")
        print("=" * 60)
        print(f"Duration: {duration}")
        print(f"States processed: {self.stats['states_processed']}")
        print(f"Districts processed: {self.stats['districts_processed']}")
        print(f"Complexes processed: {self.stats['complexes_processed']}")
        print(f"Cases found: {self.stats['cases_found']}")
        print(f"Cases downloaded: {self.stats['cases_downloaded']}")
        print(f"PDFs downloaded: {self.stats['pdfs_downloaded']}")
        print(f"PDFs failed: {self.stats['pdfs_failed']}")
        print(f"Errors: {self.stats['errors']}")

        return self.stats


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(description="eCourts Mobile API Scraper")

    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--state",
        type=int,
        action="append",
        dest="states",
        help="State code to scrape (can be specified multiple times)"
    )
    parser.add_argument(
        "--district",
        type=int,
        action="append",
        dest="districts",
        help="District code to scrape (can be specified multiple times)"
    )
    parser.add_argument(
        "--complex",
        type=str,
        action="append",
        dest="complexes",
        help="Complex code to scrape (can be specified multiple times)"
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        dest="years",
        help="Year to scrape (can be specified multiple times, default: 2024, 2023, 2022)"
    )
    parser.add_argument(
        "--filter",
        choices=["Pending", "Disposed", "Both"],
        default="Both",
        help="Case status filter (default: Both)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between API calls in seconds (default: 0.5)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of cases to download"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip existing cases"
    )
    parser.add_argument(
        "--download-pdfs",
        action="store_true",
        help="Try to download PDF files (experimental - may not work due to auth)"
    )

    args = parser.parse_args()

    scraper = MobileScraper(
        output_dir=args.output_dir,
        delay=args.delay,
        download_pdfs=args.download_pdfs  # Disabled by default
    )

    stats = scraper.scrape(
        state_codes=args.states,
        district_codes=args.districts,
        complex_codes=args.complexes,
        years=args.years,
        pending_disposed=args.filter,
        skip_existing=not args.no_skip,
        limit=args.limit
    )

    # Save stats
    stats_file = Path(args.output_dir) / "scrape_stats.json"
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nStats saved to {stats_file}")


if __name__ == "__main__":
    main()
