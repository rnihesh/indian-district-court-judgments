"""
Metadata Processor for Indian District Court Judgments
Converts raw metadata JSON files to Parquet format for analytics

Usage:
    python process_metadata.py
    python process_metadata.py --year 2025 --state 24
"""

import argparse
import json
import logging
import re
import sys
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import colorlog
import pandas as pd
from botocore import UNSIGNED
from botocore.client import Config

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


class DistrictCourtMetadataProcessor:
    """Processes metadata from S3 and generates Parquet files"""

    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str = "",
        batch_size: int = 10000,
        years_to_process: Optional[List[str]] = None,
        states_to_process: Optional[List[str]] = None,
    ):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.batch_size = batch_size
        self.years_to_process = years_to_process
        self.states_to_process = states_to_process
        self.s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def list_metadata_tars(self) -> List[Dict]:
        """List all metadata TAR files in S3"""
        tars = []
        prefix = f"{self.s3_prefix}metadata/tar/"

        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                if not key.endswith(".tar"):
                    continue

                # Parse path: metadata/tar/year=YYYY/state=XX/district=YY/complex=ZZ/metadata.tar
                match = re.search(
                    r"year=(\d{4})/state=(\w+)/district=(\w+)/complex=(\w+)/",
                    key,
                )
                if match:
                    year = match.group(1)
                    state = match.group(2)
                    district = match.group(3)
                    complex_code = match.group(4)

                    # Apply filters
                    if self.years_to_process and year not in self.years_to_process:
                        continue
                    if self.states_to_process and state not in self.states_to_process:
                        continue

                    tars.append({
                        "key": key,
                        "year": year,
                        "state": state,
                        "district": district,
                        "complex": complex_code,
                    })

        return tars

    @staticmethod
    def extract_date_from_html(html: str) -> Optional[str]:
        """Extract order date from raw HTML"""
        # Try various date patterns
        patterns = [
            r"Order Date\s*:\s*(\d{2}-\d{2}-\d{4})",
            r"Decision Date\s*:\s*(\d{2}-\d{2}-\d{4})",
            r"Date\s*:\s*(\d{2}-\d{2}-\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    @staticmethod
    def extract_case_type(html: str) -> Optional[str]:
        """Extract case type from raw HTML"""
        patterns = [
            r"Case Type\s*:\s*([^<\n]+)",
            r"<td[^>]*>([A-Z]+\s*/\s*\d+/\d+)</td>",
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    @staticmethod
    def extract_petitioner(html: str) -> Optional[str]:
        """Extract petitioner name from raw HTML"""
        patterns = [
            r"Petitioner\s*:\s*([^<\n]+)",
            r"Appellant\s*:\s*([^<\n]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    @staticmethod
    def extract_respondent(html: str) -> Optional[str]:
        """Extract respondent name from raw HTML"""
        patterns = [
            r"Respondent\s*:\s*([^<\n]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    @staticmethod
    def process_metadata_static(metadata: Dict, year: str) -> Optional[Dict]:
        """Process a single metadata record"""
        try:
            raw_html = metadata.get("raw_html", "")

            processed = {
                "cnr": metadata.get("cnr", ""),
                "state_code": metadata.get("state_code", ""),
                "state_name": metadata.get("state_name", ""),
                "district_code": metadata.get("district_code", ""),
                "district_name": metadata.get("district_name", ""),
                "complex_code": metadata.get("complex_code", ""),
                "complex_name": metadata.get("complex_name", ""),
                "year": int(year),
                "scraped_at": metadata.get("scraped_at", ""),
            }

            # Extract additional fields from HTML
            processed["order_date"] = DistrictCourtMetadataProcessor.extract_date_from_html(
                raw_html
            )
            processed["case_type"] = DistrictCourtMetadataProcessor.extract_case_type(
                raw_html
            )
            processed["petitioner"] = DistrictCourtMetadataProcessor.extract_petitioner(
                raw_html
            )
            processed["respondent"] = DistrictCourtMetadataProcessor.extract_respondent(
                raw_html
            )

            # Add cell data if available
            for i in range(10):
                cell_key = f"cell_{i}"
                if cell_key in metadata:
                    processed[cell_key] = metadata[cell_key]

            return processed

        except Exception as e:
            logger.debug(f"Error processing metadata: {e}")
            return None

    def process_tar_file(self, tar_info: Dict) -> List[Dict]:
        """Process a single TAR file and return records"""
        records = []

        with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            # Download TAR file
            self.s3.download_file(self.s3_bucket, tar_info["key"], str(tmp_path))

            # Process TAR contents
            with tarfile.open(tmp_path, "r") as tf:
                for member in tf.getmembers():
                    if not member.name.endswith(".json"):
                        continue

                    f = tf.extractfile(member)
                    if not f:
                        continue

                    try:
                        metadata = json.load(f)
                        processed = self.process_metadata_static(
                            metadata, tar_info["year"]
                        )
                        if processed:
                            records.append(processed)
                    except json.JSONDecodeError:
                        logger.debug(f"Invalid JSON in {member.name}")
                    except Exception as e:
                        logger.debug(f"Error processing {member.name}: {e}")

        except Exception as e:
            logger.error(f"Error processing TAR {tar_info['key']}: {e}")
        finally:
            tmp_path.unlink(missing_ok=True)

        return records

    def process_bucket_metadata(self) -> tuple[List[str], int]:
        """Process all metadata in the bucket and generate Parquet files"""
        # List all metadata TARs
        tars = self.list_metadata_tars()
        logger.info(f"Found {len(tars)} metadata TAR files to process")

        if not tars:
            return [], 0

        # Group by year/state for Parquet output
        records_by_key = {}
        total_records = 0

        for tar_info in tars:
            records = self.process_tar_file(tar_info)
            if records:
                key = (tar_info["year"], tar_info["state"])
                if key not in records_by_key:
                    records_by_key[key] = []
                records_by_key[key].extend(records)
                total_records += len(records)
                logger.info(
                    f"Processed {len(records)} records from {tar_info['year']}/{tar_info['state']}/{tar_info['district']}/{tar_info['complex']}"
                )

        # Generate Parquet files
        processed_years = set()
        s3_write = boto3.client("s3")

        for (year, state), records in records_by_key.items():
            if not records:
                continue

            df = pd.DataFrame(records)

            # Remove duplicates by CNR
            if "cnr" in df.columns:
                df = df.drop_duplicates(subset=["cnr"], keep="last")

            # Output path
            parquet_key = f"metadata/parquet/year={year}/state={state}/metadata.parquet"

            # Write to temp file and upload
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            try:
                df.to_parquet(tmp_path, compression="snappy", index=False)
                s3_write.upload_file(str(tmp_path), self.s3_bucket, parquet_key)
                logger.info(f"Uploaded {len(df)} records to {parquet_key}")
                processed_years.add(year)
            finally:
                tmp_path.unlink(missing_ok=True)

        return list(processed_years), total_records


def main():
    parser = argparse.ArgumentParser(
        description="Process District Court metadata to Parquet"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="indian-district-court-judgments",
        help="S3 bucket name",
    )
    parser.add_argument(
        "--year",
        type=str,
        default=None,
        help="Filter by year",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Filter by state code",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for processing",
    )
    args = parser.parse_args()

    years = [args.year] if args.year else None
    states = [args.state] if args.state else None

    processor = DistrictCourtMetadataProcessor(
        s3_bucket=args.bucket,
        years_to_process=years,
        states_to_process=states,
        batch_size=args.batch_size,
    )

    processed_years, total_records = processor.process_bucket_metadata()

    if total_records > 0:
        logger.info(
            f"Successfully processed {total_records} records across {len(processed_years)} years"
        )
    else:
        logger.warning("No metadata records were processed")


if __name__ == "__main__":
    main()
