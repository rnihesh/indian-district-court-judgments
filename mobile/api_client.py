"""
eCourts Mobile API Client.

Provides access to the mobile app API for fetching court data.
"""

import json
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from pathlib import Path

import requests

from crypto import encrypt_data_cbc, decrypt_response_cbc, decrypt_url_param
from urllib.parse import urlparse, parse_qs, unquote


# API Configuration
BASE_URL = "https://app.ecourts.gov.in/ecourt_mobile_DC"
PACKAGE_NAME = "gov.ecourts.eCourtsServices"

# Request headers
DEFAULT_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "eCourtsServices/2.0.1 (iPhone; iOS 26.2; Scale/3.00)",
    "Accept-Language": "en-IN;q=1",
    "Accept-Encoding": "gzip, deflate, br",
}


@dataclass
class State:
    """State data."""
    code: int
    name: str
    bilingual: bool = False
    hindi_name: str = ""
    national_code: str = ""


@dataclass
class District:
    """District data."""
    code: int
    name: str
    state_code: int


@dataclass
class CourtComplex:
    """Court complex data."""
    code: str
    name: str
    njdg_est_code: str
    state_code: int
    district_code: int


@dataclass
class CaseType:
    """Case type data."""
    code: int
    name: str
    local_name: str = ""


@dataclass
class Case:
    """Case summary data."""
    case_no: str
    cino: str
    case_type: str
    case_number: str
    reg_year: str
    petitioner: str
    court_code: str


@dataclass
class Order:
    """Order/judgment data with PDF link."""
    order_number: int
    order_date: str
    order_type: str  # "Order", "Judgement", etc.
    pdf_url: Optional[str] = None
    is_final: bool = False  # True for final orders, False for interim


class MobileAPIClient:
    """Client for eCourts Mobile API."""

    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self.device_uuid = str(uuid.uuid4()).replace('-', '')[:16]
        self.jwt_token = ""
        self.jsession = f"JSESSION={random.randint(1000000, 99999999)}"
        self.session = requests.Session()

    def _get_uid(self) -> str:
        """Get device UID."""
        return f"{self.device_uuid}:{PACKAGE_NAME}"

    def _make_request(
        self,
        endpoint: str,
        params: dict,
        include_auth: bool = True,
        retry_count: int = 3
    ) -> Optional[Any]:
        """
        Make an encrypted request to the API.

        Args:
            endpoint: API endpoint
            params: Request parameters
            include_auth: Whether to include Authorization header
            retry_count: Number of retries on failure

        Returns:
            Decrypted response data or None on error
        """
        url = f"{self.base_url}/{endpoint}"

        # Add UID to params
        params_with_uid = {**params, "uid": self._get_uid()}

        # Encrypt params
        encrypted_params = encrypt_data_cbc(params_with_uid)

        # Build headers
        headers = {
            **DEFAULT_HEADERS,
            "Cookie": self.jsession,
        }

        if include_auth:
            encrypted_token = encrypt_data_cbc(self.jwt_token if self.jwt_token else "")
            headers["Authorization"] = f"Bearer {encrypted_token}"

        for attempt in range(retry_count):
            try:
                response = self.session.get(
                    url,
                    params={"params": encrypted_params},
                    headers=headers,
                    timeout=60
                )

                if response.status_code != 200:
                    continue

                text = response.text.strip()

                # Try to decrypt
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    try:
                        decrypted = decrypt_response_cbc(text)

                        # Store token if present
                        if isinstance(decrypted, dict) and decrypted.get("token"):
                            self.jwt_token = decrypted["token"]

                        # Check for error status
                        if isinstance(decrypted, dict) and decrypted.get("status") == "N":
                            msg = decrypted.get("Msg") or decrypted.get("msg")
                            if msg == "Not in session !":
                                # Session expired, retry without auth
                                self.jwt_token = ""
                                continue
                            return None

                        return decrypted
                    except Exception:
                        continue

                # Try JSON
                try:
                    return response.json()
                except json.JSONDecodeError:
                    continue

            except requests.RequestException:
                if attempt < retry_count - 1:
                    time.sleep(1)
                continue

        return None

    def get_states(self) -> list[State]:
        """Get list of all states."""
        result = self._make_request(
            "stateWebService.php",
            {"action_code": "getStates", "time": str(random.randint(1000000, 9999999))}
        )

        if not result or "states" not in result:
            return []

        return [
            State(
                code=s["state_code"],
                name=s["state_name"],
                bilingual=s.get("bilingual") == "Y",
                hindi_name=s.get("state_name_hindi", ""),
                national_code=s.get("nationalstate_code", ""),
            )
            for s in result["states"]
        ]

    def get_districts(self, state_code: int) -> list[District]:
        """Get districts for a state."""
        result = self._make_request(
            "districtWebService.php",
            {"state_code": str(state_code), "test_param": "1"}
        )

        if not result:
            return []

        # Key can be "district" or "districts"
        districts_data = result.get("districts") or result.get("district") or []

        return [
            District(
                code=d["dist_code"],
                name=d["dist_name"],
                state_code=state_code,
            )
            for d in districts_data
        ]

    def get_court_complexes(self, state_code: int, dist_code: int) -> list[CourtComplex]:
        """Get court complexes for a district."""
        result = self._make_request(
            "courtEstWebService.php",
            {
                "action_code": "fillCourtComplex",
                "state_code": str(state_code),
                "dist_code": str(dist_code),
            }
        )

        if not result or "courtComplex" not in result:
            return []

        complexes = []
        for c in result["courtComplex"]:
            # njdg_est_code can be comma-separated list, take first one
            njdg_code = str(c["njdg_est_code"]).split(",")[0].strip()
            complexes.append(CourtComplex(
                code=c["complex_code"],
                name=c["court_complex_name"],
                njdg_est_code=njdg_code,
                state_code=state_code,
                district_code=dist_code,
            ))
        return complexes

    def get_case_types(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        language: str = "english"
    ) -> list[CaseType]:
        """Get case types for a court."""
        result = self._make_request(
            "caseNumberWebService.php",
            {
                "state_code": str(state_code),
                "dist_code": str(dist_code),
                "court_code": court_code,
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )

        if not result:
            return []

        # Key can be "caseType" or "case_types"
        case_types_data = result.get("caseType") or result.get("case_types") or []

        case_types = []
        for ct in case_types_data:
            # Handle nested structure: {"case_type": "code~name#code~name#..."}
            if "case_type" in ct and isinstance(ct["case_type"], str):
                # Format: "69~ARBEP - Description#51~A.R.B.O.P - Description#..."
                entries = ct["case_type"].split("#")
                for entry in entries:
                    if "~" in entry:
                        code_part, name_part = entry.split("~", 1)
                        try:
                            code = int(code_part.strip())
                        except ValueError:
                            code = 0
                        case_types.append(CaseType(code=code, name=name_part.strip(), local_name=""))
            else:
                # Handle different key names
                code = ct.get("type_code") or ct.get("case_type_code") or ct.get("code", 0)
                name = ct.get("type_name") or ct.get("case_type_name") or ct.get("name", "")
                local_name = ct.get("ltype_name") or ct.get("local_name", "")
                case_types.append(CaseType(code=code, name=name, local_name=local_name))

        return case_types

    def search_cases_by_type(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_type: int,
        year: int,
        pending_disposed: str = "D",
        language: str = "english"
    ) -> list[Case]:
        """
        Search cases by type and year.

        Args:
            state_code: State code
            dist_code: District code
            court_code: Court code (from court complex)
            case_type: Case type code
            year: Registration year
            pending_disposed: "P" for pending, "D" for disposed
            language: Language flag

        Returns:
            List of matching cases
        """
        result = self._make_request(
            "searchByCaseType.php",
            {
                "state_code": str(state_code),
                "dist_code": str(dist_code),
                "court_code_arr": court_code,  # Use court_code_arr like the app
                "case_type": str(case_type),
                "year": str(year),
                "pendingDisposed": pending_disposed,
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )

        if not result:
            return []

        # print(f"   DEBUG search result: {json.dumps(result, indent=2)[:500]}")

        # Response can be a dict with court_code -> cases
        cases = []
        if isinstance(result, dict):
            for court_key, court_data in result.items():
                if isinstance(court_data, dict) and "caseNos" in court_data:
                    for c in court_data["caseNos"]:
                        cases.append(Case(
                            case_no=c.get("case_no") or c.get("filing_no", ""),
                            cino=c.get("cino", ""),
                            case_type=c.get("type_name", ""),
                            case_number=c.get("case_no2", ""),
                            reg_year=c.get("reg_year", ""),
                            petitioner=c.get("petnameadArr", ""),
                            court_code=str(c.get("court_code", court_key)),
                        ))

        return cases

    def get_case_history(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_no: str,
        language: str = "english"
    ) -> Optional[dict]:
        """
        Get full case history and details.

        Args:
            state_code: State code
            dist_code: District code
            court_code: Court code
            case_no: Case number
            language: Language flag

        Returns:
            Case history data or None
        """
        result = self._make_request(
            "caseHistoryWebService.php",
            {
                "state_code": str(state_code),
                "dist_code": str(dist_code),
                "court_code": court_code,
                "case_no": case_no,
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )

        if not result or "history" not in result:
            return None

        return result["history"]

    def get_labels(self, language: str = "english") -> Optional[dict]:
        """Get UI labels (useful for understanding data)."""
        result = self._make_request(
            "getAllLabelsWebService.php",
            {
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )
        return result

    @staticmethod
    def extract_orders_from_html(html: str, is_final: bool = True) -> list[Order]:
        """
        Extract order information from HTML response.

        Args:
            html: HTML string from finalOrder or interimOrder field
            is_final: Whether this is from finalOrder (True) or interimOrder (False)

        Returns:
            List of Order objects with PDF URLs
        """
        orders = []
        if not html or "Order not uploaded" in html:
            return orders

        # Find all rows in the order table
        # Pattern: order number, date, and link
        row_pattern = r'<tr><td[^>]*>(?:&nbsp;)*(\d+)</td><td[^>]*>(?:&nbsp;)*([^<]+)</td><td[^>]*>.*?(?:<a[^>]*href\s*=\s*[\'"]([^\'"]+)[\'"][^>]*>.*?<font[^>]*>(?:&nbsp;)*([^<]+)</font>|<span[^>]*color:[^>]*green[^>]*>(?:&nbsp;)*([^<]+)</span>)'

        # Also try simpler pattern for links
        link_pattern = r"<a[^>]*href\s*=\s*['\"]([^'\"]+display_pdf[^'\"]+)['\"][^>]*>.*?<font[^>]*>\s*(?:&nbsp;)*\s*([^<]+?)\s*</font>"

        # Find all PDF links
        links = re.findall(link_pattern, html, re.IGNORECASE | re.DOTALL)

        # Try to parse table rows
        # Simple pattern: look for order number and date in table cells
        cell_pattern = r'<td[^>]*>(?:&nbsp;)*(\d+)</td>\s*<td[^>]*>(?:&nbsp;)*(\d{2}-\d{2}-\d{4})</td>'
        rows = re.findall(cell_pattern, html)

        if links:
            for i, match in enumerate(links):
                url = match[0]
                order_type = (match[1] if len(match) > 1 else "Order").strip()
                order_num = i + 1
                order_date = ""

                # Try to match with row data
                if i < len(rows):
                    order_num = int(rows[i][0])
                    order_date = rows[i][1]

                orders.append(Order(
                    order_number=order_num,
                    order_date=order_date,
                    order_type=order_type,
                    pdf_url=url,
                    is_final=is_final
                ))

        return orders

    def get_orders_from_history(self, history: dict) -> tuple[list[Order], list[Order]]:
        """
        Extract all orders from case history.

        Args:
            history: Case history dictionary

        Returns:
            Tuple of (final_orders, interim_orders)
        """
        final_orders = []
        interim_orders = []

        final_html = history.get("finalOrder", "")
        if final_html:
            final_orders = self.extract_orders_from_html(final_html, is_final=True)

        interim_html = history.get("interimOrder", "")
        if interim_html:
            interim_orders = self.extract_orders_from_html(interim_html, is_final=False)

        return final_orders, interim_orders

    def download_pdf(
        self,
        pdf_url: str,
        output_path: str,
        retry_count: int = 3
    ) -> bool:
        """
        Download a PDF from the given URL.

        The PDF URLs contain encrypted params and authtoken that are session-bound.
        This method extracts the params, re-encrypts them with the current session's
        JWT token, and downloads the PDF.

        Args:
            pdf_url: Full URL to the PDF (containing encrypted params/authtoken)
            output_path: Local path to save the PDF
            retry_count: Number of retries on failure

        Returns:
            True if download succeeded, False otherwise
        """
        # Parse the URL to extract encrypted params
        parsed = urlparse(pdf_url)
        query_params = parse_qs(parsed.query)

        encrypted_params = query_params.get("params", [""])[0]

        if not encrypted_params:
            return False

        try:
            # Decrypt the original params to get the filename and other data
            original_params = decrypt_url_param(encrypted_params)

            if not isinstance(original_params, dict):
                return False

            # Build fresh params with the same data
            fresh_params = {
                "filename": original_params.get("filename", ""),
                "caseno": original_params.get("caseno", ""),
                "cCode": original_params.get("cCode", ""),
                "appFlag": original_params.get("appFlag", "1"),
                "state_cd": original_params.get("state_cd", ""),
                "dist_cd": original_params.get("dist_cd", ""),
                "court_code": original_params.get("court_code", ""),
                "bilingual_flag": original_params.get("bilingual_flag", "0"),
            }

            # Re-encrypt params
            new_encrypted_params = encrypt_data_cbc(fresh_params)

            # Encrypt our current JWT token as authtoken
            auth_token = self.jwt_token if self.jwt_token else ""
            encrypted_auth = encrypt_data_cbc(f"Bearer {auth_token}")

            # Build the new URL
            new_url = f"{self.base_url}/display_pdf.php"

            headers = {
                **DEFAULT_HEADERS,
                "Cookie": self.jsession,
            }

        except Exception:
            return False

        for attempt in range(retry_count):
            try:
                response = self.session.get(
                    new_url,
                    params={
                        "params": new_encrypted_params,
                        "authtoken": encrypted_auth,
                    },
                    headers=headers,
                    timeout=120,
                    stream=True
                )

                if response.status_code == 200:
                    # Check first few bytes to see if it's a PDF
                    first_chunk = next(response.iter_content(chunk_size=4), b'')

                    if first_chunk == b'%PDF':
                        # Ensure directory exists
                        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

                        with open(output_path, "wb") as f:
                            f.write(first_chunk)
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        return True
                    else:
                        # Might be encrypted error response, try to decrypt
                        content = first_chunk + response.content
                        if len(content) > 32:
                            try:
                                text = content.decode('utf-8', errors='ignore').strip()
                                if all(c in '0123456789abcdef' for c in text[:32]):
                                    decrypted = decrypt_response_cbc(text)
                                    # If we got here, it's an error - retry won't help
                                    return False
                            except Exception:
                                pass
                        return False

            except requests.RequestException:
                if attempt < retry_count - 1:
                    time.sleep(1)
                continue

        return False

    def download_pdf_direct(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        filename: str,
        case_no: str,
        output_path: str,
        retry_count: int = 3
    ) -> bool:
        """
        Download a PDF directly using filename and case information.

        This is an alternative method when you have the filename from decrypted params.

        Args:
            state_code: State code
            dist_code: District code
            court_code: Court code
            filename: PDF filename (e.g., '/orders/2025/205400023292025_2.pdf')
            case_no: Case number
            output_path: Local path to save the PDF
            retry_count: Number of retries on failure

        Returns:
            True if download succeeded, False otherwise
        """
        # Build params
        params = {
            "filename": filename,
            "caseno": case_no,
            "cCode": court_code,
            "appFlag": "1",
            "state_cd": str(state_code),
            "dist_cd": str(dist_code),
            "court_code": court_code,
            "bilingual_flag": "0",
        }

        # Encrypt params
        encrypted_params = encrypt_data_cbc(params)

        # Encrypt our JWT token as authtoken
        auth_token = self.jwt_token if self.jwt_token else ""
        encrypted_auth = encrypt_data_cbc(f"Bearer {auth_token}")

        url = f"{self.base_url}/display_pdf.php"

        headers = {
            **DEFAULT_HEADERS,
            "Cookie": self.jsession,
        }

        for attempt in range(retry_count):
            try:
                response = self.session.get(
                    url,
                    params={
                        "params": encrypted_params,
                        "authtoken": encrypted_auth,
                    },
                    headers=headers,
                    timeout=120,
                    stream=True
                )

                if response.status_code == 200:
                    first_chunk = next(response.iter_content(chunk_size=4), b'')

                    if first_chunk == b'%PDF':
                        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

                        with open(output_path, "wb") as f:
                            f.write(first_chunk)
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        return True
                    else:
                        return False

            except requests.RequestException:
                if attempt < retry_count - 1:
                    time.sleep(1)
                continue

        return False


def main():
    """Test the API client."""
    client = MobileAPIClient()

    print("Testing eCourts Mobile API Client")
    print("=" * 60)

    # Get states
    print("\n1. Getting states...")
    states = client.get_states()
    print(f"   Found {len(states)} states")
    for s in states[:5]:
        print(f"   - {s.code}: {s.name}")
    print("   ...")

    if not states:
        print("Failed to get states")
        return

    # Get districts for state 29 (Telangana based on results)
    print("\n2. Getting districts for state 29...")
    districts = client.get_districts(29)
    print(f"   Found {len(districts)} districts")
    for d in districts[:5]:
        print(f"   - {d.code}: {d.name}")
    print("   ...")

    if not districts:
        return

    # Get court complexes for Hyderabad (larger city, more cases)
    district = next((d for d in districts if "hyderabad" in d.name.lower()), districts[0])
    print(f"\n3. Getting court complexes for {district.name}...")
    complexes = client.get_court_complexes(29, district.code)
    print(f"   Found {len(complexes)} court complexes")
    for c in complexes[:3]:
        print(f"   - {c.code}: {c.name} (NJDG: {c.njdg_est_code})")
    print("   ...")

    if not complexes:
        return

    # Get case types for City Small Causes Court (more civil cases)
    complex_ = next((c for c in complexes if "small" in c.name.lower()), complexes[0])
    print(f"\n4. Getting case types for {complex_.name}...")
    case_types = client.get_case_types(29, district.code, complex_.njdg_est_code)
    print(f"   Found {len(case_types)} case types")
    for ct in case_types[:5]:
        print(f"   - {ct.code}: {ct.name}")
    print("   ...")

    if not case_types:
        return

    # Try multiple case types until we find one with data
    print(f"\n5. Searching cases (using complex_code={complex_.code})...")
    cases = []
    for case_type in case_types[:5]:  # Try first 5 case types
        for year in [2023, 2022]:
            # Try both complex_code and njdg_est_code
            for court_id in [complex_.code, complex_.njdg_est_code]:
                print(f"   Trying court={court_id}, type={case_type.code}, year={year}...")
                cases = client.search_cases_by_type(
                    state_code=29,
                    dist_code=district.code,
                    court_code=str(court_id),
                    case_type=case_type.code,
                    year=year,
                    pending_disposed="Disposed"
                )
                if cases:
                    break
            if cases:
                break
        if cases:
            break
    print(f"   Found {len(cases)} cases")
    for c in cases[:3]:
        print(f"   - {c.case_type}/{c.case_number}/{c.reg_year}: {c.petitioner[:50]}...")
    print("   ...")

    if cases:
        # Get case history
        case = cases[0]
        print(f"\n6. Getting case history for {case.case_no}...")
        history = client.get_case_history(
            state_code=29,
            dist_code=district.code,
            court_code=case.court_code,
            case_no=case.case_no
        )
        if history:
            print(f"   Got case history with {len(history)} sections")
            print(f"   Keys: {list(history.keys()) if isinstance(history, dict) else 'list'}")


if __name__ == "__main__":
    main()
