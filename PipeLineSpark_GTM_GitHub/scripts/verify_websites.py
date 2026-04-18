"""
verify_websites.py
------------------
Verifies that each company's website actually matches the company name.
Fetches each site, checks page title + body for company name keywords.
Outputs two files per input:
  - *_verified.csv   : websites that match
  - *_unmatched.csv  : websites that don't match (to be manually reviewed or dropped)

Usage:
  python scripts/verify_websites.py
"""

import csv
import os
import re
import sys
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_FILES = [
    r"C:\Users\imraa\Downloads\gmap_small_firms.csv",
    r"C:\Users\imraa\Downloads\gmap_large_firms.csv",
]

LOG_DIR = r"C:\Users\imraa\Downloads\PipeLineSpark_GTM\logs"
os.makedirs(LOG_DIR, exist_ok=True)
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
LOG_FILE = os.path.join(LOG_DIR, f"verify_websites_{ts}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

MAX_WORKERS     = 20
REQUEST_TIMEOUT = 8

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

# Words to strip before comparing (common filler words)
STRIP_WORDS = {
    "llc", "inc", "corp", "co", "ltd", "the", "a", "and", "&",
    "group", "staffing", "recruiting", "solutions", "services",
    "personnel", "professionals", "employment", "agency", "firm",
    "partners", "associates", "consulting", "branch", "office",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize(text: str) -> set:
    """Lowercase, strip punctuation, split into meaningful words."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    words = text.split()
    return {w for w in words if w not in STRIP_WORDS and len(w) > 2}


def check_match(company_name: str, page_text: str, page_title: str, domain: str) -> tuple[bool, str]:
    """
    Returns (is_match, reason).
    Match if at least 1 meaningful keyword from company name appears in:
      - page title, OR
      - domain name, OR
      - page body (first 3000 chars)
    """
    company_words = normalize(company_name)
    if not company_words:
        return True, "no keywords to check"

    domain_clean = normalize(domain)
    title_clean  = normalize(page_title)
    body_clean   = normalize(page_text[:5000])

    # Check domain first (fastest signal)
    domain_hits = company_words & domain_clean
    if domain_hits:
        return True, f"domain match: {domain_hits}"

    # Check page title
    title_hits = company_words & title_clean
    if title_hits:
        return True, f"title match: {title_hits}"

    # Check body
    body_hits = company_words & body_clean
    if len(body_hits) >= 1:
        return True, f"body match: {body_hits}"

    return False, f"no match found (looked for: {company_words})"


def verify_row(row: dict) -> dict:
    website = row.get("website", "").strip()
    name    = row.get("name", "").strip()

    if not website:
        return {**row, "verified": "NO_WEBSITE", "match_reason": ""}

    if not website.startswith("http"):
        website = "https://" + website

    parsed = urlparse(website)
    domain = parsed.netloc.lower().replace("www.", "")

    try:
        resp = requests.get(website, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            return {**row, "verified": "FETCH_FAILED", "match_reason": f"HTTP {resp.status_code}"}

        soup      = BeautifulSoup(resp.text, "html.parser")
        page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
        page_text  = soup.get_text(" ", strip=True)

        is_match, reason = check_match(name, page_text, page_title, domain)
        return {**row, "verified": "YES" if is_match else "NO", "match_reason": reason}

    except requests.exceptions.SSLError:
        # Try http fallback
        try:
            http_url = website.replace("https://", "http://")
            resp = requests.get(http_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")
            page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            page_text  = soup.get_text(" ", strip=True)
            is_match, reason = check_match(name, page_text, page_title, domain)
            return {**row, "verified": "YES" if is_match else "NO", "match_reason": reason}
        except Exception as e2:
            return {**row, "verified": "FETCH_FAILED", "match_reason": str(e2)[:100]}
    except Exception as e:
        return {**row, "verified": "FETCH_FAILED", "match_reason": str(e)[:100]}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_file(input_path: str):
    base = os.path.splitext(input_path)[0]
    verified_path   = base + "_verified.csv"
    unmatched_path  = base + "_unmatched.csv"

    with open(input_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) + ["verified", "match_reason"]

    log.info(f"\nProcessing: {os.path.basename(input_path)} ({len(rows)} rows)")

    verified   = []
    unmatched  = []
    processed  = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(verify_row, row): row for row in rows}
        for future in as_completed(futures):
            processed += 1
            result = future.result()
            if result["verified"] == "YES":
                verified.append(result)
            else:
                unmatched.append(result)

            if processed % 200 == 0:
                log.info(f"  {processed}/{len(rows)} done | Verified: {len(verified)} | Unmatched: {len(unmatched)}")

    # Write outputs
    for path, data, label in [
        (verified_path,  verified,  "Verified"),
        (unmatched_path, unmatched, "Unmatched"),
    ]:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        log.info(f"  {label}: {len(data)} rows -> {path}")


if __name__ == "__main__":
    for f in INPUT_FILES:
        process_file(f)
    log.info("\nAll done.")
