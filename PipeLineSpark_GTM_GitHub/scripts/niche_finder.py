"""
niche_finder.py
---------------
Generates B2B niche candidates, builds Apollo search URLs, then opens Apollo
in a real browser to verify each niche has at least 2,000 contacts before saving.

Run:
  python scripts/niche_finder.py
  python scripts/niche_finder.py --count 50
  python scripts/niche_finder.py --focus "trades"
  python scripts/niche_finder.py --min-tam 5000
  python scripts/niche_finder.py --skip-tam          # skip browser check
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import random
from datetime import datetime, timezone
from urllib.parse import quote

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

DEEPSEEK_API_KEY    = os.getenv("DEEPSEEK_API_KEY")
GOOGLE_CREDS_FILE   = os.getenv("GOOGLE_CREDS_FILE")
GOOGLE_SHEET_ID     = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_GID    = int(os.getenv("GOOGLE_SHEET_GID", "0"))
APOLLO_SESSION_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "apollo_session.pkl")

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
SEEN_FILE  = os.path.join(BASE_DIR, "logs", "seen_niches.json")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com/v1")


# ── Apollo URL builder ─────────────────────────────────────────────────────────

TITLES = ["CEO", "Founder", "vp of sales", "owner"]

def build_apollo_url(keywords: list[str], max_employees: int = 20) -> str:
    base = "https://app.apollo.io/#/people"
    parts = ["contactEmailStatusV2[]=verified"]
    for title in TITLES:
        parts.append(f"personTitles[]={quote(title, safe='')}")
    parts += [
        "sortAscending=false",
        "sortByField=%5Bnone%5D",
        "marketSegments[]=b2b",
        "personLocations[]=Canada",
        "personLocations[]=United%20States",
        f"organizationNumEmployeesRanges[]=3%2C{max_employees}",
        "includedOrganizationKeywordFields[]=tags",
        "includedOrganizationKeywordFields[]=name",
    ]
    for kw in keywords:
        parts.append(f"qOrganizationKeywordTags[]={quote(kw.strip(), safe='')}")
    parts += ["page=1", "recommendationConfigId=score"]
    return f"{base}?{'&'.join(parts)}"


# ── TAM verification via Selenium ─────────────────────────────────────────────

def _parse_count(text: str) -> int | None:
    """Parse Apollo count strings like '2,341 total', '1.2K total', '3M total'."""
    m = re.search(r'([\d,]+\.?\d*)\s*([KkMm])?\s*total', text, re.I)
    if not m:
        return None
    num_str = m.group(1).replace(',', '')
    suffix  = (m.group(2) or '').upper()
    try:
        value = float(num_str)
        if suffix == 'K':
            value *= 1_000
        elif suffix == 'M':
            value *= 1_000_000
        return int(value)
    except ValueError:
        return None


def _get_count_from_page(driver) -> int | None:
    """Extract the total count shown on an Apollo people search page."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    # Wait for the spinner to disappear (up to 20s)
    try:
        WebDriverWait(driver, 20).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, '[data-cy="loader"]'))
        )
    except Exception:
        pass

    # Now poll for the count text (up to 10 more seconds)
    deadline = time.time() + 10
    while time.time() < deadline:
        body = driver.execute_script("return document.body.innerText")
        m = re.search(r'Total\s*([\d,]+\.?\d*)\s*([KkMm]?)', body, re.I)
        if m:
            num_str = m.group(1).replace(',', '')
            suffix  = m.group(2).upper()
            try:
                value = float(num_str)
                if suffix == 'K':
                    value *= 1_000
                elif suffix == 'M':
                    value *= 1_000_000
                return int(value)
            except ValueError:
                pass
        time.sleep(1)
    return None


def _build_driver() -> "webdriver.Chrome":
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    # Use selenium-manager to auto-download the right chromedriver
    return webdriver.Chrome(options=opts)


def _ensure_logged_in(driver):
    driver.get("https://app.apollo.io/#/login")
    print("\n  A browser window has opened.")
    print("  Log into Apollo using Google, then come back here and press Enter.")
    input("  Press Enter once you are fully logged in and see the Apollo dashboard... ")


def verify_tam(niches: list[dict], min_tam: int) -> list[dict]:
    """Open Apollo in a real browser, check each niche's contact count, drop < min_tam."""
    try:
        from selenium import webdriver
    except ImportError:
        print("[WARN] selenium not installed — skipping TAM check. Run: pip install selenium")
        for n in niches:
            n["tam"] = "unchecked"
        return niches

    print(f"\nOpening Apollo to verify TAM (minimum {min_tam:,} contacts) ...")

    driver = _build_driver()
    verified = []

    try:
        _ensure_logged_in(driver)

        for i, niche in enumerate(niches, 1):
            label = niche["niche"]
            print(f"  [{i}/{len(niches)}] {label} ... ", end="", flush=True)

            try:
                driver.get(niche["apollo_url"])
                time.sleep(3)
                count = _get_count_from_page(driver)
            except Exception as e:
                print(f"error ({e}) — keeping")
                niche["tam"] = "unknown"
                verified.append(niche)
                continue

            if count is None:
                print("count not found — keeping")
                niche["tam"] = "unknown"
                verified.append(niche)
            elif count < min_tam:
                print(f"{count:,} - SKIP (< {min_tam:,})")
                niche["tam"] = count
            else:
                keywords = [k.strip() for k in niche["keywords_str"].split(",") if k.strip()]
                if 1000 <= count < 2000:
                    niche["apollo_url"] = build_apollo_url(keywords, max_employees=30)
                    print(f"{count:,} - PASS (expanded to 3-30 employees)")
                elif 2000 <= count < 2500:
                    niche["apollo_url"] = build_apollo_url(keywords, max_employees=25)
                    print(f"{count:,} - PASS (expanded to 3-25 employees)")
                else:
                    print(f"{count:,} - PASS")
                niche["tam"] = count
                verified.append(niche)
                niche["tam"] = count

            time.sleep(random.uniform(2, 4))

    finally:
        driver.quit()

    passed = len(verified)
    dropped = len(niches) - passed
    print(f"\n{passed} passed, {dropped} dropped (TAM < {min_tam:,})")
    return verified


# ── Niche generation ───────────────────────────────────────────────────────────

def call_deepseek(batch_count: int, focus_line: str, exclude: list[str]) -> list[dict]:
    exclude_line = ("\nDo NOT repeat these: " + ", ".join(exclude)) if exclude else ""
    prompt = f"""You are a B2B outbound sales strategist. Generate exactly {batch_count} business niches for cold email outreach.

HARD FILTERS — every niche must pass ALL of these:
1. Abundant in United States and Canada — thousands of companies exist
2. Company size 3-20 employees, owner is the decision maker
3. Average contract value $2000 or more
4. B2B only — sells to businesses, not consumers
5. Reachable by cold email — not franchise, not enterprise, not retail
6. Specific enough that every company in the niche does the exact same thing{focus_line}{exclude_line}

NICHE SPECIFICITY — this is critical:
- BAD: "trade contractors" — too broad, covers 10 different industries
- BAD: "consulting firms" — meaningless
- GOOD: "commercial electrical contractors" — everyone does the same work
- GOOD: "revenue cycle management consultants" — specific service, specific buyer
- Every niche must be narrow enough that all companies share the same buyer, same pain, same offer

KEYWORD RULES — most important part:
- Exactly 15 keywords per niche, every one describing THE EXACT SAME business type
- Use 2-5 word phrases — never single generic words
- FORBIDDEN single words: "industrial", "consulting", "service", "commercial", "management", "equipment", "contractor", "company" alone — these match thousands of unrelated businesses
- GOOD example for commercial HVAC contractors: ["commercial HVAC contractor", "HVAC contracting company", "commercial heating and cooling contractor", "commercial air conditioning contractor", "HVAC mechanical contractor", "commercial HVAC installation", "commercial HVAC service company", "HVAC system installation contractor", "commercial HVAC repair company", "rooftop unit contractor", "commercial refrigeration contractor", "HVAC balancing contractor", "commercial ductwork contractor", "chiller system contractor", "commercial ventilation contractor"]
- BAD example: ["hvac", "mechanical", "contractor", "commercial", "service", "building"] — single generic words that pull in unrelated companies
- GOOD example for MSP: ["managed IT services", "managed service provider", "MSP company", "IT managed services provider", "co-managed IT services", "remote monitoring and management", "IT infrastructure management company", "managed network services", "managed helpdesk services", "managed security services provider", "business IT support company", "managed IT support", "managed cloud services provider", "IT consulting and managed services", "managed IT solutions provider"]

IMPORTANT: Use only straight double quotes. No apostrophes inside any string value.

For each niche:
- niche: 2-5 words, lowercase plural, specific not broad
- keywords: exactly 15 tight keyword phrases (2-5 words each, no single generic words)
- icp: exact decision maker title, 2-4 words
- why_high_ticket: one short sentence, no apostrophes
- pain_point: one short sentence, no apostrophes
- viability_score: integer 1-10

Respond ONLY with valid JSON, no markdown, no extra text:
{{"niches": [{{"niche": "example niche", "keywords": ["kw1","kw2","kw3","kw4","kw5","kw6","kw7","kw8","kw9","kw10","kw11","kw12","kw13","kw14","kw15"], "icp": "example icp", "why_high_ticket": "reason", "pain_point": "pain", "viability_score": 8}}]}}"""


    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5,
        max_tokens=8000,
    )
    content = resp.choices[0].message.content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```\w*\n?", "", content).rstrip("```").strip()

    try:
        return json.loads(content).get("niches", [])
    except json.JSONDecodeError:
        cut = content.rfind('},')
        if cut != -1:
            try:
                return json.loads(content[:cut + 1] + "]}").get("niches", [])
            except Exception:
                pass
        print("[WARN] Could not parse batch — skipping")
        return []


def load_seen_niches() -> list[str]:
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []


def save_seen_niches(existing: list[str], new_niches: list[dict]):
    updated = list(set(existing + [n.get("niche", "") for n in new_niches]))
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(updated, f, indent=2)


def generate_niches(count: int, focus: str, skip_tam: bool, min_tam: int) -> list[dict]:
    focus_line = f"\nFocus area: {focus}" if focus else ""
    previously_seen = load_seen_niches()
    all_niches = []
    seen = list(previously_seen)

    if previously_seen:
        print(f"Excluding {len(previously_seen)} niches from previous runs")

    for i in range(0, count, 20):
        batch = min(20, count - i)
        print(f"Generating niches {i+1}-{i+batch} ...")
        results = call_deepseek(batch, focus_line, seen)
        all_niches.extend(results)
        seen.extend(n.get("niche", "") for n in results)

    for n in all_niches:
        keywords = n.get("keywords", [n.get("niche", "")])
        n["apollo_url"]   = build_apollo_url(keywords)
        n["keywords_str"] = ", ".join(keywords)
        n["tam"]          = "unchecked"

    all_niches.sort(key=lambda x: -x.get("viability_score", 0))

    # Save all generated niches to seen (so we don't regenerate them next run)
    save_seen_niches(previously_seen, all_niches)

    if not skip_tam:
        all_niches = verify_tam(all_niches, min_tam)

    return all_niches


# ── Output ─────────────────────────────────────────────────────────────────────

def print_results(niches: list[dict]):
    strong = [n for n in niches if n.get("viability_score", 0) >= 8]
    mid    = [n for n in niches if 6 <= n.get("viability_score", 0) < 8]
    weak   = [n for n in niches if n.get("viability_score", 0) < 6]

    print("\n" + "=" * 75)
    print(f"  NICHE FINDER — {len(niches)} niches passed TAM filter")
    print("=" * 75)

    def tam_label(n):
        t = n.get("tam", "unchecked")
        if isinstance(t, int):
            return f"{t:,}"
        return str(t)

    print(f"\n  STRONG (8-10) — {len(strong)} niches\n")
    for n in strong:
        print(f"  [{n['viability_score']}/10] {n['niche']}  (TAM: {tam_label(n)})")
        print(f"         ICP:             {n['icp']}")
        print(f"         Why high ticket: {n['why_high_ticket']}")
        print(f"         Pain point:      {n['pain_point']}")
        print(f"         Keywords:        {n['keywords_str']}")
        print(f"         Apollo:          {n['apollo_url']}")
        print()

    if mid:
        print(f"\n  DECENT (6-7) — {len(mid)} niches\n")
        for n in mid:
            print(f"  [{n['viability_score']}/10] {n['niche']}  (TAM: {tam_label(n)})")
            print(f"         Keywords: {n['keywords_str']}")
            print(f"         Apollo:   {n['apollo_url']}")
            print()

    if weak:
        print(f"\n  SKIP (<6) — {len(weak)} niches")
        for n in weak:
            print(f"  [{n['viability_score']}/10] {n['niche']}  (TAM: {tam_label(n)})")
        print()

    print("=" * 75)


FIELDS = ["viability_score", "tam", "niche", "keywords", "icp", "why_high_ticket", "pain_point", "apollo_url"]

def _niche_to_row(n: dict) -> dict:
    return {
        "viability_score": n["viability_score"],
        "tam":             n.get("tam", "unchecked"),
        "niche":           n["niche"],
        "keywords":        n["keywords_str"],
        "icp":             n["icp"],
        "why_high_ticket": n["why_high_ticket"],
        "pain_point":      n["pain_point"],
        "apollo_url":      n["apollo_url"],
    }

def save_csv(niches: list[dict]) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_path = os.path.join(OUTPUT_DIR, f"niches_{ts}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows([_niche_to_row(n) for n in niches])
    return out_path


def merge_into_csv(niches: list[dict], target_path: str) -> int:
    """Append niches into target CSV, skipping any niche name already present. Returns count added."""
    existing_niches = set()
    existing_rows = []

    if os.path.exists(target_path):
        with open(target_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
            existing_niches = {r["niche"].strip().lower() for r in existing_rows}

    new_rows = [
        _niche_to_row(n) for n in niches
        if n["niche"].strip().lower() not in existing_niches
    ]

    all_rows = existing_rows + new_rows
    with open(target_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    return len(new_rows)


# ── Google Sheets ─────────────────────────────────────────────────────────────

def write_to_sheets(csv_path: str):
    creds_path = os.path.join(BASE_DIR, GOOGLE_CREDS_FILE) if GOOGLE_CREDS_FILE else None
    if not (creds_path and os.path.exists(creds_path) and GOOGLE_SHEET_ID):
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
        worksheet = next((ws for ws in sh.worksheets() if ws.id == GOOGLE_SHEET_GID), sh.sheet1)
        with open(csv_path, encoding="utf-8") as f:
            rows = list(csv.reader(f))
        worksheet.clear()
        worksheet.update(rows)
        print(f"Google Sheets updated: {worksheet.title}")
    except Exception as e:
        print(f"[WARN] Sheets write failed: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        row.setdefault("tam", "unchecked")
        row["keywords_str"] = row.get("keywords", "")
        row["viability_score"] = int(row.get("viability_score", 0))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count",       type=int, default=40,   help="Niches to generate (default: 40)")
    parser.add_argument("--focus",       default="",             help='Focus area e.g. "trades"')
    parser.add_argument("--min-tam",     type=int, default=1000, help="Minimum Apollo contact count (default: 1000)")
    parser.add_argument("--skip-tam",    action="store_true",    help="Skip browser TAM check")
    parser.add_argument("--verify-only", metavar="CSV",          help="Skip generation, just TAM-check an existing CSV")
    parser.add_argument("--merge-into",  metavar="CSV",          help="Merge passing niches into this existing CSV instead of creating a new file")
    args = parser.parse_args()

    if args.verify_only:
        if not os.path.exists(args.verify_only):
            sys.exit(f"[ERROR] File not found: {args.verify_only}")
        print(f"Loading {args.verify_only} ...")
        niches = load_csv(args.verify_only)
        print(f"Loaded {len(niches)} niches — running TAM check only")
        niches = verify_tam(niches, args.min_tam)
    else:
        if not DEEPSEEK_API_KEY:
            sys.exit("[ERROR] DEEPSEEK_API_KEY not set in .env")
        niches = generate_niches(args.count, args.focus, args.skip_tam, args.min_tam)

    if not niches:
        print("\nNo niches passed the TAM filter. Try --min-tam 1000 or --skip-tam to see all.")
        return

    print_results(niches)

    if args.merge_into:
        added = merge_into_csv(niches, args.merge_into)
        print(f"\nAdded {added} new niches to: {args.merge_into}")
        if added < len(niches):
            print(f"({len(niches) - added} already existed — skipped)")
        write_to_sheets(args.merge_into)
    else:
        out_path = save_csv(niches)
        print(f"\nSaved {len(niches)} niches to: {out_path}")
        write_to_sheets(out_path)


if __name__ == "__main__":
    main()
