"""
clutch_scraper.py
-----------------
Scrapes a Clutch.co category directory and outputs a filtered CSV of companies
matching the PipelineSpark ICP filters (US/Canada, 3-30 employees, $5k+ min project).

Usage:
  python scripts/clutch_scraper.py
  # will prompt for niche URL + max pages

Flags:
  --url <clutch_url>        skip the niche prompt
  --max-pages <N>           stop after N pages (default: all)
  --min-project <N>         min project size in $ (default: 5000)
  --max-employees <N>       upper bound on employee range (default: 30)
  --with-linkedin           open each profile page to grab LinkedIn URL (SLOW)
  --out <path>              output CSV path
"""

import argparse
import csv
import io
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse, parse_qs

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from seleniumbase import Driver

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR    = os.path.join(BASE_DIR, "logs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR,    exist_ok=True)

US_CA_REGIONS = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
    "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
    "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT",
}

FIELDS = [
    "company_name", "website", "profile_url", "linkedin_url",
    "rating", "reviews", "min_project", "hourly_rate",
    "employee_range", "location", "country",
    "focus_areas", "services_provided", "description",
    "passed_filters", "filter_notes",
]


def parse_card_text(text: str) -> dict:
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    out = {
        "company_name": lines[0] if lines else "",
        "rating": "", "reviews": "", "min_project": "", "hourly_rate": "",
        "employee_range": "", "location": "",
        "focus_areas": "", "services_provided": "", "description": "",
    }

    for line in lines:
        if not out["rating"]:
            m = re.fullmatch(r"(\d\.\d)", line)
            if m: out["rating"] = m.group(1)
        if not out["reviews"]:
            m = re.search(r"(\d+)\s+reviews?\b", line, re.I)
            if m: out["reviews"] = m.group(1)
        if not out["min_project"] and re.fullmatch(r"\$[\d,]+\+?", line):
            out["min_project"] = line
        if not out["min_project"] and line.startswith("<") and "$" in line and "/" not in line:
            out["min_project"] = line
        if not out["hourly_rate"] and "/ hr" in line.lower():
            out["hourly_rate"] = line
        if not out["employee_range"] and re.fullmatch(r"\d[\d,]*\s*-\s*\d[\d,]*|\d[\d,]*\+|\d[\d,]*", line):
            out["employee_range"] = line
        if not out["location"] and re.search(r",\s*[A-Z]{2}\b", line) and len(line) < 60:
            out["location"] = line

    if "SERVICES PROVIDED" in text:
        try:
            block = text.split("SERVICES PROVIDED", 1)[1].split("FOCUS AREAS", 1)[0]
            out["services_provided"] = " | ".join(
                l.strip() for l in block.split("\n") if l.strip() and re.match(r"\d+%", l.strip())
            )
        except Exception:
            pass
    if "FOCUS AREAS" in text:
        try:
            block = text.split("FOCUS AREAS", 1)[1]
            focus_lines = []
            for l in block.split("\n"):
                s = l.strip()
                if not s: continue
                if re.match(r"\d+%", s):
                    focus_lines.append(s)
                else:
                    if focus_lines: break
            out["focus_areas"] = " | ".join(focus_lines)
        except Exception:
            pass

    for line in lines:
        if len(line) > 120 and "%" not in line and "SERVICES" not in line and "FOCUS" not in line:
            out["description"] = line
            break

    return out


def decode_website(redirect_url: str) -> str:
    if not redirect_url: return ""
    try:
        qs = parse_qs(urlparse(redirect_url).query)
        u = qs.get("u", [""])[0]
        if u:
            parsed = urlparse(u)
            return f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else u
    except Exception:
        pass
    return ""


def location_country(loc: str) -> str:
    if not loc: return ""
    m = re.search(r",\s*([A-Z]{2})\b", loc)
    if not m: return ""
    code = m.group(1)
    if code in {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}:
        return "Canada"
    if code in US_CA_REGIONS:
        return "United States"
    return ""


def employees_match(emp_range: str, max_employees: int) -> bool:
    if not emp_range: return False
    nums = [int(n.replace(",", "")) for n in re.findall(r"\d[\d,]*", emp_range)]
    if not nums: return False
    return nums[0] <= max_employees


def min_project_value(text: str) -> int:
    if not text: return 0
    nums = re.findall(r"\d[\d,]*", text)
    if not nums: return 0
    return int(nums[0].replace(",", ""))


def extract_page_raw(driver) -> list[dict]:
    """Return raw card dicts from current DOM."""
    return driver.execute_script("""
        const rows = Array.from(document.querySelectorAll('.provider-row'));
        return rows.map(r => ({
            text: r.innerText || '',
            websiteRedirect: (r.querySelector('a[href*="r.clutch.co/redirect"]') || {}).href || '',
            profileUrl: ((r.querySelector('a[href*="/profile/"]') || {}).href || '').split('?')[0].split('#')[0],
        }));
    """) or []


def merge_into_seen(seen: dict, cards_raw: list[dict]) -> int:
    """
    Dedup by profile_url. Featured cards (no website) carry rich data.
    Directory cards (have website) carry the URL. Merge both into one record.
    Returns count of newly added profiles.
    """
    added = 0
    for c in cards_raw:
        profile = c["profileUrl"]
        if not profile:
            continue
        website = decode_website(c["websiteRedirect"])
        data    = parse_card_text(c["text"])
        data["website"]      = website
        data["profile_url"]  = profile
        data["linkedin_url"] = ""
        data["country"]      = location_country(data["location"])

        if profile not in seen:
            seen[profile] = data
            added += 1
        else:
            existing = seen[profile]
            # Fill gaps: prefer non-empty values from either card
            for key in ("website", "rating", "reviews", "min_project", "hourly_rate",
                        "employee_range", "location", "country", "focus_areas",
                        "services_provided", "description"):
                if not existing.get(key) and data.get(key):
                    existing[key] = data[key]
    return added


def apply_filters(row: dict, min_project: int, max_employees: int) -> tuple[bool, str]:
    notes = []
    if row.get("country") not in ("United States", "Canada"):
        notes.append(f"location not US/CA ({row.get('location','')})")
    if not employees_match(row.get("employee_range",""), max_employees):
        notes.append(f"employees {row.get('employee_range','')!r} > {max_employees}")
    mp = min_project_value(row.get("min_project",""))
    if mp and mp < min_project:
        notes.append(f"min project ${mp} < ${min_project}")
    passed = len(notes) == 0
    return passed, " ; ".join(notes)


def fetch_linkedin(driver, profile_url: str) -> str:
    try:
        driver.uc_open(profile_url)
        time.sleep(3)
        li = driver.execute_script("""
            const a = document.querySelector('a[href*="linkedin.com/company"], a[href*="linkedin.com/in"]');
            return a ? a.href : '';
        """)
        return li or ""
    except Exception:
        return ""


def save_checkpoint(rows: list[dict], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def log_error(msg: str, log_path: str):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()} | {msg}\n")


def wait_for_page_change(driver, old_first_name: str, timeout: int = 15) -> bool:
    """Wait until the first provider-row shows a different company name."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            first = driver.execute_script("""
                const r = document.querySelector('.provider-row');
                return r ? r.innerText.split('\\n')[0].trim() : '';
            """)
            if first and first != old_first_name:
                return True
        except Exception:
            pass
        time.sleep(0.8)
    return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=None)
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--min-project", type=int, default=5000)
    p.add_argument("--max-employees", type=int, default=30)
    p.add_argument("--with-linkedin", action="store_true")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    url = args.url
    if not url:
        print("\nWhich Clutch niche do you want to scrape?")
        print("Paste the full Clutch category URL:")
        print("  e.g. https://clutch.co/real-estate/commercial-property-management/janitorial-cleaning\n")
        url = input("URL: ").strip()
    if not url.startswith("https://clutch.co/"):
        sys.exit("[ERROR] URL must start with https://clutch.co/")

    max_pages     = args.max_pages
    min_project   = args.min_project
    max_employees = args.max_employees

    niche_slug = url.rstrip("/").split("/")[-1]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_path = args.out or os.path.join(OUTPUT_DIR, f"clutch_{niche_slug}_{ts}.csv")
    err_path = os.path.join(LOG_DIR, f"clutch_errors_{ts}.txt")

    print(f"\nScraping: {url}")
    print(f"Filters:  min project ${min_project:,}, employees <={max_employees}, US/Canada only")
    print(f"Output:   {out_path}")
    print(f"Errors:   {err_path}\n")

    driver = Driver(uc=True, headless=False)
    # profile_url → merged dict
    seen: dict[str, dict] = {}

    try:
        page = 1
        consecutive_no_new = 0

        while True:
            page_url = f"{url}?page={page}" if page > 1 else url
            print(f"[page {page}] {page_url}")

            # Remember first company name before navigation so we can detect DOM update
            old_first = driver.execute_script("""
                const r = document.querySelector('.provider-row');
                return r ? r.innerText.split('\\n')[0].trim() : '';
            """) if page > 1 else ""

            try:
                driver.uc_open(page_url)
                time.sleep(5)
                try: driver.uc_gui_click_captcha()
                except Exception: pass
                time.sleep(2)
            except Exception as e:
                log_error(f"page {page} load failed: {e}", err_path)
                print(f"  [err] load failed: {e}")
                break

            # For pages 2+, confirm DOM changed (not showing page 1 content again)
            if page > 1 and old_first:
                changed = wait_for_page_change(driver, old_first, timeout=10)
                if not changed:
                    print(f"  [warn] DOM didn't change from page {page-1} — content may be same. Trying get().")
                    driver.get(page_url)
                    time.sleep(6)
                    changed = wait_for_page_change(driver, old_first, timeout=8)
                    if not changed:
                        print(f"  [warn] Still no change — recording zero new and continuing.")

            cards_raw  = extract_page_raw(driver)
            before     = len(seen)
            new_count  = merge_into_seen(seen, cards_raw)
            total_seen = len(seen)

            # Apply filters to newly seen rows for the pass_count display
            pass_count = 0
            for profile, row in seen.items():
                if "passed_filters" not in row:
                    passed, notes = apply_filters(row, min_project, max_employees)
                    row["passed_filters"] = "yes" if passed else "no"
                    row["filter_notes"]   = notes
                    if passed: pass_count += 1

            print(f"  cards={len(cards_raw)}, new profiles={new_count}, total={total_seen}")

            # Checkpoint every 50 unique profiles
            if total_seen and total_seen % 50 < new_count + 1:
                save_checkpoint(list(seen.values()), out_path)
                print(f"  [checkpoint] wrote {total_seen} rows")

            if new_count == 0:
                consecutive_no_new += 1
                if consecutive_no_new >= 2:
                    print("  2 pages with no new profiles — stopping")
                    break
            else:
                consecutive_no_new = 0

            if max_pages and page >= max_pages:
                print(f"  reached max-pages={max_pages}")
                break

            has_next = driver.execute_script("""
                return !!document.querySelector('a[href*="page=%d"]');
            """ % (page + 1))
            if not has_next:
                print("  no next-page link — done")
                break
            page += 1

        # Optional LinkedIn pass
        if args.with_linkedin:
            qualified = [r for r in seen.values() if r.get("passed_filters") == "yes" and r.get("profile_url")]
            print(f"\nFetching LinkedIn for {len(qualified)} qualified rows...")
            for i, r in enumerate(qualified, 1):
                r["linkedin_url"] = fetch_linkedin(driver, r["profile_url"])
                if i % 10 == 0:
                    save_checkpoint(list(seen.values()), out_path)
                    print(f"  [{i}/{len(qualified)}] linkedin checkpoint")

    finally:
        all_rows = list(seen.values())
        save_checkpoint(all_rows, out_path)
        try: driver.quit()
        except Exception: pass

    all_rows = list(seen.values())
    total    = len(all_rows)
    passed   = sum(1 for r in all_rows if r.get("passed_filters") == "yes")
    print(f"\n--- DONE ---")
    print(f"Total unique companies scraped: {total}")
    print(f"Passed filters (US/CA, ≤{max_employees} emp, ≥${min_project:,} project): {passed}")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
