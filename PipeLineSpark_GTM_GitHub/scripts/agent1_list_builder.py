"""
agent1_list_builder.py
-----------------------
Takes any raw CSV and outputs a clean, deduplicated, DNS-validated CSV ready for enrichment.
- Auto-detects column names (Google Maps, Apollo, LinkedIn, plain company lists)
- Normalises websites to https://domain.com, strips UTMs
- DNS validates all domains — logs dead ones to logs/
- Detects whether emails are already present (Apollo exports)
- Deduplicates by domain
- Run: python scripts/agent1_list_builder.py --input input/raw.csv [--output input/cleaned.csv]
"""

import argparse
import csv
import io
import os
import re
import socket
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
INPUT_DIR = os.path.join(BASE_DIR, "input")
os.makedirs(LOG_DIR, exist_ok=True)

MAX_WORKERS = 20

# ── Column name detection ──────────────────────────────────────────────────────

COMPANY_COLS = ["company_name", "company", "organization", "business_name", "name", "Company Name"]
WEBSITE_COLS = ["website", "url", "domain", "company_website", "Website", "URL"]
FIRST_NAME_COLS = ["first_name", "firstname", "First Name", "first name"]
LAST_NAME_COLS = ["last_name", "lastname", "Last Name", "last name"]
EMAIL_COLS = ["email", "email_address", "Email", "Email Address", "work_email"]


def find_col(fieldnames: list, candidates: list) -> str | None:
    lower = {f.lower(): f for f in fieldnames}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


# ── URL normalisation ──────────────────────────────────────────────────────────

def normalise_url(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    parsed = urlparse(raw)
    # Strip UTMs and query strings
    clean = urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))
    return clean


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc or parsed.path
    host = re.sub(r"^www\.", "", host.lower().strip())
    return host.split("/")[0]


# ── DNS validation ─────────────────────────────────────────────────────────────

def dns_ok(domain: str) -> bool:
    try:
        socket.setdefaulttimeout(5)
        socket.getaddrinfo(domain, None)
        return True
    except Exception:
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to raw CSV")
    parser.add_argument("--output", default="", help="Output path (default: input/<basename>_cleaned_<ts>.csv)")
    parser.add_argument("--test", action="store_true", help="Process first 20 rows only")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.isabs(input_path):
        input_path = os.path.join(BASE_DIR, input_path)

    print(f"Loading {input_path} ...")
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if args.test:
        rows = rows[:20]
        print(f"[TEST] Processing first {len(rows)} rows only")

    print(f"Loaded {len(rows)} rows | Columns: {fieldnames}")

    # Detect columns
    col_company = find_col(fieldnames, COMPANY_COLS)
    col_website = find_col(fieldnames, WEBSITE_COLS)
    col_email = find_col(fieldnames, EMAIL_COLS)
    col_first = find_col(fieldnames, FIRST_NAME_COLS)
    col_last = find_col(fieldnames, LAST_NAME_COLS)

    print(f"Detected — company: {col_company} | website: {col_website} | "
          f"email: {col_email} | first: {col_first} | last: {col_last}")

    if not col_website and not col_company:
        sys.exit("[ERROR] Cannot find a company or website column. Check column names.")

    emails_present = col_email and any(r.get(col_email, "").strip() for r in rows)
    print(f"Emails already present: {emails_present}")

    # Normalise websites
    for r in rows:
        raw = r.get(col_website, "").strip() if col_website else ""
        if not raw and col_company:
            raw = ""  # no website fallback, will be flagged
        r["_website_norm"] = normalise_url(raw) if raw else ""
        r["_domain"] = extract_domain(r["_website_norm"]) if r["_website_norm"] else ""

    # Deduplicate by domain (keep first occurrence)
    seen_domains: set[str] = set()
    deduped = []
    dup_count = 0
    for r in rows:
        d = r["_domain"]
        if not d:
            deduped.append(r)
            continue
        if d in seen_domains:
            dup_count += 1
            continue
        seen_domains.add(d)
        deduped.append(r)

    print(f"Duplicates removed: {dup_count} | Unique rows: {len(deduped)}")

    # DNS validation
    domains_to_check = [r["_domain"] for r in deduped if r["_domain"]]
    print(f"DNS-validating {len(domains_to_check)} domains ({MAX_WORKERS} workers) ...")

    dns_results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(dns_ok, d): d for d in set(domains_to_check)}
        done = 0
        for fut in as_completed(futures):
            d = futures[fut]
            dns_results[d] = fut.result()
            done += 1
            if done % 100 == 0:
                print(f"  DNS checked {done}/{len(futures)}")

    dead_domains = [d for d, ok in dns_results.items() if not ok]
    print(f"Dead domains: {len(dead_domains)} | Live: {len([d for d, ok in dns_results.items() if ok])}")

    # Log dead domains
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    dead_log = os.path.join(LOG_DIR, f"dead_domains_{ts}.txt")
    with open(dead_log, "w", encoding="utf-8") as f:
        for d in sorted(dead_domains):
            f.write(d + "\n")
    print(f"Dead domains logged: {dead_log}")

    # Build output rows
    out_fieldnames = ["company_name", "website", "domain", "first_name", "last_name",
                      "email", "emails_present", "dns_ok", "notes"]

    live_count = 0
    dead_count = 0
    no_domain_count = 0
    out_rows = []
    for r in deduped:
        domain = r["_domain"]
        website_norm = r["_website_norm"]
        ok = dns_results.get(domain, None)

        notes_parts = []
        if not domain:
            notes_parts.append("no domain")
            no_domain_count += 1
        elif ok is False:
            notes_parts.append("dead domain")
            dead_count += 1
        else:
            live_count += 1

        out_rows.append({
            "company_name": r.get(col_company, "") if col_company else "",
            "website": website_norm,
            "domain": domain,
            "first_name": r.get(col_first, "") if col_first else "",
            "last_name": r.get(col_last, "") if col_last else "",
            "email": r.get(col_email, "") if col_email else "",
            "emails_present": "yes" if emails_present else "no",
            "dns_ok": "yes" if ok else ("no" if ok is False else "unknown"),
            "notes": "; ".join(notes_parts),
        })

    # Quality gate: at least 40% DNS pass
    total_with_domain = live_count + dead_count
    if total_with_domain > 0:
        pass_rate = live_count / total_with_domain
        print(f"\nDNS pass rate: {pass_rate:.0%} ({live_count}/{total_with_domain})")
        if pass_rate < 0.40:
            print(f"[WARN] DNS pass rate {pass_rate:.0%} is below 40% quality gate — check input list quality")

    # Write output
    if args.output:
        out_path = args.output
        if not os.path.isabs(out_path):
            out_path = os.path.join(BASE_DIR, out_path)
    else:
        base = re.sub(r"\.(csv)$", "", os.path.basename(input_path), flags=re.IGNORECASE)
        out_path = os.path.join(INPUT_DIR, f"{base}_cleaned_{ts}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"\n{'='*60}")
    print(f"Output:          {out_path}")
    print(f"Total rows:      {len(out_rows)}")
    print(f"Live domains:    {live_count}")
    print(f"Dead domains:    {dead_count}")
    print(f"No domain:       {no_domain_count}")
    print(f"Emails present:  {emails_present}")
    print(f"{'='*60}")
    print("\nSample (first 5 rows):")
    for r in out_rows[:5]:
        print(f"  {r['company_name']!r:40} | {r['domain']:35} | dns:{r['dns_ok']}")


if __name__ == "__main__":
    main()
