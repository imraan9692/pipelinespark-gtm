"""
agent2_enricher.py
------------------
Universal ICP-agnostic enricher. Works on any CSV with company_name + website columns.
Designed to run on agent1_list_builder.py output, but handles raw Apollo exports too.

Pipeline per company (domain):
  1. DNS check — skip dead domains immediately
  2. ICP qualification:
       a. Keyword signals from CSV columns (requires 2+ agreeing signals for a confident yes)
       b. Scrape homepage + contact/about/team pages
       c. Single DeepSeek call → icp_match + value_prop + business_icp + niche
  3. Email finding — skipped if emails already present in input:
       a. Regex on all scraped page text (free)
       b. DeepSeek fallback on combined page text (only if regex found nothing useful)
       c. Exa contents fallback (only if scraping returned no text at all)
  4. Decision maker scoring — drops tier-5 contacts from companies that have tier 1-4

Run:
  python scripts/agent2_enricher.py --input input/cleaned.csv --icp "managed IT services"
  python scripts/agent2_enricher.py --input input/cleaned.csv --icp "commercial cleaning" --test
"""

import argparse
import csv
import io
import json
import os
import re
import socket
import sys
import time
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock
from urllib.parse import urlparse, urljoin

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
EXA_API_KEY = os.getenv("EXA_API_KEY")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

MAX_WORKERS = 10
CHECKPOINT_EVERY = 50
REQUEST_TIMEOUT = 10

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com/v1")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

OUTPUT_FIELDS = [
    "first_name", "last_name", "email", "company_name", "website",
    "icp_match", "value_prop", "business_icp", "niche",
    "email_source", "confidence_score", "notes",
    "title", "dm_tier",
]

# ── Email quality constants ────────────────────────────────────────────────────

BLOCKED_DOMAINS = {"godaddy.com", "sedo.com", "parkingcrew.com", "hugedomains.com", "undeveloped.com"}
GENERIC_PREFIXES = {"info", "hello", "contact", "support", "admin", "office", "mail",
                    "team", "sales", "help", "enquiries", "enquiry", "general", "noreply",
                    "no-reply", "webmaster", "postmaster", "billing", "accounts", "hr"}
PERSONAL_PROVIDERS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
                      "ymail.com", "sbcglobal.net", "aol.com", "live.com", "msn.com", "me.com"}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# ── Decision maker title scoring ──────────────────────────────────────────────

DM_TIER_1 = [
    "chief revenue", "cro", "vp of sales", "vp sales", "head of sales",
    "director of sales", "vp of business development", "head of business development",
    "head of growth", "founding gtm", "founding sales", "vp business development",
]
DM_TIER_2 = [
    "chief executive", "ceo", "founder", "co-founder", "cofounder",
    "president", "owner", "managing partner", "managing director",
]
DM_TIER_3 = [
    "chief operating", "coo", "chief technology", "cto", "chief financial", "cfo",
    "chief marketing", "cmo", "chief product", "cpo", "general manager",
    "executive director",
]
DM_TIER_4 = [
    "vice president", "svp", "evp", "senior vice president",
    "executive vice president", "director",
]
NON_DM_TITLES = [
    "founding engineer", "founding software", "founding ai", "founding prompt",
    "founding recruiting", "business technology consultant", "agency collective",
    "member", "intern", "associate", "analyst", "engineer", "developer",
    "designer", "recruiter", "coordinator",
]


def score_title(title: str) -> int:
    t = title.lower()
    for kw in NON_DM_TITLES:
        if kw in t:
            return 5
    for kw in DM_TIER_1:
        if kw in t:
            return 1
    for kw in DM_TIER_2:
        if kw in t:
            return 2
    for kw in DM_TIER_3:
        if kw in t:
            return 3
    for kw in DM_TIER_4:
        if kw in t:
            return 4
    return 5


# ── Column detection ───────────────────────────────────────────────────────────

COMPANY_COLS = ["company_name", "company", "organization", "business_name", "Company Name"]
WEBSITE_COLS = ["website", "url", "domain", "company_website", "Website", "URL", "Company Website", "company website"]
FIRST_COLS = ["first_name", "firstname", "First Name", "first name"]
LAST_COLS = ["last_name", "lastname", "Last Name", "last name"]
EMAIL_COLS = ["email", "email_address", "Email", "Email Address", "work_email"]
TITLE_COLS = ["title", "job_title", "Title", "Job Title", "position"]


def find_col(fieldnames: list, candidates: list) -> str | None:
    lower = {f.lower(): f for f in fieldnames}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


# ── URL / domain helpers ───────────────────────────────────────────────────────

def normalise_domain(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        host = urlparse(raw).netloc.lower()
        return re.sub(r"^www\.", "", host)
    except Exception:
        return raw


def normalise_url(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    return f"{p.scheme}://{p.netloc.rstrip('/')}"


# ── Network helpers ────────────────────────────────────────────────────────────

def dns_ok(domain: str) -> bool:
    try:
        socket.setdefaulttimeout(5)
        socket.getaddrinfo(domain, None)
        return True
    except Exception:
        return False


def scrape_page(url: str) -> str:
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True)
        if resp.status_code == 200:
            text = re.sub(r"<[^>]+>", " ", resp.text)
            text = re.sub(r"\s+", " ", text).strip()
            return text[:4000]
    except Exception:
        pass
    return ""


def fetch_all_pages(base_url: str) -> str:
    slugs = ["", "/contact", "/contact-us", "/about", "/about-us",
             "/team", "/people", "/staff", "/company"]
    base = normalise_url(base_url)
    texts = []
    for slug in slugs:
        text = scrape_page(base + slug)
        if text:
            texts.append(text)
    return " ".join(texts)[:8000]


def fetch_exa_text(website: str) -> str:
    if not EXA_API_KEY or not website:
        return ""
    try:
        url = website if website.startswith("http") else f"https://{website}"
        resp = requests.post(
            "https://api.exa.ai/contents",
            headers={"x-api-key": EXA_API_KEY, "Content-Type": "application/json"},
            json={"ids": [url], "text": {"maxCharacters": 3000}},
            timeout=15,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0].get("text", "")
    except Exception:
        pass
    return ""


# ── Email extraction ───────────────────────────────────────────────────────────

def classify_email(email: str, company_domain: str) -> tuple[str, str]:
    """Returns (confidence, note). Empty string = discard."""
    e = email.lower().strip()
    domain = e.split("@")[-1] if "@" in e else ""

    if domain in BLOCKED_DOMAINS:
        return "", "blocked domain"
    prefix = e.split("@")[0]
    if domain in PERSONAL_PROVIDERS:
        return "low", "personal/free email provider"
    if prefix in GENERIC_PREFIXES:
        return "medium", "generic inbox"
    if company_domain and domain != company_domain:
        return "low", f"domain mismatch ({domain} vs {company_domain})"
    return "high", "named business email"


def extract_emails_regex(text: str, company_domain: str) -> list[dict]:
    found = []
    for match in EMAIL_RE.finditer(text):
        email = match.group().lower()
        confidence, note = classify_email(email, company_domain)
        if confidence:
            found.append({"email": email, "confidence": confidence, "note": note})
    # Deduplicate, prefer higher confidence
    seen: dict[str, dict] = {}
    for item in found:
        e = item["email"]
        if e not in seen or (item["confidence"] == "high" and seen[e]["confidence"] != "high"):
            seen[e] = item
    return list(seen.values())


def extract_emails_llm(site_text: str, company_name: str, company_domain: str) -> list[dict]:
    if not site_text.strip():
        return []
    prompt = (
        f"Extract all email addresses from the text below for the company '{company_name}' "
        f"(domain: {company_domain}).\n"
        "Include obfuscated formats like 'name [at] domain [dot] com'.\n"
        "Also extract any person names and their job titles if present.\n\n"
        f"{site_text[:4000]}\n\n"
        "Respond ONLY with valid JSON:\n"
        '{"emails": [{"email": "...", "name": "...", "title": "..."}], "contacts": [{"name": "...", "title": "...", "email": "..."}]}\n'
        "If nothing found, return empty arrays."
    )
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=400,
        )
        content = resp.choices[0].message.content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```\w*\n?", "", content).rstrip("```").strip()
        data = json.loads(content)
        results = []
        for item in data.get("emails", []) + data.get("contacts", []):
            e = item.get("email", "").lower().strip()
            if not e or "@" not in e:
                continue
            confidence, note = classify_email(e, company_domain)
            if confidence:
                results.append({
                    "email": e,
                    "confidence": confidence,
                    "note": note,
                    "name": item.get("name", ""),
                    "title": item.get("title", ""),
                })
        return results
    except Exception:
        return []


def best_email(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    order = {"high": 0, "medium": 1, "low": 2}
    return min(candidates, key=lambda x: order.get(x.get("confidence", "low"), 3))


# ── ICP + personalisation ──────────────────────────────────────────────────────

def icp_keyword_score(row: dict, icp_tokens: list[str]) -> int:
    """Count how many ICP keyword tokens appear across CSV columns."""
    combined = " ".join(str(v).lower() for v in row.values())
    return sum(1 for token in icp_tokens if token in combined)


def classify_and_enrich(company_name: str, website: str, site_text: str, icp_target: str) -> dict:
    """Single DeepSeek call: icp_match + value_prop + business_icp + niche."""
    prompt = (
        f"You are evaluating a company for a B2B cold email campaign.\n\n"
        f"TARGET ICP: {icp_target}\n\n"
        f"Company: {company_name}\n"
        f"Website: {website}\n"
        f"Site text: {site_text[:3000] if site_text else 'Not available'}\n\n"
        "Rules:\n"
        f"- icp_match: does this company match '{icp_target}'? yes / no / uncertain\n"
        "  When in doubt, use uncertain not no.\n"
        "- value_prop: completes 'I saw you ___'. Must include ONE primary service AND the specific buyer. "
        "e.g. 'offer managed IT services to dental practices'. Never list multiple services.\n"
        "- business_icp: the exact buyer from value_prop (job title or business type, 2-4 words). "
        "Use a job title if the service fits multiple industries, a business type if industry-specific. "
        "Never use vague terms like 'businesses' or 'companies'.\n"
        "- niche: 2-4 words, lowercase plural, what this company does. e.g. 'managed IT services companies'\n"
        "- confidence: high / medium / low\n"
        "- reason: one sentence explaining icp_match decision\n\n"
        "Respond ONLY with valid JSON:\n"
        '{"icp_match": "...", "value_prop": "...", "business_icp": "...", "niche": "...", '
        '"confidence": "...", "reason": "..."}'
    )
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=200,
        )
        content = resp.choices[0].message.content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```\w*\n?", "", content).rstrip("```").strip()
        return json.loads(content)
    except Exception as e:
        return {
            "icp_match": "uncertain",
            "value_prop": "",
            "business_icp": "",
            "niche": "",
            "confidence": "low",
            "reason": f"LLM error: {e}",
        }


# ── Per-domain processing ─────────────────────────────────────────────────────

def process_domain(domain: str, contacts: list[dict], icp_target: str,
                   icp_tokens: list[str], emails_in_input: bool,
                   idx: int, total: int) -> list[dict]:
    """
    Enriches all contacts for a single domain. Returns output rows.
    Scraping and LLM calls happen once per domain, shared across all contacts.
    """
    # Use first contact for company-level signals
    rep = contacts[0]
    company_name = rep.get("_col_company", "") or domain
    website = rep.get("_col_website", "") or f"https://{domain}"

    tag = f"[{idx}/{total}] {company_name[:45]}"

    # ── Step 1: DNS ──────────────────────────────────────────────────────────
    if not dns_ok(domain):
        print(f"  {tag} — dead domain")
        return [{
            **_blank_row(rep),
            "icp_match": "no",
            "notes": "dead domain",
        }]

    # ── Step 2: Keyword pre-filter ────────────────────────────────────────────
    kw_score = icp_keyword_score(rep, icp_tokens)
    keyword_confident = kw_score >= 2

    # ── Step 3: Scrape (always needed for ICP + personalization) ──────────────
    site_text = fetch_all_pages(website)
    if not site_text:
        site_text = fetch_exa_text(website)
        exa_used = bool(site_text)
    else:
        exa_used = False

    # ── Step 4: ICP + personalization (single LLM call) ──────────────────────
    if keyword_confident and not site_text:
        # Strong keyword hit but no site text — trust keyword
        enrichment = {
            "icp_match": "uncertain",
            "value_prop": "",
            "business_icp": "",
            "niche": "",
            "confidence": "medium",
            "reason": "keyword match, no site text available",
        }
    else:
        enrichment = classify_and_enrich(company_name, website, site_text, icp_target)

    icp_match = enrichment.get("icp_match", "uncertain")

    if icp_match == "no":
        print(f"  {tag} — excluded: {enrichment.get('reason', '')[:60]}")
        return [{
            **_blank_row(rep),
            "icp_match": "no",
            "notes": enrichment.get("reason", ""),
        }]

    print(f"  {tag} — {icp_match} | {enrichment.get('niche', '')[:35]}")

    # ── Step 5: Email finding (skip if emails already present) ───────────────
    email_candidates = []
    email_source = ""

    if emails_in_input:
        # Apollo export: emails already in CSV, skip finding
        email_source = "apollo"
    else:
        # Regex pass (free)
        email_candidates = extract_emails_regex(site_text, domain)
        high_conf = [e for e in email_candidates if e["confidence"] == "high"]

        if not high_conf:
            # DeepSeek fallback on combined page text
            llm_emails = extract_emails_llm(site_text, company_name, domain)
            if llm_emails:
                email_candidates = llm_emails
                email_source = "llm"
            elif not site_text:
                # Exa fallback only if scraping returned nothing
                exa_text = fetch_exa_text(website) if not exa_used else site_text
                if exa_text:
                    email_candidates = extract_emails_regex(exa_text, domain)
                    if not email_candidates:
                        email_candidates = extract_emails_llm(exa_text, company_name, domain)
                    email_source = "exa"
            else:
                email_source = "llm"
        else:
            email_source = "scrape"

    # ── Step 6: Build output rows (one per contact, apply DM scoring) ─────────
    out_rows = []
    for contact in contacts:
        row = _blank_row(contact)
        row.update({
            "icp_match": icp_match,
            "value_prop": enrichment.get("value_prop", ""),
            "business_icp": enrichment.get("business_icp", ""),
            "niche": enrichment.get("niche", ""),
            "confidence_score": enrichment.get("confidence", "medium"),
            "notes": enrichment.get("reason", ""),
        })

        if emails_in_input:
            row["email"] = contact.get("_col_email", "")
            row["email_source"] = "apollo"
        elif email_candidates:
            picked = best_email(email_candidates)
            if picked:
                row["email"] = picked["email"]
                row["email_source"] = email_source or picked.get("source", "scrape")
                row["confidence_score"] = picked["confidence"]
                if picked.get("note"):
                    row["notes"] = (row["notes"] + "; " + picked["note"]).strip("; ")
            else:
                row["email_source"] = ""
        else:
            row["notes"] = (row.get("notes", "") + "; no email found").strip("; ")

        row["dm_tier"] = score_title(row.get("title", ""))
        out_rows.append(row)

    # ── Step 7: DM scoring — drop tier-5 if tier 1-4 exists ──────────────────
    has_dm = any(r["dm_tier"] <= 4 for r in out_rows)
    if has_dm:
        kept = [r for r in out_rows if r["dm_tier"] <= 4]
        # Cap at 2 contacts per company, prefer tier 1
        kept.sort(key=lambda r: r["dm_tier"])
        out_rows = kept[:2]

    return out_rows


def _blank_row(source: dict) -> dict:
    return {
        "first_name": source.get("_col_first", ""),
        "last_name": source.get("_col_last", ""),
        "email": source.get("_col_email", ""),
        "company_name": source.get("_col_company", ""),
        "website": source.get("_col_website", ""),
        "icp_match": "",
        "value_prop": "",
        "business_icp": "",
        "niche": "",
        "email_source": "",
        "confidence_score": "",
        "notes": "",
        "title": source.get("_col_title", ""),
        "dm_tier": 5,
    }


# ── Checkpoint ────────────────────────────────────────────────────────────────

write_lock = Lock()


def load_done_domains(output_path: str) -> set[str]:
    if not os.path.exists(output_path):
        return set()
    try:
        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return {normalise_domain(r.get("website", "")) for r in reader if r.get("website")}
    except Exception:
        return set()


def append_rows(output_path: str, rows: list[dict]):
    file_exists = os.path.exists(output_path)
    with write_lock:
        with open(output_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to cleaned CSV (agent1 output or any CSV)")
    parser.add_argument("--icp", required=True, help='Target ICP, e.g. "managed IT services"')
    parser.add_argument("--output", default="", help="Output path (default: output/<name>_enriched_<ts>.csv)")
    parser.add_argument("--test", action="store_true", help="Process first 20 rows only")
    args = parser.parse_args()

    if not DEEPSEEK_API_KEY:
        sys.exit("[ERROR] DEEPSEEK_API_KEY not set in .env")

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

    print(f"Loaded {len(rows)} rows | ICP target: '{args.icp}'")

    # Detect columns
    col_company = find_col(fieldnames, COMPANY_COLS)
    col_website = find_col(fieldnames, WEBSITE_COLS)
    col_email = find_col(fieldnames, EMAIL_COLS)
    col_first = find_col(fieldnames, FIRST_COLS)
    col_last = find_col(fieldnames, LAST_COLS)
    col_title = find_col(fieldnames, TITLE_COLS)

    print(f"Columns — company:{col_company} website:{col_website} email:{col_email} "
          f"first:{col_first} last:{col_last} title:{col_title}")

    if not col_website and not col_company:
        sys.exit("[ERROR] Cannot find a company or website column.")

    emails_in_input = bool(col_email and any(r.get(col_email, "").strip() for r in rows))
    print(f"Emails already in input: {emails_in_input}")

    # ICP keyword tokens for pre-filter
    icp_tokens = [t.lower() for t in args.icp.split()]

    # Stamp internal columns
    for r in rows:
        r["_col_company"] = r.get(col_company, "").strip() if col_company else ""
        r["_col_website"] = r.get(col_website, "").strip() if col_website else ""
        r["_col_email"] = r.get(col_email, "").strip() if col_email else ""
        r["_col_first"] = r.get(col_first, "").strip() if col_first else ""
        r["_col_last"] = r.get(col_last, "").strip() if col_last else ""
        r["_col_title"] = r.get(col_title, "").strip() if col_title else ""
        r["_domain"] = normalise_domain(r["_col_website"] or r["_col_company"])

    # Group by domain (so we scrape + LLM once per company)
    domain_groups: dict[str, list[dict]] = defaultdict(list)
    no_domain = []
    for r in rows:
        if r["_domain"]:
            domain_groups[r["_domain"]].append(r)
        else:
            no_domain.append(r)

    print(f"Unique domains: {len(domain_groups)} | No domain: {len(no_domain)}")

    # Build output path
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    if args.output:
        out_path = args.output if os.path.isabs(args.output) else os.path.join(BASE_DIR, args.output)
    else:
        base = re.sub(r"\.(csv)$", "", os.path.basename(input_path), flags=re.IGNORECASE)
        base = re.sub(r"_cleaned_\d{4}-\d{2}-\d{2}.*$", "", base)
        out_path = os.path.join(OUTPUT_DIR, f"{base}_enriched_{ts}.csv")

    # Resumability: skip domains already in output
    done_domains = load_done_domains(out_path)
    if done_domains:
        print(f"Resuming — skipping {len(done_domains)} already-processed domains")

    pending = [(d, g) for d, g in domain_groups.items() if d not in done_domains]
    print(f"Domains to process: {len(pending)}")

    # Write header if new file
    if not os.path.exists(out_path):
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUTPUT_FIELDS).writeheader()

    processed = [0]
    stats = {"yes": 0, "uncertain": 0, "no": 0, "total_written": 0}
    total = len(domain_groups)

    def run(item):
        idx, (domain, contacts) = item
        return process_domain(domain, contacts, args.icp, icp_tokens,
                              emails_in_input, idx, total)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(run, (i + 1, item)): item[0]
                   for i, item in enumerate(pending)}

        for fut in as_completed(futures):
            try:
                out_rows = fut.result()
            except Exception as e:
                print(f"  [worker error] {e}")
                continue

            append_rows(out_path, out_rows)

            for r in out_rows:
                match = r.get("icp_match", "no")
                stats[match] = stats.get(match, 0) + 1
                if match in ("yes", "uncertain"):
                    stats["total_written"] += 1

            processed[0] += 1
            if processed[0] % CHECKPOINT_EVERY == 0:
                print(f"\n  Checkpoint: {processed[0]}/{len(pending)} domains processed "
                      f"| yes:{stats['yes']} uncertain:{stats['uncertain']} no:{stats['no']}\n")

    # Quality gate
    email_count = 0
    qualified_count = 0
    try:
        with open(out_path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("icp_match") in ("yes", "uncertain"):
                    qualified_count += 1
                    if r.get("email", "").strip():
                        email_count += 1
    except Exception:
        pass

    if qualified_count == 0:
        print("\n[WARN] No qualified rows in output. Check --icp value and input data.")
    if qualified_count > 0 and email_count / qualified_count < 0.20:
        print(f"\n[WARN] Email rate {email_count/qualified_count:.0%} is below 20%. "
              "Consider adding Exa key or checking website accessibility.")

    print(f"\n{'='*65}")
    print(f"Output:           {out_path}")
    print(f"Domains processed:{processed[0]}")
    print(f"  ICP yes:        {stats.get('yes', 0)}")
    print(f"  ICP uncertain:  {stats.get('uncertain', 0)}")
    print(f"  ICP no:         {stats.get('no', 0)}")
    print(f"  Emails found:   {email_count} / {qualified_count} qualified")
    print(f"{'='*65}")

    # Sample output
    print("\nSample (first 5 qualified rows):")
    shown = 0
    try:
        with open(out_path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("icp_match") in ("yes", "uncertain") and shown < 5:
                    print(f"  {r.get('first_name'):12} {r.get('company_name', '')[:35]:35} "
                          f"| {r.get('niche', '')[:30]:30} | email:{bool(r.get('email'))}")
                    shown += 1
    except Exception:
        pass


if __name__ == "__main__":
    main()
