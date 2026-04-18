"""
regen_keywords.py
-----------------
Takes an existing niches CSV with broad/few keywords and regenerates 15 tight
keywords per niche via DeepSeek, then rebuilds the Apollo URLs.

Run:
  python scripts/regen_keywords.py --input output/niches_2026-04-17T06-24-40Z.csv
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import quote

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

DEEPSEEK_API_KEY  = os.getenv("DEEPSEEK_API_KEY")
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_GID  = int(os.getenv("GOOGLE_SHEET_GID", "0"))
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com/v1")

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


def regen_keywords(niche_name: str) -> list[str]:
    prompt = f"""You are a B2B outbound sales strategist helping build Apollo.io search filters.

Generate exactly 15 keyword phrases for this specific niche: "{niche_name}"

RULES:
- Every keyword must describe THE EXACT SAME business type — different ways companies in this niche label themselves
- Mix of 1-4 word phrases — single words are allowed ONLY if they are the industry's actual common name (e.g. "MSP", "HVAC", "CPA") not generic words like "service" or "consulting" alone
- Stay within the niche — no crossing into adjacent industries
- Prioritise terms companies actually use on their LinkedIn profiles and Apollo tags: common abbreviations, industry nicknames, how clients google them, how trade associations describe them
- Include a mix of specific phrases AND the industry shorthand that practitioners actually use

Good example for "commercial HVAC contractors":
["commercial HVAC", "HVAC contractor", "HVAC contracting", "mechanical contractor", "commercial heating and cooling", "HVAC installation", "HVAC service company", "commercial air conditioning", "HVAC repair", "rooftop unit service", "commercial refrigeration", "ductwork contractor", "HVAC mechanical", "building HVAC", "HVAC systems contractor"]

IMPORTANT: Use only straight double quotes. No apostrophes.

Respond ONLY with valid JSON, no markdown:
{{"keywords": ["kw1","kw2","kw3","kw4","kw5","kw6","kw7","kw8","kw9","kw10","kw11","kw12","kw13","kw14","kw15"]}}"""

    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=1000,
    )
    content = resp.choices[0].message.content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```\w*\n?", "", content).rstrip("`").strip()
    try:
        return json.loads(content).get("keywords", [])
    except Exception:
        print(f"  [WARN] Could not parse keywords for '{niche_name}'")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CSV to regenerate keywords for")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"[ERROR] File not found: {args.input}")

    with open(args.input, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows)} niches from {args.input}")
    print("Regenerating 15 tight keywords per niche via DeepSeek...\n")

    for i, row in enumerate(rows, 1):
        niche = row["niche"]
        print(f"  [{i}/{len(rows)}] {niche} ... ", end="", flush=True)
        keywords = regen_keywords(niche)
        if keywords:
            row["keywords"] = ", ".join(keywords)
            row["apollo_url"] = build_apollo_url(keywords)
            print(f"{len(keywords)} keywords")
        else:
            print("skipped")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_path = os.path.join(OUTPUT_DIR, f"niches_regen_{ts}.csv")
    fields = [f for f in rows[0].keys() if f != "contact_count"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved to: {out_path}")

    # Write to Google Sheets
    creds_path = os.path.join(BASE_DIR, GOOGLE_CREDS_FILE) if GOOGLE_CREDS_FILE else None
    if creds_path and os.path.exists(creds_path) and GOOGLE_SHEET_ID:
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(GOOGLE_SHEET_ID)
            # Find tab by gid
            worksheet = next((ws for ws in sh.worksheets() if ws.id == GOOGLE_SHEET_GID), sh.sheet1)
            worksheet.clear()
            header = ["viability_score", "niche", "keywords", "icp", "why_high_ticket", "pain_point", "apollo_url"]
            data = [header] + [[str(row.get(f, "")) for f in header] for row in rows]
            worksheet.update(data)
            print(f"Written to Google Sheets tab: {worksheet.title}")
        except Exception as e:
            print(f"[WARN] Could not write to Google Sheets: {e}")
    else:
        print("[WARN] Google Sheets not configured — CSV only")

    print("\nNow TAM-check it:")
    print(f"  python scripts/niche_finder.py --verify-only {out_path} --merge-into output/niches_2026-04-17T19-55-20Z.csv")


if __name__ == "__main__":
    main()
