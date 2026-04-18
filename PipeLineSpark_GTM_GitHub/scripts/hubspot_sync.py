"""
hubspot_sync.py
---------------
Reads the enriched GTM email CSV and syncs every contact into HubSpot:
  - Creates or updates a Contact record
  - Creates or updates a Company record and associates it
  - Creates a Deal in the "GTM Job Hunt" pipeline at stage "Emailed"
  - Stores tier, variant used, campaign name, and email copy as custom properties

Usage:
  python scripts/hubspot_sync.py --input output/gtm_emails_<timestamp>.csv --variant A --campaign "GTM Engineer Outreach"

Run with --dry-run to preview without writing anything to HubSpot.
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HUBSPOT_TOKEN = os.getenv("HUBSPOT_API_KEY")
if not HUBSPOT_TOKEN:
    sys.exit("ERROR: HUBSPOT_API_KEY not set in .env")

BASE_URL = "https://api.hubapi.com"
HEADERS  = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
LOG_FILE = os.path.join(LOG_DIR, f"hubspot_sync_{ts}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

write_lock = Lock()
MAX_WORKERS = 5  # Conservative — HubSpot rate limit is 100 req/10s

# ---------------------------------------------------------------------------
# HubSpot API helpers
# ---------------------------------------------------------------------------
def hs_get(path: str, params: dict = None) -> dict | None:
    resp = requests.get(f"{BASE_URL}{path}", headers=HEADERS, params=params, timeout=15)
    if resp.status_code == 200:
        return resp.json()
    return None

def hs_post(path: str, body: dict) -> dict | None:
    resp = requests.post(f"{BASE_URL}{path}", headers=HEADERS, json=body, timeout=15)
    if resp.status_code in (200, 201):
        return resp.json()
    log.warning(f"POST {path} failed {resp.status_code}: {resp.text[:200]}")
    return None

def hs_patch(path: str, body: dict) -> dict | None:
    resp = requests.patch(f"{BASE_URL}{path}", headers=HEADERS, json=body, timeout=15)
    if resp.status_code == 200:
        return resp.json()
    log.warning(f"PATCH {path} failed {resp.status_code}: {resp.text[:200]}")
    return None

def hs_put(path: str, body) -> dict | None:
    resp = requests.put(f"{BASE_URL}{path}", headers=HEADERS, json=body, timeout=15)
    if resp.status_code in (200, 201):
        return resp.json()
    log.warning(f"PUT {path} failed {resp.status_code}: {resp.text[:200]}")
    return None

# ---------------------------------------------------------------------------
# Ensure custom properties exist on Contact object
# ---------------------------------------------------------------------------
CUSTOM_CONTACT_PROPS = [
    {"name": "gtm_tier",          "label": "GTM Tier",          "type": "string",   "fieldType": "text"},
    {"name": "gtm_variant_used",  "label": "Variant Used",       "type": "string",   "fieldType": "text"},
    {"name": "gtm_campaign",      "label": "GTM Campaign",       "type": "string",   "fieldType": "text"},
    {"name": "gtm_email_copy",    "label": "Email Copy Sent",    "type": "string",   "fieldType": "textarea"},
    {"name": "gtm_enrichment",    "label": "Enrichment Notes",   "type": "string",   "fieldType": "textarea"},
]

def ensure_custom_properties():
    """Create custom contact properties if they don't already exist."""
    existing = hs_get("/crm/v3/properties/contacts")
    existing_names = {p["name"] for p in existing.get("results", [])} if existing else set()

    for prop in CUSTOM_CONTACT_PROPS:
        if prop["name"] not in existing_names:
            body = {
                "name": prop["name"],
                "label": prop["label"],
                "type": prop["type"],
                "fieldType": prop["fieldType"],
                "groupName": "contactinformation",
            }
            result = hs_post("/crm/v3/properties/contacts", body)
            if result:
                log.info(f"  Created property: {prop['name']}")
            else:
                log.warning(f"  Could not create property: {prop['name']}")

# ---------------------------------------------------------------------------
# Pipeline setup — get or create "GTM Job Hunt" pipeline
# ---------------------------------------------------------------------------
def get_or_create_pipeline() -> tuple[str, dict]:
    """
    Returns (pipeline_id, {stage_name: stage_id}).
    Free HubSpot only allows 1 pipeline, so we use the existing default
    'Sales Pipeline' and add any missing GTM stages to it.
    """
    DESIRED_STAGES = [
        {"label": "Emailed",       "metadata": {"probability": "0.1"}},
        {"label": "Opened",        "metadata": {"probability": "0.2"}},
        {"label": "Replied",       "metadata": {"probability": "0.4"}},
        {"label": "Call Booked",   "metadata": {"probability": "0.6"}},
        {"label": "Interviewing",  "metadata": {"probability": "0.8"}},
        {"label": "Offer",         "metadata": {"probability": "0.9"}},
        {"label": "Hired",         "metadata": {"probability": "1.0"}},
        {"label": "No Response",   "metadata": {"probability": "0.0"}},
        {"label": "Not Interested","metadata": {"probability": "0.0"}},
    ]

    pipelines = hs_get("/crm/v3/pipelines/deals")
    if not pipelines or not pipelines.get("results"):
        sys.exit("ERROR: Could not fetch HubSpot pipelines")

    # Use the first (and likely only) pipeline
    pipeline = pipelines["results"][0]
    pipeline_id = pipeline["id"]
    existing_stages = {s["label"]: s["id"] for s in pipeline.get("stages", [])}
    log.info(f"Using pipeline: {pipeline['label']} (id={pipeline_id})")

    # Add any missing stages
    for i, stage in enumerate(DESIRED_STAGES):
        if stage["label"] not in existing_stages:
            body = {
                "label":        stage["label"],
                "displayOrder": 100 + i,  # append after existing stages
                "metadata":     stage["metadata"],
            }
            result = hs_post(f"/crm/v3/pipelines/deals/{pipeline_id}/stages", body)
            if result:
                existing_stages[stage["label"]] = result["id"]
                log.info(f"  Added stage: {stage['label']} (id={result['id']})")
            else:
                log.warning(f"  Could not add stage: {stage['label']}")

    return pipeline_id, existing_stages

# ---------------------------------------------------------------------------
# Contact — find by email or create
# ---------------------------------------------------------------------------
def upsert_contact(row: dict, variant_copy: str, variant: str, campaign: str) -> str | None:
    email = row["email"].strip()
    if not email:
        return None

    # Search for existing contact
    search = hs_post("/crm/v3/objects/contacts/search", {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "properties": ["email", "firstname", "lastname"],
        "limit": 1,
    })
    existing = search.get("results", []) if search else []

    props = {
        "firstname":        row["first_name"],
        "lastname":         row["last_name"],
        "email":            email,
        "jobtitle":         row["title"],
        "company":          row["company"],
        "website":          row["website"],
        "gtm_tier":         row["tier"],
        "gtm_variant_used": variant,
        "gtm_campaign":     campaign,
        "gtm_email_copy":   variant_copy[:65000],  # HubSpot textarea limit
        "gtm_enrichment":   row["enrichment"][:65000],
    }

    if existing:
        contact_id = existing[0]["id"]
        hs_patch(f"/crm/v3/objects/contacts/{contact_id}", {"properties": props})
        return contact_id
    else:
        result = hs_post("/crm/v3/objects/contacts", {"properties": props})
        return result["id"] if result else None

# ---------------------------------------------------------------------------
# Company — find by domain or create
# ---------------------------------------------------------------------------
def upsert_company(row: dict) -> str | None:
    website = row["website"].strip()
    company_name = row["company"].strip()
    if not company_name:
        return None

    # Normalise domain for search
    domain = website.replace("https://", "").replace("http://", "").replace("www.", "").strip("/").split("/")[0]

    if domain:
        search = hs_post("/crm/v3/objects/companies/search", {
            "filterGroups": [{"filters": [{"propertyName": "domain", "operator": "EQ", "value": domain}]}],
            "properties": ["name", "domain"],
            "limit": 1,
        })
        existing = search.get("results", []) if search else []
    else:
        existing = []

    props = {
        "name":    company_name,
        "domain":  domain,
        "website": website,
    }

    if existing:
        company_id = existing[0]["id"]
        hs_patch(f"/crm/v3/objects/companies/{company_id}", {"properties": props})
        return company_id
    else:
        result = hs_post("/crm/v3/objects/companies", {"properties": props})
        return result["id"] if result else None

# ---------------------------------------------------------------------------
# Associate contact with company
# ---------------------------------------------------------------------------
def associate_contact_company(contact_id: str, company_id: str):
    hs_put(
        f"/crm/v4/objects/contacts/{contact_id}/associations/companies/{company_id}",
        [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 1}],
    )

# ---------------------------------------------------------------------------
# Deal — create one per contact at "Emailed" stage
# ---------------------------------------------------------------------------
def create_deal(row: dict, contact_id: str, company_id: str,
                pipeline_id: str, stage_id: str, campaign: str) -> str | None:
    deal_name = f"{row['first_name']} {row['last_name']} — {row['company']} ({campaign})"
    props = {
        "dealname":   deal_name,
        "pipeline":   pipeline_id,
        "dealstage":  stage_id,
        "dealtype":   "newbusiness",
    }
    result = hs_post("/crm/v3/objects/deals", {"properties": props})
    if not result:
        return None
    deal_id = result["id"]

    # Associate deal with contact
    hs_put(
        f"/crm/v4/objects/deals/{deal_id}/associations/contacts/{contact_id}",
        [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}],
    )
    # Associate deal with company
    if company_id:
        hs_put(
            f"/crm/v4/objects/deals/{deal_id}/associations/companies/{company_id}",
            [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 5}],
        )
    return deal_id

# ---------------------------------------------------------------------------
# Process one row
# ---------------------------------------------------------------------------
def process_row(idx: int, total: int, row: dict,
                variant: str, campaign: str,
                pipeline_id: str, stage_id: str,
                dry_run: bool) -> dict:

    first   = row["first_name"]
    last    = row["last_name"]
    company = row["company"]
    email   = row["email"].strip()

    variant_copy = row.get(f"variant_{variant.lower()}", "")

    log.info(f"[{idx}/{total}] {first} {last} | {company}")

    if not email:
        return {"status": "skipped", "reason": "no email", **row}

    if dry_run:
        return {"status": "dry_run", **row}

    # Upsert contact
    contact_id = upsert_contact(row, variant_copy, variant, campaign)
    if not contact_id:
        return {"status": "error: contact upsert failed", **row}

    # Upsert company
    company_id = upsert_company(row)
    if company_id:
        associate_contact_company(contact_id, company_id)

    # Create deal
    deal_id = create_deal(row, contact_id, company_id, pipeline_id, stage_id, campaign)

    return {
        "status":     "ok",
        "contact_id": contact_id,
        "company_id": company_id or "",
        "deal_id":    deal_id or "",
        **row,
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    required=True,  help="Path to enriched GTM email CSV")
    parser.add_argument("--variant",  required=True,  choices=["A","B","C"], help="Which email variant to use (A, B, or C)")
    parser.add_argument("--campaign", required=True,  help="Campaign name (used in HubSpot deal names)")
    parser.add_argument("--dry-run",  action="store_true", help="Preview without writing to HubSpot")
    args = parser.parse_args()

    log.info(f"Variant: {args.variant} | Campaign: {args.campaign} | Dry run: {args.dry_run}")

    # Read CSV
    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    total = len(rows)
    log.info(f"Loaded {total} contacts")

    if not args.dry_run:
        log.info("Setting up HubSpot custom properties...")
        ensure_custom_properties()

        log.info("Setting up GTM Job Hunt pipeline...")
        pipeline_id, stages = get_or_create_pipeline()
        stage_id = stages.get("Emailed")
        if not stage_id:
            sys.exit("ERROR: Could not find 'Emailed' stage in pipeline")
        log.info(f"Pipeline ready: {pipeline_id} | Emailed stage: {stage_id}")
    else:
        pipeline_id = stage_id = "dry_run"

    # Filter to rows with emails only (warn on skips)
    with_email    = [r for r in rows if r.get("email","").strip()]
    without_email = [r for r in rows if not r.get("email","").strip()]
    log.info(f"With email: {len(with_email)} | Without email (will skip): {len(without_email)}")


    # Preview before proceeding
    print(f"\n{'='*60}")
    print(f"ABOUT TO SYNC TO HUBSPOT")
    print(f"  Campaign:    {args.campaign}")
    print(f"  Variant:     {args.variant}")
    print(f"  Contacts:    {len(with_email)}")
    print(f"  Dry run:     {args.dry_run}")
    print(f"{'='*60}")
    confirm = input("\nType YES to proceed: ").strip()
    if confirm != "YES":
        print("Aborted.")
        sys.exit(0)

    ok = errors = skipped = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                process_row, i+1, total, row,
                args.variant, args.campaign,
                pipeline_id, stage_id, args.dry_run
            ): row
            for i, row in enumerate(rows)
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                status = result.get("status", "")
                with write_lock:
                    if status == "ok" or status == "dry_run":
                        ok += 1
                    elif status == "skipped":
                        skipped += 1
                    else:
                        errors += 1
            except Exception as e:
                log.error(f"Thread error: {e}")
                errors += 1
            # Gentle rate limiting — HubSpot allows 100 req/10s
            time.sleep(0.1)

    log.info("=" * 60)
    log.info(f"Done. OK: {ok} | Skipped (no email): {skipped} | Errors: {errors}")
    log.info(f"Log: {LOG_FILE}")

if __name__ == "__main__":
    main()
