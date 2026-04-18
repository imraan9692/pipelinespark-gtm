"""
instantly_hubspot_sync.py
--------------------------
Polls ALL campaigns in your Instantly account, checks lead statuses,
and updates the matching HubSpot deal stage automatically.

Run this on a schedule (e.g. every 30 minutes via Task Scheduler or cron).

Stage mapping:
  Instantly status       → HubSpot deal stage
  ──────────────────────────────────────────
  email_opened           → Opened
  replied                → Replied
  bounced                → No Response
  unsubscribed           → Not Interested
  interested             → Call Booked
  meeting_booked         → Call Booked
  not_interested         → Not Interested
  out_of_office          → Opened (still alive, just OOO)

Usage:
  python scripts/instantly_hubspot_sync.py
  python scripts/instantly_hubspot_sync.py --dry-run
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

INSTANTLY_KEY = os.getenv("INSTANTLY_API_KEY")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_API_KEY")

if not INSTANTLY_KEY:
    sys.exit("ERROR: INSTANTLY_API_KEY not set in .env")
if not HUBSPOT_TOKEN:
    sys.exit("ERROR: HUBSPOT_API_KEY not set in .env")

INSTANTLY_BASE = "https://api.instantly.ai/api/v1"
HUBSPOT_BASE   = "https://api.hubapi.com"

HS_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
LOG_FILE = os.path.join(LOG_DIR, f"instantly_hubspot_sync_{ts}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Instantly status → HubSpot stage label mapping
# ---------------------------------------------------------------------------
STATUS_TO_STAGE = {
    "email_opened":    "Opened",
    "opened":          "Opened",
    "replied":         "Replied",
    "bounced":         "No Response",
    "unsubscribed":    "Not Interested",
    "interested":      "Call Booked",
    "meeting_booked":  "Call Booked",
    "not_interested":  "Not Interested",
    "out_of_office":   "Opened",
}

# Stage priority — higher number = further in pipeline, never move backwards
STAGE_PRIORITY = {
    "Emailed":       1,
    "Opened":        2,
    "Replied":       3,
    "Call Booked":   4,
    "Interviewing":  5,
    "Offer":         6,
    "Hired":         7,
    "No Response":   0,
    "Not Interested": 0,
}

# ---------------------------------------------------------------------------
# Instantly API helpers
# ---------------------------------------------------------------------------
def instantly_get(path: str, params: dict = None) -> dict | None:
    params = params or {}
    params["api_key"] = INSTANTLY_KEY
    resp = requests.get(f"{INSTANTLY_BASE}{path}", params=params, timeout=15)
    if resp.status_code == 200:
        return resp.json()
    log.warning(f"Instantly GET {path} failed {resp.status_code}: {resp.text[:200]}")
    return None

def get_all_campaigns() -> list:
    result = instantly_get("/campaign/list", {"limit": 100, "skip": 0})
    if not result:
        return []
    campaigns = result if isinstance(result, list) else result.get("campaigns", result.get("data", []))
    log.info(f"Found {len(campaigns)} campaigns in Instantly")
    return campaigns

def get_campaign_leads(campaign_id: str) -> list:
    """Fetch all leads for a campaign with pagination."""
    leads = []
    skip = 0
    limit = 100
    while True:
        result = instantly_get("/lead/list", {
            "campaign_id": campaign_id,
            "limit": limit,
            "skip": skip,
        })
        if not result:
            break
        batch = result if isinstance(result, list) else result.get("leads", result.get("data", []))
        if not batch:
            break
        leads.extend(batch)
        if len(batch) < limit:
            break
        skip += limit
    return leads

# ---------------------------------------------------------------------------
# HubSpot API helpers
# ---------------------------------------------------------------------------
def hs_post(path: str, body: dict) -> dict | None:
    resp = requests.post(f"{HUBSPOT_BASE}{path}", headers=HS_HEADERS, json=body, timeout=15)
    if resp.status_code in (200, 201):
        return resp.json()
    log.warning(f"HubSpot POST {path} failed {resp.status_code}: {resp.text[:200]}")
    return None

def hs_patch(path: str, body: dict) -> dict | None:
    resp = requests.patch(f"{HUBSPOT_BASE}{path}", headers=HS_HEADERS, json=body, timeout=15)
    if resp.status_code == 200:
        return resp.json()
    log.warning(f"HubSpot PATCH {path} failed {resp.status_code}: {resp.text[:200]}")
    return None

def get_pipeline_stages() -> dict:
    """Returns {stage_label: stage_id} from the first pipeline."""
    resp = requests.get(f"{HUBSPOT_BASE}/crm/v3/pipelines/deals", headers=HS_HEADERS, timeout=15)
    if resp.status_code != 200:
        return {}
    pipelines = resp.json().get("results", [])
    if not pipelines:
        return {}
    return {s["label"]: s["id"] for s in pipelines[0].get("stages", [])}

def find_deal_by_email(email: str) -> dict | None:
    """Find a HubSpot deal associated with a contact by email."""
    # First find the contact
    search = hs_post("/crm/v3/objects/contacts/search", {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "properties": ["email"],
        "limit": 1,
    })
    if not search or not search.get("results"):
        return None
    contact_id = search["results"][0]["id"]

    # Get associated deals
    resp = requests.get(
        f"{HUBSPOT_BASE}/crm/v4/objects/contacts/{contact_id}/associations/deals",
        headers=HS_HEADERS,
        timeout=15,
    )
    if resp.status_code != 200 or not resp.json().get("results"):
        return None

    deal_id = resp.json()["results"][0]["toObjectId"]

    # Get the deal's current stage
    deal_resp = requests.get(
        f"{HUBSPOT_BASE}/crm/v3/objects/deals/{deal_id}?properties=dealstage,dealname",
        headers=HS_HEADERS,
        timeout=15,
    )
    if deal_resp.status_code == 200:
        return {"id": deal_id, **deal_resp.json().get("properties", {})}
    return None

def update_deal_stage(deal_id: str, stage_id: str, stage_label: str, email: str, dry_run: bool):
    if dry_run:
        log.info(f"  [DRY RUN] Would update deal {deal_id} → {stage_label}")
        return
    result = hs_patch(f"/crm/v3/objects/deals/{deal_id}", {"properties": {"dealstage": stage_id}})
    if result:
        log.info(f"  Updated deal {deal_id} ({email}) → {stage_label}")
    else:
        log.warning(f"  Failed to update deal {deal_id} ({email})")

# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------
def sync(dry_run: bool):
    log.info(f"Starting Instantly → HubSpot sync | Dry run: {dry_run}")

    # Load pipeline stages once
    stages = get_pipeline_stages()
    if not stages:
        sys.exit("ERROR: Could not load HubSpot pipeline stages")
    log.info(f"Pipeline stages loaded: {list(stages.keys())}")

    campaigns = get_all_campaigns()
    if not campaigns:
        log.warning("No campaigns found in Instantly")
        return

    total_updated = 0
    total_skipped = 0
    total_no_deal = 0

    for campaign in campaigns:
        campaign_id   = campaign.get("id", campaign.get("campaign_id", ""))
        campaign_name = campaign.get("name", campaign.get("campaign_name", campaign_id))
        log.info(f"\nCampaign: {campaign_name} ({campaign_id})")

        leads = get_campaign_leads(campaign_id)
        log.info(f"  {len(leads)} leads")

        for lead in leads:
            email  = lead.get("email", "").strip().lower()
            status = lead.get("status", lead.get("lead_status", "")).lower().replace(" ", "_")

            if not email or not status:
                continue

            # Map Instantly status to HubSpot stage label
            target_stage_label = STATUS_TO_STAGE.get(status)
            if not target_stage_label:
                continue  # Status we don't track (e.g. "sending", "pending")

            target_stage_id = stages.get(target_stage_label)
            if not target_stage_id:
                log.warning(f"  Stage '{target_stage_label}' not found in HubSpot pipeline")
                continue

            # Find the deal in HubSpot
            deal = find_deal_by_email(email)
            if not deal:
                total_no_deal += 1
                continue  # Contact not in HubSpot yet

            deal_id = deal["id"]
            current_stage_id = deal.get("dealstage", "")

            # Get current stage label for priority check
            current_stage_label = next(
                (label for label, sid in stages.items() if sid == current_stage_id), ""
            )
            current_priority = STAGE_PRIORITY.get(current_stage_label, 0)
            target_priority  = STAGE_PRIORITY.get(target_stage_label, 0)

            # Never move a deal backwards in the pipeline
            if target_priority <= current_priority and current_priority > 0:
                total_skipped += 1
                continue

            update_deal_stage(deal_id, target_stage_id, target_stage_label, email, dry_run)
            total_updated += 1

            # Respect HubSpot rate limit
            time.sleep(0.15)

    log.info("\n" + "=" * 60)
    log.info(f"Sync complete.")
    log.info(f"  Updated:        {total_updated}")
    log.info(f"  Already current:{total_skipped}")
    log.info(f"  No HubSpot deal:{total_no_deal}")
    log.info(f"  Log: {LOG_FILE}")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to HubSpot")
    args = parser.parse_args()
    sync(dry_run=args.dry_run)
