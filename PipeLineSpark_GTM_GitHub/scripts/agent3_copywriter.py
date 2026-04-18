"""
agent3_copywriter.py
---------------------
Generates cold email copy via DeepSeek, previews it, then pushes to Instantly.
Takes any enriched CSV (agent2 output) and optionally analyst findings from agent4.

What it does:
  1. Reads the enriched CSV and profiles the top niches and ICPs in the file
  2. Optionally reads analyst findings (--findings) to inform copy direction
  3. Calls DeepSeek to generate 3 campaign angles, each with opener + 2 follow-ups
  4. Shows a full preview with real lead variable fills
  5. Waits for your approval — nothing is pushed until you type YES
  6. Creates 3 paused campaigns in Instantly, sets sequences, pushes leads
  7. Quality gate: confirms >0 leads uploaded per campaign

Run:
  python scripts/agent3_copywriter.py --input output/enriched.csv --campaign-name "MSP Outbound"
  python scripts/agent3_copywriter.py --input output/enriched.csv --campaign-name "MSP Q2" --findings logs/analyst_findings.json
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv("C:/Users/imraa/Downloads/PipeLineSpark_GTM/.env")

INSTANTLY_API_KEY = os.getenv("INSTANTLY_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
INSTANTLY_BASE = "https://api.instantly.ai/api/v2"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com/v1")

# Words DeepSeek tends to use that are banned in CLAUDE.md
BANNED_WORDS = [
    "leverage", "streamline", "seamlessly", "unlock", "game-changer", "innovative",
    "cutting-edge", "tailored", "pain points", "value proposition", "strategic",
    "synergy", "scalable", "robust", "empower", "transform", "revolutionize",
    "comprehensive", "utilize", "facilitate", "optimize", "elevate", "reimagine",
    "I wanted to", "I noticed", "I hope this email finds you", "hope this finds you well",
    "I came across your profile",
]

BANNED_OPENERS = [
    "I wanted to", "I noticed", "I came across", "I hope this email",
    "hope this finds", "I came across your profile",
]


# ── Copy validation ────────────────────────────────────────────────────────────

def validate_copy(text: str) -> list[str]:
    """Returns list of issues found in the copy."""
    issues = []
    if " — " in text or "\u2014" in text:
        issues.append("em dash found (replace with period or comma)")
    for word in BANNED_WORDS:
        if word.lower() in text.lower():
            issues.append(f"banned word: '{word}'")
    for opener in BANNED_OPENERS:
        if text.lower().startswith(opener.lower()):
            issues.append(f"filler opener: '{opener}'")
    return issues


# ── Instantly API helpers ──────────────────────────────────────────────────────

def instantly_req(method: str, path: str, payload: dict = None) -> dict:
    headers = {"Authorization": f"Bearer {INSTANTLY_API_KEY}", "Content-Type": "application/json"}
    resp = requests.request(method, f"{INSTANTLY_BASE}{path}", json=payload,
                            headers=headers, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"  [API error] {e} — {resp.text[:300]}")
        raise
    return resp.json()


def create_campaign(name: str) -> str:
    data = instantly_req("POST", "/campaigns", {
        "name": name,
        "campaign_schedule": {
            "schedules": [{
                "name": "Default",
                "timing": {"from": "09:00", "to": "17:00"},
                "days": {"1": True, "2": True, "3": True, "4": True, "5": True},
                "timezone": "America/New_York",
            }]
        },
    })
    return data["id"]


def set_email_sequence(campaign_id: str, steps: list[dict]):
    instantly_req("POST", f"/campaigns/{campaign_id}/sequences", {
        "steps": [
            {
                "type": "email",
                "delay": step["delay"],
                "variants": [{"subject": step["subject"], "body": step["body"]}],
            }
            for step in steps
        ]
    })


def push_leads(campaign_id: str, rows: list[dict]) -> int:
    pushed = 0
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        leads = [{
            "email": r.get("email", ""),
            "first_name": r.get("first_name", ""),
            "last_name": r.get("last_name", ""),
            "company_name": r.get("company_name", ""),
            "website": r.get("website", ""),
            "niche": r.get("niche", ""),
            "business_icp": r.get("business_icp", ""),
        } for r in batch]
        try:
            instantly_req("POST", "/leads", {
                "campaign_id": campaign_id,
                "leads": leads,
                "skip_if_in_workspace": True,
            })
            pushed += len(batch)
            print(f"    Pushed batch {i // 100 + 1} ({len(batch)} leads)")
        except Exception as e:
            print(f"    [push error] {e}")
        time.sleep(0.5)
    return pushed


# ── Copy generation ────────────────────────────────────────────────────────────

def generate_campaigns(top_niches: list[str], top_icps: list[str],
                       analyst_findings: str) -> list[dict]:
    """
    Calls DeepSeek to write 3 campaign angles.
    Each angle: opener (step 1) + 2 follow-ups (steps 2 & 3).
    Returns list of campaign dicts matching CAMPAIGNS format.
    """
    niche_str = ", ".join(top_niches[:3]) or "companies in this space"
    icp_str = ", ".join(top_icps[:3]) or "business owners"
    findings_section = (
        f"\n\nANALYST FINDINGS (from previous campaigns — use these to inform angle and copy):\n{analyst_findings}"
        if analyst_findings else ""
    )

    prompt = f"""You are writing cold email sequences for a performance-based cold email agency called PipelineSpark.

TARGET AUDIENCE
Niches: {niche_str}
ICPs: {icp_str}
Offer: We run cold email systems that book 7 to 12 qualified meetings with {{{{business_icp}}}} onto your calendar in 45 days. Zero risk, you only pay for results.{findings_section}

TASK
Write 3 distinct campaign angles. Each angle has 3 steps:
- Step 1 (opener): initial email, delay 0 days
- Step 2 (follow-up 1): short bump, delay 3 days
- Step 3 (follow-up 2): final breakup, delay 5 days

VARIABLES (Instantly resolves these at send time — use them exactly as written):
{{{{firstName}}}}, {{{{companyName}}}}, {{{{niche}}}}, {{{{business_icp}}}}

RULES — these are strict, violations will be corrected:
- No em dashes (—). Use periods or commas instead.
- No AI words: leverage, streamline, seamlessly, unlock, game-changer, innovative, cutting-edge, tailored, pain points, value proposition, strategic, synergy, scalable, robust, empower, transform, revolutionize, comprehensive, utilize, facilitate
- No filler openers: "I wanted to", "I noticed", "I came across", "I hope this email finds you"
- Must sound like a real person wrote it in 2 minutes
- Keep each email to 4-6 lines max including greeting and CTA
- Follow-ups reference the opener briefly then add a new angle or question
- Step 3 is a clean breakup — no guilt-tripping, no begging
- Subject lines: short and lowercase. Step 2 and 3 subject is empty string (thread replies)

ANGLES TO WRITE (one per campaign):
A: Pain angle — the specific problem they face getting new clients
B: Curiosity/question angle — one genuine question about their current approach
C: Social proof / "others like you" angle — what similar companies are doing

Respond ONLY with valid JSON in this exact format:
{{
  "campaigns": [
    {{
      "label": "A - [angle name]",
      "steps": [
        {{"delay": 0, "subject": "...", "body": "..."}},
        {{"delay": 3, "subject": "", "body": "..."}},
        {{"delay": 5, "subject": "", "body": "..."}}
      ]
    }},
    {{
      "label": "B - [angle name]",
      "steps": [...]
    }},
    {{
      "label": "C - [angle name]",
      "steps": [...]
    }}
  ]
}}"""

    print("  Calling DeepSeek to write campaign copy ...")
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
        max_tokens=2000,
    )
    content = resp.choices[0].message.content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```\w*\n?", "", content).rstrip("```").strip()

    data = json.loads(content)
    return data["campaigns"]


def fix_copy_issues(campaigns: list[dict]) -> list[dict]:
    """Post-processing: replace em dashes, warn on banned words."""
    for c in campaigns:
        for step in c.get("steps", []):
            step["body"] = step["body"].replace(" — ", ", ").replace("—", ",")
            step["subject"] = step["subject"].replace(" — ", ", ").replace("—", ",")
    return campaigns


# ── Preview ────────────────────────────────────────────────────────────────────

def print_preview(campaigns: list[dict], samples: list[dict]):
    print("\n" + "=" * 70)
    print("COPY PREVIEW — all 3 campaigns")
    print("=" * 70)

    for c in campaigns:
        print(f"\n{'─'*70}")
        print(f"  {c['label']}")
        print(f"{'─'*70}")
        for i, step in enumerate(c["steps"], 1):
            label = "Opener" if i == 1 else f"Follow-up {i-1}"
            delay = step['delay']
            print(f"\n  Step {i} ({label}, +{delay}d)")
            if step["subject"]:
                print(f"  Subject: {step['subject']}")
            print()
            for line in step["body"].split("\n"):
                print(f"    {line}")

            # Check for copy issues
            issues = validate_copy(step["body"])
            if issues:
                print(f"\n  [COPY ISSUES]:")
                for issue in issues:
                    print(f"    - {issue}")

    print(f"\n{'─'*70}")
    print("  SAMPLE VARIABLE FILLS (first 5 leads)")
    print(f"{'─'*70}")
    for r in samples[:5]:
        print(f"  {r.get('first_name', '(no name)'):15} @ {r.get('company_name', '')[:35]:35}")
        print(f"    niche:        {r.get('niche', '')}")
        print(f"    business_icp: {r.get('business_icp', '')}")
        print()
    print("=" * 70)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Enriched CSV (agent2 output)")
    parser.add_argument("--campaign-name", required=True, help='Base name, e.g. "MSP Outbound"')
    parser.add_argument("--findings", default="", help="Path to analyst_findings.json (optional)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt (non-interactive)")
    args = parser.parse_args()

    if not INSTANTLY_API_KEY:
        sys.exit("[ERROR] INSTANTLY_API_KEY not set in .env")
    if not DEEPSEEK_API_KEY:
        sys.exit("[ERROR] DEEPSEEK_API_KEY not set in .env")

    # ── Load enriched CSV ──────────────────────────────────────────────────────
    input_path = args.input
    if not os.path.isabs(input_path):
        input_path = os.path.join(BASE_DIR, input_path)

    print(f"Loading {input_path} ...")
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))

    qualified = [
        r for r in all_rows
        if r.get("icp_match") in ("yes", "uncertain")
        and r.get("email", "").strip()
        and r.get("niche", "").strip()
    ]

    if not qualified:
        sys.exit("[ERROR] No qualified rows with email + niche found. Run agent2 enricher first.")

    print(f"Qualified leads (email + niche + icp_match yes/uncertain): {len(qualified)}")

    # ── Profile the list ───────────────────────────────────────────────────────
    niche_counts = Counter(r.get("niche", "").strip().lower() for r in qualified if r.get("niche"))
    icp_counts = Counter(r.get("business_icp", "").strip().lower() for r in qualified if r.get("business_icp"))
    top_niches = [n for n, _ in niche_counts.most_common(5)]
    top_icps = [i for i, _ in icp_counts.most_common(5)]

    print(f"\nList profile:")
    print(f"  Top niches: {', '.join(top_niches[:3])}")
    print(f"  Top ICPs:   {', '.join(top_icps[:3])}")

    # ── Load analyst findings (optional) ──────────────────────────────────────
    analyst_text = ""
    if args.findings:
        findings_path = args.findings
        if not os.path.isabs(findings_path):
            findings_path = os.path.join(BASE_DIR, findings_path)
        if os.path.exists(findings_path):
            with open(findings_path, encoding="utf-8") as f:
                findings_data = json.load(f)
            analyst_text = findings_data.get("analysis", "")
            auto_paused = findings_data.get("auto_paused", [])
            print(f"\nAnalyst findings loaded from {findings_path}")
            if auto_paused:
                print(f"  Previously auto-paused campaigns: {', '.join(auto_paused)}")
            if analyst_text:
                print(f"  Analysis summary: {analyst_text[:200]}...")
        else:
            print(f"[WARN] Findings file not found: {findings_path}")

    # ── Generate copy via DeepSeek ─────────────────────────────────────────────
    print("\nGenerating campaign copy ...")
    campaigns = generate_campaigns(top_niches, top_icps, analyst_text)
    campaigns = fix_copy_issues(campaigns)

    if len(campaigns) != 3:
        print(f"[WARN] Expected 3 campaigns, got {len(campaigns)}. Proceeding anyway.")

    # ── Preview ────────────────────────────────────────────────────────────────
    print_preview(campaigns, qualified)

    print(f"\nCampaign name prefix: {args.campaign_name}")
    print(f"Leads to push per campaign: {len(qualified)}")
    print(f"\nCampaigns will be created PAUSED. You activate manually in Instantly after reviewing.")

    # ── Confirmation ───────────────────────────────────────────────────────────
    if not args.yes:
        confirm = input(
            f"\nType YES to create {len(campaigns)} campaigns and push {len(qualified)} leads each: "
        ).strip()
        if confirm.upper() != "YES":
            print("Aborted.")
            sys.exit(0)

    # ── Push to Instantly ──────────────────────────────────────────────────────
    campaign_log = []
    total_pushed = 0

    for c in campaigns:
        name = f"{args.campaign_name} | {c['label']}"
        print(f"\n[{c['label']}] Creating: {name}")
        try:
            campaign_id = create_campaign(name)
            print(f"  Campaign ID: {campaign_id}")

            print("  Writing sequence ...")
            set_email_sequence(campaign_id, c["steps"])

            print(f"  Pushing {len(qualified)} leads ...")
            pushed = push_leads(campaign_id, qualified)
            total_pushed += pushed

            if pushed == 0:
                print(f"  [WARN] 0 leads pushed — check Instantly API or lead format")

            campaign_log.append({"name": name, "id": campaign_id, "leads_pushed": pushed})
            print(f"  Done — {pushed} leads pushed")

        except Exception as e:
            print(f"  [ERROR] {e}")
            campaign_log.append({"name": name, "id": None, "leads_pushed": 0, "error": str(e)})

    # ── Quality gate ───────────────────────────────────────────────────────────
    successful = [c for c in campaign_log if c.get("leads_pushed", 0) > 0]
    if not successful:
        print("\n[ERROR] No leads were successfully pushed to any campaign. Check API key and lead data.")
    else:
        print(f"\n[OK] {len(successful)}/{len(campaigns)} campaigns have leads.")

    # ── Write campaign log ─────────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    log_path = os.path.join(LOG_DIR, f"campaign_push_{ts}.json")
    log_data = {
        "pushed_at": ts,
        "campaign_name_prefix": args.campaign_name,
        "input_file": input_path,
        "total_leads_per_campaign": len(qualified),
        "campaigns": campaign_log,
        "copy": campaigns,
    }
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)

    print(f"\n{'='*65}")
    print(f"Campaign log: {log_path}")
    print(f"Campaigns created: {len(campaigns)}")
    for c in campaign_log:
        status = f"{c['leads_pushed']} leads" if c.get("leads_pushed") else "FAILED"
        print(f"  {c['name']}: {status}")
    print(f"\nAll campaigns are PAUSED. Review copy in Instantly then activate.")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
