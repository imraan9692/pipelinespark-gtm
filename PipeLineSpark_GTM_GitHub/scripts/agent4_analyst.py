"""
agent4_analyst.py
------------------
Audits all Instantly campaigns, auto-pauses high-bounce ones, outputs findings for Agent 3.
- Pulls reply rate, bounce rate, opportunities per campaign
- Auto-pauses any active campaign exceeding 2% bounce rate
- Runs DeepSeek analysis on performance data
- Outputs logs/analyst_findings.json — feed into agent3_copywriter.py with --findings
- Run: python scripts/agent4_analyst.py
"""

import io
import json
import os
import sys
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

BOUNCE_THRESHOLD = 0.02  # auto-pause if bounce rate exceeds this

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com/v1")


# ── Instantly API helpers ──────────────────────────────────────────────────────

def instantly_req(method: str, path: str, payload: dict = None) -> dict:
    headers = {"Authorization": f"Bearer {INSTANTLY_API_KEY}", "Content-Type": "application/json"}
    resp = requests.request(method, f"{INSTANTLY_BASE}{path}", json=payload, headers=headers, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"  [API error] {e} — {resp.text}")
        raise
    return resp.json()


def list_campaigns() -> list:
    data = instantly_req("GET", "/campaigns?limit=100")
    return data.get("items", data) if isinstance(data, dict) else data


def get_analytics() -> list:
    data = instantly_req("GET", "/campaigns/analytics?limit=100")
    return data.get("items", data) if isinstance(data, dict) else data


def pause_campaign(campaign_id: str):
    instantly_req("POST", f"/campaigns/{campaign_id}/pause")


# ── Analysis ──────────────────────────────────────────────────────────────────

def run_deepseek_analysis(campaigns_summary: list) -> str:
    if not DEEPSEEK_API_KEY:
        return "No DeepSeek API key — skipping LLM analysis."

    summary_text = json.dumps(campaigns_summary, indent=2)
    prompt = (
        "You are a cold email performance analyst. "
        "Below is performance data for active outbound campaigns.\n\n"
        f"{summary_text}\n\n"
        "Analyse the data and provide:\n"
        "1. Which campaigns are performing well and why\n"
        "2. Which are underperforming and the likely cause (copy, targeting, timing)\n"
        "3. Specific copy or targeting changes to improve reply rates\n"
        "4. Recommended next campaign angle based on what's working\n\n"
        "Be direct and specific. Use plain language, no filler."
    )
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1000,
        temperature=0.3,
    )
    return resp.choices[0].message.content.strip()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not INSTANTLY_API_KEY:
        sys.exit("[ERROR] INSTANTLY_API_KEY not set in .env")

    print("Fetching campaigns ...")
    campaigns = list_campaigns()
    print(f"Found {len(campaigns)} campaigns")

    print("Fetching analytics ...")
    try:
        analytics = get_analytics()
    except Exception as e:
        print(f"[WARN] Could not fetch analytics: {e}")
        analytics = []

    # Index analytics by campaign_id
    analytics_by_id: dict[str, dict] = {}
    for a in analytics:
        cid = a.get("campaign_id") or a.get("id")
        if cid:
            analytics_by_id[cid] = a

    paused_campaigns = []
    campaigns_summary = []

    for c in campaigns:
        cid = c.get("id")
        name = c.get("name", "Unknown")
        status = c.get("status", "unknown")

        a = analytics_by_id.get(cid, {})
        sent = a.get("emails_sent", 0) or 0
        bounced = a.get("bounced", 0) or 0
        replied = a.get("replied", 0) or 0
        opened = a.get("opened", 0) or 0
        opportunities = a.get("opportunities", 0) or 0

        bounce_rate = bounced / sent if sent > 0 else 0
        reply_rate = replied / sent if sent > 0 else 0
        open_rate = opened / sent if sent > 0 else 0

        summary = {
            "id": cid,
            "name": name,
            "status": status,
            "sent": sent,
            "opened": opened,
            "replied": replied,
            "bounced": bounced,
            "opportunities": opportunities,
            "bounce_rate_pct": round(bounce_rate * 100, 2),
            "reply_rate_pct": round(reply_rate * 100, 2),
            "open_rate_pct": round(open_rate * 100, 2),
            "auto_paused": False,
        }

        # Auto-pause if bounce rate exceeds threshold and campaign is active
        if status == "active" and bounce_rate > BOUNCE_THRESHOLD and sent >= 50:
            print(f"  [AUTO-PAUSE] {name} — bounce rate {bounce_rate:.1%} > {BOUNCE_THRESHOLD:.0%}")
            try:
                pause_campaign(cid)
                summary["auto_paused"] = True
                paused_campaigns.append(name)
            except Exception as e:
                print(f"    [ERROR] Could not pause {name}: {e}")

        campaigns_summary.append(summary)

        flag = " *** AUTO-PAUSED" if summary["auto_paused"] else ""
        print(f"  {name[:50]:50} | sent:{sent:5} | reply:{reply_rate:.1%} | bounce:{bounce_rate:.1%} | opps:{opportunities}{flag}")

    # DeepSeek analysis
    print("\nRunning DeepSeek analysis ...")
    active_with_data = [c for c in campaigns_summary if c["sent"] > 0]
    analysis_text = run_deepseek_analysis(active_with_data) if active_with_data else "No campaigns with send data to analyse."
    print("\n--- Analysis ---")
    print(analysis_text)

    # Write findings JSON
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    findings = {
        "generated_at": ts,
        "campaigns": campaigns_summary,
        "auto_paused": paused_campaigns,
        "analysis": analysis_text,
    }
    findings_path = os.path.join(LOG_DIR, "analyst_findings.json")
    with open(findings_path, "w", encoding="utf-8") as f:
        json.dump(findings, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Findings written: {findings_path}")
    print(f"Campaigns audited: {len(campaigns_summary)}")
    print(f"Auto-paused: {len(paused_campaigns)}")
    if paused_campaigns:
        for n in paused_campaigns:
            print(f"  - {n}")
    print(f"{'='*60}")
    print(f"\nTo use findings in next campaign:")
    print(f"  python scripts/agent3_copywriter.py --input output/<file>.csv --campaign-name \"...\" --findings {findings_path}")


if __name__ == "__main__":
    main()
