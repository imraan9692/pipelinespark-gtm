"""
backfill_niche.py
-----------------
Regenerates the `niche` column on an existing enriched CSV as a 2-3 word
descriptor of what the company makes/does (e.g. "estate planning software").

Does NOT re-scrape anything — uses company_name + value_prop already in the CSV.
Batches 40 companies per DeepSeek call. Checkpoints every batch.

Usage:
  python scripts/backfill_niche.py --input output/legaltech_final_<ts>.csv
"""

import argparse
import csv
import json
import os
import re
import sys
import io

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(api_key=os.getenv("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com")

BATCH_SIZE = 40


def get_niche_batch(items: list[dict]) -> dict[str, str]:
    """
    items: list of {company_name, value_prop}
    Returns {company_name: niche}
    """
    numbered = "\n".join(
        f'{i+1}. {r["company_name"]} — {r["value_prop"]}'
        for i, r in enumerate(items)
    )

    prompt = f"""For each company, give me a 2 to 3 word niche that describes what the company makes or does.
It should be a short product or service descriptor. Not the buyer, not the industry.

Good examples: "contract management software", "legal billing tools", "estate planning software", "compliance automation", "legal research AI", "document automation software"

Companies:
{numbered}

Return ONLY a JSON object mapping number to niche: {{"1": "niche", "2": "niche", ...}}
No markdown. No explanation. Pure JSON only."""

    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1000,
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        result = json.loads(raw)
        return {items[int(k) - 1]["company_name"]: v for k, v in result.items()}
    except Exception as e:
        print(f"  [DeepSeek error] {e}")
        return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Enriched CSV to backfill")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"[ERROR] File not found: {args.input}")

    # Load all rows
    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows)} rows from {args.input}")

    # Cache file lives next to the input
    cache_path = os.path.join(os.path.dirname(args.input), ".niche_cache.json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            niche_cache = json.load(f)
        print(f"Cache loaded: {len(niche_cache)} companies already done")
    else:
        niche_cache = {}

    # Deduplicate companies that need processing
    seen = {}
    for r in rows:
        name = r.get("company_name", "").strip()
        vp = r.get("value_prop", "").strip()
        if name and vp and name not in niche_cache and name not in seen:
            seen[name] = {"company_name": name, "value_prop": vp}

    todo = list(seen.values())
    total_batches = (len(todo) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Companies to process: {len(todo)} ({total_batches} batches)")

    for i in range(0, len(todo), BATCH_SIZE):
        batch = todo[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches}...")
        result = get_niche_batch(batch)
        niche_cache.update(result)
        # Save checkpoint after every batch
        with open(cache_path, "w") as f:
            json.dump(niche_cache, f, indent=2)

    # Apply updated niches back to all rows
    updated = 0
    for r in rows:
        name = r.get("company_name", "").strip()
        if name in niche_cache:
            r["niche"] = niche_cache[name]
            updated += 1

    # Write output file with timestamp
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    base = os.path.splitext(os.path.basename(args.input))[0]
    # Strip any existing timestamp suffix and re-stamp
    base_clean = re.sub(r'_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z$', '', base)
    out_path = os.path.join("output", f"{base_clean}_{ts}.csv")

    fieldnames = list(rows[0].keys())
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. {updated} rows updated.")
    print(f"Output: {out_path}")

    # Show 5 random examples
    import random
    qualified = [r for r in rows if r.get("niche") and r.get("business_icp") and r.get("email")]
    sample = random.sample(qualified, min(5, len(qualified)))
    print("\n5 random examples:")
    print(f"{'company':<30} {'niche':<35} {'business_icp'}")
    print("-" * 90)
    for r in sample:
        print(f"{r['company_name']:<30} {r['niche']:<35} {r['business_icp']}")


if __name__ == "__main__":
    main()
