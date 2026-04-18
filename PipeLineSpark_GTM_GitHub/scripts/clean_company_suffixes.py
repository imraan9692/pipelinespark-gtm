"""
clean_company_suffixes.py
--------------------------
Strips legal/corporate suffixes from company_name column.
Removes: LLC, Inc, Ltd, Corp, LLP, PLLC, PC, PLC, Co, Limited, Incorporated, etc.
Also strips trailing punctuation and whitespace.

Usage:
  python scripts/clean_company_suffixes.py --input output/legaltech_final_<ts>.csv
"""

import argparse
import csv
import os
import re
import sys
import io
from datetime import datetime, timezone

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Suffixes to strip — order matters, longer ones first
SUFFIXES = [
    "Incorporated", "Corporation", "International", "Limited Liability Company",
    "LLC", "LLP", "PLLC", "PLC", "Ltd", "Limited", "Corp", "Inc",
    "PC", "LP", "Co", "GmbH", "BV", "AG", "SA", "SAS", "Pty", "Pvt",
]

# Build a single regex: match any suffix at end of string, optionally preceded by comma/period/space
suffix_re = re.compile(
    r"[,.\s]+(?:" + "|".join(re.escape(s) for s in SUFFIXES) + r")\.?[,.\s]*$",
    re.IGNORECASE
)


def clean_name(name: str) -> str:
    original = name.strip()
    # Fix all-caps names (e.g. "ICIT SOLUTIONS" → "Icit Solutions")
    # Only title-case if at least one word is longer than 4 chars (avoids "GCS IT" → "Gcs It")
    result = original
    words = result.split()
    if result == result.upper() and any(len(w) > 4 for w in words):
        result = result.title()
    # Apply suffix stripping repeatedly until stable (handles "Corp, LLC")
    prev = None
    while result != prev:
        prev = result
        result = suffix_re.sub("", result).strip(" ,.")
    return result if result else original


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    with open(args.input, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(csv.DictReader(open(args.input, encoding="utf-8-sig")).fieldnames)

    print(f"Loaded {len(rows)} rows")

    changed = []
    for r in rows:
        original = r.get("company_name", "").strip()
        cleaned = clean_name(original)
        if cleaned != original:
            changed.append((original, cleaned))
            r["company_name"] = cleaned

    print(f"Names cleaned: {len(changed)}")
    print("\nSample changes:")
    for orig, clean in changed[:20]:
        print(f"  {orig!r:50} → {clean!r}")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    base = re.sub(r"_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z$", "",
                  os.path.splitext(os.path.basename(args.input))[0])
    out_path = os.path.join("output", f"{base}_{ts}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nOutput: {out_path}")


if __name__ == "__main__":
    main()
