# PipeLineSpark GTM Toolkit

An end-to-end outbound sales engine built in Python. Takes a raw lead list and turns it into a fully enriched, personalised, campaign-ready CSV — then pushes it live to Instantly and syncs to HubSpot. Everything runs from the command line with no manual steps between stages.

---

## What It Does

Most outbound teams spend hours manually pulling lists, researching companies, and writing copy. This toolkit automates the entire workflow — from raw Apollo export to live campaign — in under 30 minutes.


---

## Architecture

```
Raw CSV (Apollo / Clutch / Google Maps)
        │
        ▼
┌─────────────────┐
│  Agent 1        │  DNS validation, dedup, column normalisation
│  List Builder   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Agent 2        │  Website scraping → ICP classification → email finding
│  Enricher       │  → personalisation (value_prop, business_icp, niche)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Agent 3        │  Generates 2-step email sequence → previews copy
│  Copywriter     │  → pushes campaign + leads to Instantly
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Agent 4        │  Pulls campaign analytics → auto-pauses >2% bounce
│  Analyst        │  → outputs findings JSON for next campaign
└─────────────────┘
```

---

## Stack

| Layer | Tool |
|-------|------|
| LLM | DeepSeek (`deepseek-chat`) via OpenAI-compatible API |
| Web scraping | `requests` + `BeautifulSoup`, Exa contents API (fallback) |
| Browser automation | Selenium + SeleniumBase UC (Cloudflare bypass) |
| Outreach platform | Instantly API |
| CRM | HubSpot API |
| Lead sourcing | Apollo.io (export + Selenium TAM check) |
| Directory scraping | Clutch.co |
| Data | Google Sheets (service account), CSV |
| Language | Python 3.x |

---

## Scripts

### Core Pipeline

| Script | What it does |
|--------|-------------|
| `agent1_list_builder.py` | Cleans raw CSVs: DNS validation, dedup by domain, column normalisation, detects existing emails |
| `agent2_enricher.py` | Scrapes company websites, classifies ICP via DeepSeek, finds emails (regex → LLM → Exa fallback), generates personalised copy per row |
| `agent3_copywriter.py` | Generates 2-step email sequence with spintax, previews before push, creates Instantly campaign + uploads leads |
| `agent4_analyst.py` | Pulls Instantly analytics, auto-pauses campaigns above 2% bounce, outputs `logs/analyst_findings.json` |

### List Building

| Script | What it does |
|--------|-------------|
| `niche_finder.py` | Generates B2B niches via DeepSeek, builds Apollo search URLs, verifies TAM via Selenium (reads live Apollo contact count), filters by TAM threshold, writes to CSV + Google Sheets |
| `regen_keywords.py` | Takes an existing niches CSV and regenerates tighter keyword sets per niche via DeepSeek |
| `clutch_scraper.py` | Scrapes Clutch.co category directories with Cloudflare bypass, filters by employee count / min project size / US+Canada, checkpoints every 50 rows |

### Utilities

| Script | What it does |
|--------|-------------|
| `hubspot_sync.py` | Syncs enriched leads to HubSpot contacts |
| `instantly_hubspot_sync.py` | Pushes Instantly campaign replies into HubSpot as activities |
| `verify_websites.py` | Bulk website reachability check |
| `clean_company_suffixes.py` | Strips LLC, Inc, Ltd etc. from company names for copy use |
| `backfill_niche.py` | Fills missing niche column on existing enriched CSVs |

---

## Key Design Decisions

**Why DeepSeek over GPT-4?**
10x cheaper at similar quality for classification and copy tasks. Runs via the same OpenAI SDK — swap `base_url` to switch models.

**Why Selenium for Apollo TAM check?**
Apollo's people search endpoint returns 403 on free plans. Selenium reads the rendered "Total X" count directly from the page — same data, no API cost.

**Why SeleniumBase UC for Clutch?**
Clutch is behind Cloudflare. Standard Selenium gets blocked. SeleniumBase's undetected Chrome mode passes the challenge automatically.

**Resumable by default**
Every script uses the output file's existing rows as a skip list. Kill a run mid-way, restart it — it picks up where it left off.

---

## Setup

```bash
git clone https://github.com/yourusername/pipelinespark-gtm
cd pipelinespark-gtm

pip install -r requirements.txt

cp .env.example .env
# fill in your API keys
```

### Required API Keys

- **DeepSeek** — LLM for classification + copy ([platform.deepseek.com](https://platform.deepseek.com))
- **Instantly** — outreach platform ([instantly.ai](https://instantly.ai))
- **Exa** — fallback web content fetcher ([exa.ai](https://exa.ai))
- **HubSpot** — CRM sync (optional)
- **Apollo** — lead sourcing (export only, free plan works)
- **Google Service Account** — Sheets write-back (optional)

---

## Usage

```bash
# Full pipeline
python scripts/agent1_list_builder.py --input input/raw.csv
python scripts/agent2_enricher.py --input input/cleaned.csv --icp "commercial cleaning companies"
python scripts/agent3_copywriter.py --input output/enriched.csv --campaign-name "Cleaning Q2"
python scripts/agent4_analyst.py

# Find niches + build Apollo lists
python scripts/niche_finder.py

# Scrape Clutch directory
python scripts/clutch_scraper.py
# (prompts for category URL, filters, runs with checkpoints)

# Regenerate keywords for existing niche CSV
python scripts/regen_keywords.py --input output/niches_2026-04-17.csv
```

---

## Output Format

Every enriched CSV is Instantly-ready with these columns:

`first_name` · `last_name` · `email` · `company_name` · `website` · `icp_match` · `value_prop` · `business_icp` · `niche` · `email_source` · `confidence_score` · `notes`

The `value_prop` column slots directly into the cold email template:

> *"I came across [company] and saw you [value_prop]..."*

---

## Directory Structure

```
├── scripts/          Python scripts
├── input/            Drop raw CSVs here
├── output/           Enriched CSVs (timestamped)
├── logs/             Error logs, analyst findings
├── skills/           Claude Code skill definitions
├── .env.example      Environment variable template
└── CLAUDE.md         Agent instructions
```

---

## License

MIT
