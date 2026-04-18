# PipelineSpark GTM Toolkit

## Who I Am
I run PipelineSpark, a cold email outreach agency. My job is to build targeted lead lists and run outbound campaigns on behalf of clients. I am the sole operator — I move fast, I'm technical enough to run scripts, but I don't write code myself. That's what you're here for.

## What This Project Does
This is my internal GTM toolkit. It takes raw lead lists and turns them into enriched, Instantly-ready CSVs. Every script in this project exists to save me time on list building and enrichment so I can focus on client work.

---

## Agent Architecture

**Layer 1: Skills (What to do)**
- Workflow definitions in `.claude/skills/`
- Claude Code auto-triggers relevant skills based on task context
- Each skill defines: goal, inputs, process steps, execution scripts, outputs, edge cases
- Generic and reusable — project-specific values live in configs

**Layer 2: Execution (Doing the work)**
- Deterministic Python scripts in `scripts/`
- Environment variables and API tokens stored in `.env`
- Handle API calls, data processing, file operations
- Reliable, testable, fast. Use scripts instead of manual work.

**Why this works:** If you do everything yourself, errors compound. 90% accuracy per step = 59% success over 5 steps. Push complexity into deterministic code so you focus on decision-making.

### Operating Principles

1. **Check skills first** — before creating a new workflow, check `.claude/skills/`. Only create new skills if none exist for the task.
2. **Check scripts before writing code** — check `scripts/` for existing tools before writing anything new.
3. **Self-anneal when things break** — read error, fix script, test again, update skill with learnings.
4. **Update skills as you learn** — skills are living documents. Do not create or overwrite skills without asking unless explicitly told to.

### Resource Management
For any process that runs longer than 2 minutes: launch it, release it, trust it. No sleep loops. No polling. Check status only when asked. All batch scripts must save progress checkpoints every 50-100 items.

---

## Directory Structure
```
PipeLineSpark_GTM/
├── CLAUDE.md           ← This file
├── .env                ← API keys (never hardcode)
├── .claude/skills/     ← Workflow definitions
├── input/              ← Drop raw CSVs here
├── output/             ← Enriched CSVs go here
├── scripts/            ← All Python scripts live here
└── logs/               ← Error logs and skipped rows
```

Always read from `input/` and write to `output/`. Never overwrite the original input file. Name output files with a UTC timestamp (e.g. `enriched_2026-04-01T00-00-00Z.csv`).

---

## My Cold Email Template
Scripts must produce columns that slot directly into this template. The structure is fixed — the numbers, timeframe, and offer line flex per campaign and ICP but the format never changes.

> Hey {{firstName}},
>
> I came across {{companyName}} and saw you {{value_prop}}.
>
> We run and manage a cold email system that books 7-12 qualified meetings with {{business_icp}} straight onto your calendar in 45 days. Zero risk, performance-based.
>
> Would you be open to a quick chat?

---

## My Stack
- **Outreach platform:** Instantly
- **Email verification:** MailTester — handled manually by me after export, do not automate this
- **Primary enrichment:** Website scraping (always first)
- **Secondary enrichment:** Exa contents API (fallback when scraping fails)
- **LLM:** DeepSeek (`deepseek-chat`) via OpenAI-compatible endpoint — used for ICP classification, email extraction (obfuscated formats), and personalization. API key in `.env` as `DEEPSEEK_API_KEY`
- **Language:** Python (always use Python unless I say otherwise)
- **Environment variables:** Stored in `.env` — never hardcode keys
- **Parallelisation:** 10 concurrent workers via `ThreadPoolExecutor` by default

---

## How I Want You to Operate
Be recursive and autonomous. When given a task:
1. Figure out what needs to happen to complete it fully
2. Write the script, run it, check the output, fix any errors yourself
3. If the output looks wrong or incomplete, iterate until it's right
4. Only stop and ask me if you are genuinely blocked or need a decision from me
5. Always show me a sample of the output (first 5 rows) when done so I can sanity check it

Do not ask for permission at every step. Just work through it.

---

## Input Formats
My CSVs will typically come in one of two formats:
1. `company_name, website` — full URL included
2. `company_name, domain` — just the domain (e.g. `acme.com`)

Always inspect the CSV first to confirm column names before doing anything. Handle both formats gracefully. Never assume column names — read them. Strip UTM parameters from any website URLs before using them.

---

## Output Format (Instantly-Ready)
Every enriched CSV must have these columns in this order:
- `first_name`
- `last_name`
- `email`
- `company_name`
- `website`
- `icp_match` — yes / no / uncertain
- `value_prop` — completes "I saw you ___". Must include ONE primary service/product AND the specific buyer. Generated together with `business_icp` in a single LLM call to guarantee they are correlated. e.g. "offer commercial office cleaning to property managers"
- `business_icp` — the specific buyer extracted from `value_prop`. Must exactly match whoever appears in `value_prop`. Determined automatically per row:
  - If the product/service can be sold across multiple industries → use a **job title/role** (e.g. "property managers", "HR managers", "office managers")
  - If the product/service only makes sense for one specific industry → use a **business type** (e.g. "law firms", "dental practices", "restaurants")
  - Never use vague terms like "businesses", "companies", or "clients"
  - Generated in the same LLM call as `value_prop` — never separately
- `niche` — short descriptor of the company type, lowercase plural. e.g. "commercial cleaning companies"
- `email_source` — where the email came from (e.g. "contact", "about", "homepage", "exa:contact", "llm")
- `confidence_score` — high / medium / low
- `notes` — anything worth flagging (e.g. "multiple emails found", "generic inbox", "no email found", ICP reason)

Do not drop rows with no email found — include them with email blank and a note. I handle verification manually.

---

## Lead Quality Filters — Apply Before Everything Else
Every lead must pass these two filters before any enrichment. If either fails, disqualify immediately.

### 1. High ticket (2k+ deal size)
The prospect must be able to afford a $2k+ service. Disqualify if:
- Solo operator / single-person business (no employees, one-person operation, sole trader signals in name or description)
- Micro-businesses with no web presence beyond a social page
- Use reviews count, description, and business name as signals — a business with 500+ reviews and a proper website is likely large enough

### 2. Reachable TAM via email
The prospect must be reachable and decision-making at the local/company level. Disqualify if:
- Purely B2C (consumer-facing only, no B2B angle)
- Franchise location of a national brand — decisions are made at corporate, not locally actionable
- No website or dead domain
- Only has a social media presence (no business email reachable)

Flag these in `notes` with the specific reason so they're easy to review.

---

## ICP Qualification — Always Run First
Before any email finding or personalization, every script must qualify each row against the target ICP.

- The ICP is passed in as a required `--icp` parameter (e.g. `--icp "commercial cleaning"`)
- There is no default — if no ICP is provided, the script must exit with an error
- ICP classification is **niche-agnostic** — keywords and LLM prompts are derived dynamically from whatever `--icp` is passed in. Never hardcode niche-specific logic
- Classify each row using this priority order — stop as soon as a confident call can be made:
  1. **Existing CSV columns** — check all columns for useful signals. Keyword match against the ICP phrase. Requires at least 2 agreeing signals for a confident yes. Google Maps categories are known to be inaccurate so always cross-reference with the business name.
  2. **LLM classification** — if CSV columns are insufficient, fetch the homepage and pass the text to DeepSeek. Prompt must be fully dynamic — no hardcoded niche logic.
- If `icp_match` is `no` → skip all enrichment, write the row with blank email fields
- If `icp_match` is `uncertain` → still attempt email finding, flag in notes for manual review
- If `icp_match` is `yes` → proceed normally

Never waste scraping or API calls on disqualified rows.

---

## Personalization Enrichment
For all qualified rows (yes + uncertain), generate `value_prop`, `business_icp`, and `niche` in a **single DeepSeek call** using the homepage text already fetched during ICP qualification — no extra web request needed.

Rules for the LLM call:
- `value_prop` and `business_icp` must be generated together — never in separate calls
- `value_prop` picks ONE primary service only — never lists multiple
- `business_icp` must exactly match whoever appears in `value_prop`
- If homepage text wasn't fetched during ICP qualification, fetch it now before calling the LLM

---

## Email Finding Logic — Priority Order
Accuracy is the top priority. Always follow this exact order:

### 0. Domain validation (before any scraping)
- DNS lookup on the domain — if it doesn't resolve, log as "dead domain" and skip

### 1. Scrape all pages + homepage, run regex first (free)
- Scrape: `/contact`, `/contact-us`, `/about`, `/about-us`, `/team`, `/people`, `/staff`, and homepage
- On team/people/about pages: extract person name and job title to support decision maker targeting
- Run a fast regex pass across all collected text — catches standard email formats at zero cost
- If a named non-generic business email is found → done, no LLM needed

### 2. DeepSeek fallback (only when regex finds nothing useful)
- If regex only found generic emails or nothing, make ONE combined DeepSeek call on all page text
- This single call extracts both emails (including obfuscated formats) AND team contacts
- Never call DeepSeek per page — always batch all pages into one call

### 3. Exa contents fetch (only if scraping returned no text at all)
- Use Exa `contents` endpoint — never Exa search (10x more expensive)
- Fetch the top 3 contact/about pages via Exa, combine, pass to DeepSeek in one call
- API key in `.env` as `EXA_API_KEY`

### 4. No email found
- Leave `email`, `email_source`, `confidence_score` blank
- Set `notes` to "no email found"
- Never guess — accuracy over volume

### 5. Additional API enrichment — do not implement until I provide a key
- When I say "I have a [tool] API key", slot it in as a new function between steps 3 and 4

---

## Email Quality Rules
- **Blocked emails** — discard entirely: `filler@godaddy.com`, parked domain emails, known parking services (godaddy.com, sedo.com, parkingcrew.com, hugedomains.com)
- **Generic prefixes** — `info@`, `hello@`, `contact@`, `support@`, `admin@`, `office@`, `mail@`, `team@`, `sales@`, `help@` → confidence: medium, note: "generic inbox"
- **Personal/free email providers** — gmail, yahoo, hotmail, outlook, icloud, ymail, sbcglobal, aol → confidence: low, note: "personal/free email provider"
- **Domain mismatch** — if email domain doesn't match company website domain → confidence: low, note the mismatch
- **Named business email** → confidence: high

---

## Decision Maker Targeting
When multiple contacts are found, prioritise in this order:

1. **Sales & Revenue leadership** — CRO, VP of Sales, Head of Sales, Director of Sales, VP/Head of Business Development, Head of Growth, Founding GTM, Founding Sales
2. **CEO / Founder / President** — any variant
3. **Other C-suite** — COO, CTO, CFO, Managing Director, Managing Partner, General Manager
4. **Other VPs / Directors** — Vice President, SVP, Director (any department)
5. **Everyone else** — engineers, recruiters, individual contributors, etc.

**Non-decision-maker removal rule:** If a company has at least one contact in tiers 1–4, drop all tier-5 contacts for that company. Only keep tier-5 if they are the sole contact.

Non-DM titles to treat as tier 5:
- Founding Engineer, Founding Software Engineer, Founding AI Engineer, Founding Prompt Engineer
- Founding Recruiting Lead, Business Technology Consultant, Agency Collective, Member

---

## 4-Agent GTM System

### Agent 1 — List Builder (`scripts/agent1_list_builder.py`)
Takes any raw CSV and outputs a clean, deduplicated, DNS-validated CSV ready for enrichment.
- Auto-detects column names (handles Google Maps, Apollo, LinkedIn, plain company lists)
- Normalizes websites to `https://domain.com`, strips UTMs
- DNS validates all domains — logs dead ones to `logs/`
- Detects whether emails are already present (Apollo exports) or need to be found
- Deduplicates by domain
- Run: `python scripts/agent1_list_builder.py --input input/raw.csv --output input/cleaned.csv`

### Agent 2 — Enricher (`scripts/agent2_enricher.py`)
Takes the cleaned list and outputs a fully Instantly-ready CSV.
- Scrapes homepage + contact/about/team pages → stores `site_text`
- ICP qualification via site_text + CSV signals (single DeepSeek call)
- Generates `value_prop`, `business_icp`, `niche` in same LLM call
- Email finding: regex → DeepSeek fallback → Exa fallback (skipped if emails already present)
- Decision maker targeting: scores titles, caps at 1-2 per company, drops non-DMs
- Resumable: skips already-enriched domains on re-run
- Run: `python scripts/agent2_enricher.py --input input/cleaned.csv --icp "commercial cleaning"`

### Agent 3 — Copywriter (`scripts/agent3_copywriter.py`)
Generates a 2-step email sequence, previews it, waits for approval, then pushes to Instantly.
- Uses enriched CSV + optional analyst findings from Agent 4
- Writes copy with light spintax ({{RANDOM | opt1 | opt2}}) where it makes sense
- Shows full preview before anything is pushed — you must approve
- Pushes campaign + all leads to Instantly API
- Campaign is created paused — you activate manually
- Run: `python scripts/agent3_copywriter.py --input output/enriched.csv --campaign-name "My Campaign"`
- With findings: add `--findings logs/analyst_findings.json`

### Agent 4 — Analyst (`scripts/agent4_analyst.py`)
Audits all Instantly campaigns, auto-pauses high-bounce ones, outputs findings for Agent 3.
- Pulls reply rate, bounce rate, opportunities per campaign
- Auto-pauses any active campaign exceeding 2% bounce rate
- Runs DeepSeek analysis on copy + performance data
- Outputs `logs/analyst_findings.json` — feed this into Agent 3 to improve next campaign
- Run: `python scripts/agent4_analyst.py`

### Orchestrator (`scripts/orchestrator.py`)
Master runner. Chains all agents with quality gates. Halts if data quality fails.
```
# Full pipeline
python scripts/orchestrator.py --input input/raw.csv --campaign-name "My Campaign" --icp "commercial cleaning"

# Start from enrichment (already cleaned)
python scripts/orchestrator.py --input input/cleaned.csv --campaign-name "My Campaign" --start-at agent2

# Analyst only
python scripts/orchestrator.py --start-at agent4

# Pull analyst findings first, then run full pipeline informed by them
python scripts/orchestrator.py --input input/raw.csv --campaign-name "My Campaign" --with-findings
```

Quality gates:
- After Agent 1: ≥40% of rows must pass DNS validation
- After Agent 2: at least 1 qualified row; warns if email rate <20%
- After Agent 3: campaign log must confirm >0 leads uploaded
- After Agent 4: findings file must exist and contain campaign data

Instantly API endpoints used:
- `POST /campaigns` — create campaign
- `POST /leads` — add lead
- `POST /campaigns/{id}/pause` — auto-pause (bounce >2%)
- `POST /campaigns/{id}/activate` — resume
- `DELETE /campaigns/{id}` — delete (no Content-Type header)
- `GET /campaigns/analytics` — pull stats

---

## Legacy Scripts
- `scripts/enrich_gmaps.py` — original enrichment script for Google Maps exports
- `scripts/backfill_personalization.py` — backfills `value_prop`, `business_icp`, `niche` on existing enriched CSV without re-scraping
- `scripts/split_output.py` — splits enriched output into qualified and disqualified CSVs
- `scripts/enrich_sdr_personalization.py` — enriches SDR/Apollo merged CSVs with B2B/high-ticket ICP evaluation

---

## Script Behavior Standards
- Print progress: `[row/total] company — status`
- Log skipped/errored rows to `logs/` with reason
- Resumable — use `website` (normalised to domain) as dedup key. Skip rows already in output file
- 10-second timeout per HTTP request — log and skip on timeout, never hang
- Rotate user agents on every request
- 10 concurrent workers via `ThreadPoolExecutor` — no per-row `sleep()` needed
- Thread-safe file writes via a lock
- If a site blocks scraping, log it and move on — never crash the run
- Accept CLI arguments for flexibility
- Include `--test` flag for dry runs on all scripts
- Save progress checkpoints every 50 items for batch operations

---

## Cold Email Copy Rules
When writing any cold email copy, NEVER use:
- Em dashes (—) — replace with a period, comma, or rewrite the sentence
- AI-sounding words: leverage, streamline, seamlessly, unlock, game-changer, innovative, cutting-edge, tailored, pain points, value proposition, strategic, synergy, scalable, robust, empower, transform, revolutionize, comprehensive, utilize, facilitate
- Filler openers: "I wanted to", "I noticed", "I came across", "I hope this email finds you", "hope this finds you well", "I came across your profile"
- Any phrasing that sounds like it was written by AI

Copy must sound like a real person wrote it in under 2 minutes.

---

## List Building — Apollo + Job Postings Workflow
When building lists from LinkedIn job postings + Apollo contact exports:

1. **Source files:** Job postings CSV + Apollo contacts CSV
2. **Join key — normalize domain from both sides:** Strip `https://`, `http://`, `www.`, trailing slashes, paths, query strings. Join on normalized domain.
3. **After merge:** Score contacts by title priority, sort within company, remove tier-5 non-DMs from companies that already have a tier 1–4 contact
4. **Then add enrichment columns** — scrape homepage per company, run ICP classification and personalization via DeepSeek

---

## General Preferences
- Accuracy over speed — fewer real emails beats many guessed ones
- Always confirm column names found in the CSV before starting work
- Comment code clearly so I understand what each block does
- When a script finishes, give me: output file name + location, rows processed, emails found, emails not found
- Keep scripts modular — each enrichment step should be its own function
- One-off/throwaway scripts go in `New/` — delete anytime. If useful long-term, move to `scripts/`
- Never delete files — archive to `.archive/` instead
