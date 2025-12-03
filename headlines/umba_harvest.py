#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
import feedparser
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Config – adjust these paths to match your UMBA layout
# -----------------------------------------------------------------------------

BASE_DIR = Path(os.path.expanduser("~")) / "C:/msys64/home/umba-kb" / "headlines"
OUTLETS_FILE = BASE_DIR / "outlets_rss.txt"
MASTER_CSV = BASE_DIR / "raw" / "headlines_master.csv"

USER_AGENT = (
    "Mozilla/5.0 (compatible; UMBA-Harvester/1.0; +https://example.com/umba)"
)

REQUEST_TIMEOUT = 20  # seconds per request
MAX_ITEMS_PER_SOURCE = 50  # safety cap per feed / page


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def today_iso():
    return dt.date.today().isoformat()


def domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc or ""
    except Exception:
        return ""


def read_outlet_urls(path: Path):
    urls = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("http://") or line.startswith("https://"):
                urls.append(line)
    return urls


def fetch_url(url: str) -> requests.Response | None:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp
    except Exception as e:
        print(f"[WARN] Failed to fetch {url}: {e}", file=sys.stderr)
        return None


# -----------------------------------------------------------------------------
# RSS / Atom handling
# -----------------------------------------------------------------------------

def harvest_rss(url: str, resp: requests.Response):
    """Return list of (date, domain, title, link) from an RSS/Atom feed."""
    entries = []
    parsed = feedparser.parse(resp.content)
    if not parsed.entries:
        return entries

    source_domain = domain_from_url(url)

    for entry in parsed.entries[:MAX_ITEMS_PER_SOURCE]:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()

        if not title or not link:
            continue

        # Try to get a date, fallback to today
        pub_date = today_iso()
        for key in ("published_parsed", "updated_parsed", "created_parsed"):
            value = getattr(entry, key, None)
            if value:
                try:
                    dt_obj = dt.datetime(*value[:6])
                    pub_date = dt_obj.date().isoformat()
                    break
                except Exception:
                    pass

        entries.append((pub_date, source_domain, title, link))

    return entries


# -----------------------------------------------------------------------------
# Generic HTML scraping (best-effort, free content only)
# -----------------------------------------------------------------------------

def harvest_html(url: str, resp: requests.Response):
    """Best-effort: get headline-like links from an HTML page."""
    entries = []
    today = today_iso()
    source_domain = domain_from_url(url)

    soup = BeautifulSoup(resp.text, "html.parser")

    # Strategy:
    # 1. Try <article> tags with <a> inside
    # 2. Fallback to <h1>/<h2>/<h3> containing <a>
    candidates = []

    # 1. <article> tags
    for article in soup.find_all("article"):
        a = article.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        href = a["href"]
        if title and href:
            candidates.append((title, href))

    # 2. headline tags as backup
    if not candidates:
        for tag_name in ("h1", "h2", "h3"):
            for tag in soup.find_all(tag_name):
                a = tag.find("a", href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                href = a["href"]
                if title and href:
                    candidates.append((title, href))

    # Normalize and limit
    seen = set()
    for title, href in candidates[:MAX_ITEMS_PER_SOURCE]:
        link = urljoin(url, href)
        key = (title, link)
        if key in seen:
            continue
        seen.add(key)
        entries.append((today, source_domain, title, link))

    return entries


# -----------------------------------------------------------------------------
# CSV handling
# -----------------------------------------------------------------------------

def load_existing_lines(path: Path):
    """Load existing lines into a set to avoid duplicates."""
    if not path.exists():
        return set()
    existing = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        for line in f:
            existing.add(line.rstrip("\n"))
    return existing


def append_entries_to_csv(path: Path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_lines = load_existing_lines(path)

    new_count = 0
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for date, domain, title, link in entries:
            row = [date, domain, title, link]
            # Construct a canonical line representation for dedupe
            line = ",".join(
                [
                    date,
                    domain,
                    f'"{title.replace("\"", "\"\"")}"',
                    f'"{link}"',
                ]
            )
            if line in existing_lines:
                continue
            writer.writerow(row)
            existing_lines.add(line)
            new_count += 1

    return new_count


# -----------------------------------------------------------------------------
# Core harvest logic
# -----------------------------------------------------------------------------

def harvest_one_url(url: str, dry_run: bool = False):
    resp = fetch_url(url)
    if resp is None:
        return []

    # First, try as RSS/Atom
    rss_entries = harvest_rss(url, resp)
    if rss_entries:
        print(f"[INFO] {url} -> RSS/Atom: {len(rss_entries)} entries")
        return rss_entries

    # If no RSS entries, treat as HTML
    html_entries = harvest_html(url, resp)
    print(f"[INFO] {url} -> HTML fallback: {len(html_entries)} entries")
    return html_entries


def harvest_all_outlets(dry_run: bool = False):
    print(f"[INFO] Using outlets file: {OUTLETS_FILE}")
    urls = read_outlet_urls(OUTLETS_FILE)
    if not urls:
        print("[ERROR] No URLs found in outlets file.", file=sys.stderr)
        return []

    all_entries = []
    for url in urls:
        entries = harvest_one_url(url, dry_run=dry_run)
        all_entries.extend(entries)

    return all_entries


# -----------------------------------------------------------------------------
# Main / CLI
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="UMBA headline harvester (RSS + HTML fallback)."
    )
    parser.add_argument(
        "--test-url",
        help="Fetch and show entries for a single URL (no CSV writes).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process all outlets but do NOT write the CSV. Print counts only.",
    )
    args = parser.parse_args()

    # Case 1: Test a single URL (no CSV writes)
    if args.test_url:
        print(f"[INFO] TEST-URL mode for: {args.test_url}")
        entries = harvest_one_url(args.test_url, dry_run=True)
        print("")
        print(f"[INFO] Found {len(entries)} entries.")
        for date, domain, title, link in entries[:10]:
            print(f"- [{date}] {domain} :: {title} -> {link}")
        if len(entries) > 10:
            print(f"... {len(entries) - 10} more not shown")
        return 0

    # Case 2: Dry-run on all outlets (no CSV writes)
    if args.dry_run:
        print("[INFO] DRY-RUN mode: no CSV writes.")
        entries = harvest_all_outlets(dry_run=True)
        print(f"[INFO] Total entries (all outlets): {len(entries)}")
        return 0

    # Case 3: Normal daily run – write to CSV
    print(f"[INFO] OUTLETS file: {OUTLETS_FILE}")
    print(f"[INFO] Writing master CSV: {MASTER_CSV}")
    entries = harvest_all_outlets(dry_run=False)
    added = append_entries_to_csv(MASTER_CSV, entries)
    print(f"[INFO] Done. Added {added} new headlines.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
