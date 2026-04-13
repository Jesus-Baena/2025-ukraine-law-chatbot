"""
Step 2: Scrape full law text from zakon.rada.gov.ua

Reads catalogue.json, fetches each law page, extracts structured text.
Saves one JSON file per law to data/laws/{id}.json.
Resumable: skips already-downloaded laws.

Output: data/laws/{law_id}.json per law
"""

import json
import time
import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
from config import (
    CATALOGUE_PATH, LAWS_DIR, LAW_BASE_URL,
    REQUEST_DELAY, REQUEST_TIMEOUT, MAX_RETRIES, BATCH_SIZE
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "uk,en;q=0.9",
}


def safe_filename(law_id: str) -> str:
    """Convert law ID to safe filename."""
    return re.sub(r'[^\w\-]', '_', law_id) + ".json"


def extract_law(html: str, law_id: str, url: str) -> dict | None:
    """
    Parse zakon.rada.gov.ua law page HTML.
    
    The site uses a consistent structure:
    - Title: <h1> tag
    - Metadata: table or header area with date, number, status
    - Body: div with id containing "law" or class "document"
      Articles/paragraphs are <p> tags within structured sections
    """
    soup = BeautifulSoup(html, "lxml")

    # --- Title ---
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)

    # --- Metadata ---
    # Look for law number, date, status in structured header
    law_number = ""
    enacted_date = ""
    status = ""
    issuer = ""

    # Try common metadata containers
    meta_area = (
        soup.find("div", class_=re.compile(r"(meta|header|info)", re.I)) or
        soup.find("table", class_=re.compile(r"(meta|header|info)", re.I))
    )
    if meta_area:
        text = meta_area.get_text(" ", strip=True)
        # Extract date pattern YYYY-MM-DD or DD.MM.YYYY
        dates = re.findall(r'\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4}', text)
        if dates:
            enacted_date = dates[0]

    # --- Body ---
    # Try multiple selectors in priority order
    body_selectors = [
        {"id": re.compile(r"law", re.I)},
        {"class": re.compile(r"(document|content|law|text)", re.I)},
        {"id": "content"},
        {"id": "main"},
    ]

    body_div = None
    for sel in body_selectors:
        body_div = soup.find("div", sel)
        if body_div:
            break

    # Fallback: use article tag or main content area
    if not body_div:
        body_div = soup.find("article") or soup.find("main")

    # Last resort: entire body
    if not body_div:
        body_div = soup.find("body")

    if not body_div:
        return None

    # --- Extract structured sections ---
    # Remove navigation, scripts, styles, footnotes
    for tag in body_div.find_all(["script", "style", "nav", "footer", "noscript"]):
        tag.decompose()

    sections = []
    current_section = {"heading": "", "text": ""}

    for elem in body_div.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        tag = elem.name
        text = elem.get_text(" ", strip=True)

        if not text or len(text) < 3:
            continue

        if tag in ["h1", "h2", "h3", "h4"]:
            # Save previous section
            if current_section["text"].strip():
                sections.append(current_section)
            current_section = {"heading": text, "text": ""}
        else:
            # Accumulate paragraph text under current section
            if current_section["text"]:
                current_section["text"] += "\n" + text
            else:
                current_section["text"] = text

    # Save last section
    if current_section["text"].strip():
        sections.append(current_section)

    # Fallback: if no sections found, get all text
    if not sections:
        full_text = body_div.get_text("\n", strip=True)
        # Split by double newlines into rough paragraphs
        paragraphs = [p.strip() for p in full_text.split("\n\n") if p.strip()]
        sections = [{"heading": "", "text": p} for p in paragraphs]

    if not sections:
        return None

    return {
        "id": law_id,
        "title": title,
        "law_number": law_number,
        "enacted_date": enacted_date,
        "status": status,
        "issuer": issuer,
        "url": url,
        "sections": sections,
        "section_count": len(sections),
    }


def fetch_with_retry(url: str) -> requests.Response | None:
    """Fetch URL with exponential backoff retry."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
            elif r.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"\n  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            elif r.status_code == 404:
                return None  # Law doesn't exist
            else:
                print(f"\n  HTTP {r.status_code} for {url}")
                time.sleep(5)
        except requests.RequestException as e:
            wait = 5 * (attempt + 1)
            print(f"\n  Error: {e}. Retry in {wait}s...")
            time.sleep(wait)
    return None


def main():
    print("=== Step 2: Scraping law texts from zakon.rada.gov.ua ===\n")

    if not CATALOGUE_PATH.exists():
        print("✗ catalogue.json not found. Run 1_fetch_catalogue.py first.")
        return

    catalogue = json.loads(CATALOGUE_PATH.read_text(encoding="utf-8"))
    print(f"Catalogue: {len(catalogue)} laws")

    # Check which are already downloaded
    done = {p.stem.replace("_", "/") for p in LAWS_DIR.glob("*.json")}
    # More robust: track by filename
    done_files = {p.name for p in LAWS_DIR.glob("*.json")}

    todo = [
        e for e in catalogue
        if safe_filename(e["id"]) not in done_files
    ]
    print(f"Already downloaded: {len(catalogue) - len(todo)}")
    print(f"To download: {len(todo)}\n")

    if not todo:
        print("✓ All laws already downloaded.")
        return

    # Stats
    success = 0
    failed = 0
    empty = 0

    for i, entry in enumerate(tqdm(todo, desc="Scraping")):
        law_id = entry["id"]
        url = LAW_BASE_URL.format(law_id=law_id)
        out_path = LAWS_DIR / safe_filename(law_id)

        response = fetch_with_retry(url)

        if response is None:
            failed += 1
            # Save error marker so we don't retry indefinitely
            out_path.write_text(
                json.dumps({"id": law_id, "error": "fetch_failed", "url": url},
                           ensure_ascii=False),
                encoding="utf-8"
            )
        else:
            result = extract_law(response.text, law_id, url)
            if result is None or result["section_count"] == 0:
                empty += 1
                out_path.write_text(
                    json.dumps({"id": law_id, "error": "empty_body", "url": url,
                                "title": entry.get("title", "")},
                               ensure_ascii=False),
                    encoding="utf-8"
                )
            else:
                # Enrich with catalogue metadata
                result["category"] = entry.get("category", "")
                result["catalogue_date"] = entry.get("date", "")
                out_path.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
                success += 1

        # Progress checkpoint every BATCH_SIZE laws
        if (i + 1) % BATCH_SIZE == 0:
            tqdm.write(f"  Checkpoint: {success} ok, {failed} failed, {empty} empty")

        time.sleep(REQUEST_DELAY)

    print(f"\n=== Done ===")
    print(f"  Success:  {success}")
    print(f"  Failed:   {failed}")
    print(f"  Empty:    {empty}")
    print(f"  Total:    {success + failed + empty}")
    print(f"\nLaw files in: {LAWS_DIR}")


if __name__ == "__main__":
    main()
