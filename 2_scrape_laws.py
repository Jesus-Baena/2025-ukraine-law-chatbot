"""
Step 2: Scrape full law text from zakon.rada.gov.ua

Reads catalogue.json, fetches each law page, extracts structured text.
Saves one JSON file per law to data/laws/{id}.json.
Resumable: skips already-downloaded laws.

Output: data/laws/{law_id}.json per law
"""

import json
import time
from tqdm import tqdm
from config import (
    CATALOGUE_PATH, LAWS_DIR, LAW_BASE_URL,
    REQUEST_DELAY, BATCH_SIZE, FORCE_RESCRAPE
)
from law_processing import extract_law, fetch_with_retry, safe_filename


def main():
    print("=== Step 2: Scraping law texts from zakon.rada.gov.ua ===\n")

    if not CATALOGUE_PATH.exists():
        print("✗ catalogue.json not found. Run 1_fetch_catalogue.py first.")
        return

    catalogue = json.loads(CATALOGUE_PATH.read_text(encoding="utf-8"))
    print(f"Catalogue: {len(catalogue)} laws")

    # Check which are already downloaded
    done_files = {p.name for p in LAWS_DIR.glob("*.json")}

    if FORCE_RESCRAPE:
        todo = list(catalogue)
        print("Force rescrape: enabled")
    else:
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
        print_url = f"{url}/print"
        out_path = LAWS_DIR / safe_filename(law_id)

        response = fetch_with_retry(print_url) or fetch_with_retry(url)

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
