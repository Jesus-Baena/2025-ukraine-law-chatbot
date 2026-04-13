"""
Step 4: Incremental update — fetch and embed new laws since last run

Designed to run daily via n8n cron or standalone cron job.
Checks data.rada.gov.ua for laws updated since last run date,
scrapes and embeds only the new/changed ones.

State tracked in data/state.json: { "last_run": "2024-01-15" }
"""

import json
import time
import requests
from datetime import datetime, date
from pathlib import Path
from sentence_transformers import SentenceTransformer

from config import (
    DATA_DIR, LAWS_DIR, STATE_PATH, QDRANT_URL, QDRANT_COLLECTION,
    EMBED_MODEL, REQUEST_DELAY, REQUEST_TIMEOUT, REQUEST_DELAY
)
from qdrant_client import QdrantClient

# Import helpers from other steps
from 2_scrape_laws import fetch_with_retry, extract_law, safe_filename
from 3_chunk_embed import law_to_chunks, embed_chunks, upsert_to_qdrant, setup_qdrant


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; rada-rag/1.0)",
    "Accept": "application/json, text/csv, */*",
}

# Open data portal: updated datasets feed
# The portal publishes daily update feeds
UPDATE_FEED_URL = "https://data.rada.gov.ua/open/data/zak.json"


def load_state() -> dict:
    """Load run state. Returns default if no state file exists."""
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    # Default: look back 7 days on first run
    return {"last_run": "2020-01-01", "total_laws_processed": 0}


def save_state(state: dict):
    """Persist run state."""
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def fetch_updated_ids(since_date: str) -> list[dict]:
    """
    Fetch law IDs updated since a given date from the Rada open data portal.
    
    The portal's dataset feed includes update timestamps.
    Filter to entries updated after since_date.
    """
    try:
        r = requests.get(UPDATE_FEED_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        all_laws = r.json()
    except Exception as e:
        print(f"  ✗ Could not fetch update feed: {e}")
        return []

    updated = []
    for law in all_laws:
        # Check various date fields
        law_date = (
            law.get("updated") or law.get("date_updated") or
            law.get("date") or law.get("enacted") or ""
        )
        if law_date and law_date[:10] >= since_date:
            law_id = (
                law.get("id") or law.get("num") or
                law.get("number") or law.get("law_id")
            )
            if law_id:
                updated.append({
                    "id": str(law_id),
                    "title": law.get("title") or law.get("name") or "",
                    "date": law_date[:10],
                    "category": law.get("category") or law.get("type") or "",
                    "url": f"https://zakon.rada.gov.ua/laws/show/{law_id}",
                })

    return updated


def delete_law_from_qdrant(client: QdrantClient, law_id: str):
    """Remove all chunks for a law from Qdrant (before re-inserting)."""
    from qdrant_client.models import Filter, FieldCondition, MatchValue
    client.delete(
        collection_name=QDRANT_COLLECTION,
        points_selector=Filter(
            must=[FieldCondition(
                key="law_id",
                match=MatchValue(value=law_id)
            )]
        )
    )


def process_law(entry: dict, model: SentenceTransformer,
                client: QdrantClient) -> bool:
    """Scrape, embed, and upsert a single law. Returns True on success."""
    law_id = entry["id"]
    url = f"https://zakon.rada.gov.ua/laws/show/{law_id}"

    response = fetch_with_retry(url)
    if response is None:
        print(f"  ✗ Failed to fetch: {law_id}")
        return False

    law = extract_law(response.text, law_id, url)
    if not law or law.get("section_count", 0) == 0:
        print(f"  ✗ Empty body: {law_id}")
        return False

    law["category"] = entry.get("category", "")
    law["catalogue_date"] = entry.get("date", "")

    # Save to disk (overwrite if exists)
    out_path = LAWS_DIR / safe_filename(law_id)
    out_path.write_text(
        json.dumps(law, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # Remove old vectors for this law, then re-insert
    delete_law_from_qdrant(client, law_id)

    chunks = law_to_chunks(law)
    if chunks:
        embeddings = embed_chunks(model, chunks)
        upsert_to_qdrant(client, chunks, embeddings)

    return True


def main():
    print("=== Step 4: Incremental update ===\n")
    print(f"Run time: {datetime.now().isoformat()}\n")

    state = load_state()
    last_run = state["last_run"]
    print(f"Last run: {last_run}")

    # Fetch new/updated law IDs
    print(f"Fetching laws updated since {last_run}...")
    updated_entries = fetch_updated_ids(last_run)
    print(f"Found {len(updated_entries)} updated laws")

    if not updated_entries:
        print("✓ Nothing to update.")
        state["last_run"] = date.today().isoformat()
        save_state(state)
        return

    # Setup
    client = QdrantClient(url=QDRANT_URL)
    setup_qdrant(client)

    import torch
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"\nLoading {EMBED_MODEL} on {device}...")
    model = SentenceTransformer(EMBED_MODEL, device=device)

    # Process each updated law
    success = 0
    failed = 0

    for i, entry in enumerate(updated_entries):
        print(f"[{i+1}/{len(updated_entries)}] {entry['id']}: {entry['title'][:60]}")
        ok = process_law(entry, model, client)
        if ok:
            success += 1
        else:
            failed += 1
        time.sleep(REQUEST_DELAY)

    # Update state
    state["last_run"] = date.today().isoformat()
    state["total_laws_processed"] = state.get("total_laws_processed", 0) + success
    save_state(state)

    # Report
    collection_info = client.get_collection(QDRANT_COLLECTION)
    print(f"\n=== Done ===")
    print(f"  Updated: {success}")
    print(f"  Failed:  {failed}")
    print(f"  Qdrant total vectors: {collection_info.points_count}")
    print(f"  Next run will check from: {state['last_run']}")


if __name__ == "__main__":
    main()
