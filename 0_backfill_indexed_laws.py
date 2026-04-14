"""
Backfill PostgreSQL staging tables using laws listed in INDEXED_LAWS.md.

This script:
1) Ensures `rada_*` staging schema exists.
2) Parses law IDs from INDEXED_LAWS.md.
3) Loads each corresponding JSON from data/laws/.
4) Persists law + sections + chunks into staging tables.
5) Marks staged chunks as already synced to Qdrant.
"""

import json
import re
from pathlib import Path

from config import DATABASE_URL, LAWS_DIR
from embedding_pipeline import law_to_chunks
from indexed_laws_tracker import upsert_indexed_law
from law_processing import safe_filename
from staging_db import (
    ensure_staging_schema,
    get_postgres_connection,
    stage_chunks_for_law,
    stage_law_with_sections,
)

INDEXED_LAWS_PATH = Path(__file__).parent / "INDEXED_LAWS.md"
LAW_ID_ROW_RE = re.compile(r"^\|\s*`([^`]+)`\s*\|")


def load_catalogue_map() -> dict[str, dict]:
    path = Path(__file__).parent / "data" / "catalogue.json"
    if not path.exists():
        return {}
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    result: dict[str, dict] = {}
    for entry in entries:
        law_id = str(entry.get("id", "")).strip()
        if law_id:
            result[law_id] = entry
    return result


def parse_indexed_law_ids() -> list[str]:
    if not INDEXED_LAWS_PATH.exists():
        raise FileNotFoundError(f"Missing {INDEXED_LAWS_PATH}")

    law_ids: list[str] = []
    for line in INDEXED_LAWS_PATH.read_text(encoding="utf-8").splitlines():
        match = LAW_ID_ROW_RE.match(line.strip())
        if match:
            law_ids.append(match.group(1))
    return law_ids


def load_law_json_for_id(law_id: str) -> dict:
    path = LAWS_DIR / safe_filename(law_id)
    if not path.exists():
        raise FileNotFoundError(f"Missing scraped law file for {law_id}: {path}")
    law = json.loads(path.read_text(encoding="utf-8"))
    if law.get("error"):
        raise ValueError(f"Law file has error marker for {law_id}: {law.get('error')}")
    return law


def main():
    print("=== Backfill indexed laws into PostgreSQL staging ===")

    if not DATABASE_URL:
        print("✗ DATABASE_URL is not configured in .env")
        return

    try:
        law_ids = parse_indexed_law_ids()
    except Exception as e:
        print(f"✗ Could not parse INDEXED_LAWS.md: {e}")
        return

    if not law_ids:
        print("✗ No law IDs found in INDEXED_LAWS.md")
        return

    print(f"Indexed laws found: {len(law_ids)}")

    try:
        conn = get_postgres_connection()
    except Exception as e:
        print(f"✗ Could not connect to PostgreSQL: {e}")
        return

    try:
        ensure_staging_schema(conn)
    except Exception as e:
        print(f"✗ Failed to initialize staging schema: {e}")
        conn.close()
        return

    staged_laws = 0
    staged_chunks = 0
    missing_files = 0
    failed = 0
    catalogue_map = load_catalogue_map()

    for law_id in law_ids:
        try:
            law = load_law_json_for_id(law_id)
            stage_law_with_sections(conn, law, source_catalogue_entry=catalogue_map.get(law_id))
            chunks = law_to_chunks(law)
            stage_chunks_for_law(conn, law, chunks, mark_qdrant_synced=True)
            upsert_indexed_law(law, len(chunks))
            staged_laws += 1
            staged_chunks += len(chunks)
            print(f"  ✓ {law_id}: {len(chunks)} chunks staged")
        except FileNotFoundError as e:
            missing_files += 1
            print(f"  ✗ {law_id}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ✗ {law_id}: {e}")

    conn.close()

    print("\n=== Backfill done ===")
    print(f"  Laws staged:    {staged_laws}")
    print(f"  Chunks staged:  {staged_chunks}")
    print(f"  Missing files:  {missing_files}")
    print(f"  Failed laws:    {failed}")


if __name__ == "__main__":
    main()
