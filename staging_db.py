import hashlib
import json
import uuid
from datetime import datetime, timezone

import psycopg  # type: ignore[import-not-found]

from config import DATABASE_URL, STAGING_STORE_RAW_JSON


def get_postgres_connection() -> psycopg.Connection:
    """Create a PostgreSQL connection for staging storage."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(DATABASE_URL)


def ensure_staging_schema(conn: psycopg.Connection):
    """Create staging tables and indexes when missing."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rada_raw_laws (
                law_id TEXT PRIMARY KEY,
                source_url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'law_html',
                http_status INTEGER,
                response_headers JSONB,
                response_body TEXT NOT NULL,
                response_sha256 TEXT NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rada_staging_laws (
                law_id TEXT PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL DEFAULT '',
                law_number TEXT,
                enacted_date TEXT,
                status TEXT,
                issuer TEXT,
                category TEXT,
                catalogue_date TEXT,
                extraction_mode TEXT NOT NULL DEFAULT 'unknown',
                section_count INTEGER NOT NULL DEFAULT 0,
                content_hash TEXT NOT NULL,
                raw_json JSONB,
                source_catalogue_json JSONB,
                needs_chunking BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE rada_staging_laws ALTER COLUMN raw_json DROP NOT NULL")
        cur.execute("ALTER TABLE rada_staging_laws ADD COLUMN IF NOT EXISTS source_catalogue_json JSONB")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rada_staging_sections (
                id BIGSERIAL PRIMARY KEY,
                law_id TEXT NOT NULL REFERENCES rada_staging_laws(law_id) ON DELETE CASCADE,
                section_index INTEGER NOT NULL,
                heading TEXT,
                text TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(law_id, section_index)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rada_staging_chunks (
                chunk_id TEXT PRIMARY KEY,
                law_id TEXT NOT NULL REFERENCES rada_staging_laws(law_id) ON DELETE CASCADE,
                chunk_index INTEGER NOT NULL,
                section_heading TEXT,
                text TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                title TEXT,
                url TEXT,
                category TEXT,
                enacted_date TEXT,
                qdrant_synced BOOLEAN NOT NULL DEFAULT FALSE,
                qdrant_point_id TEXT,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(law_id, chunk_index)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rada_pipeline_state (
                id INTEGER PRIMARY KEY,
                last_incremental_run TEXT,
                total_laws_processed INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            INSERT INTO rada_pipeline_state (id)
            VALUES (1)
            ON CONFLICT (id) DO NOTHING
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rada_staging_laws_needs_chunking ON rada_staging_laws(needs_chunking)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rada_staging_sections_law_id ON rada_staging_sections(law_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rada_staging_chunks_sync ON rada_staging_chunks(qdrant_synced)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rada_raw_laws_fetched_at ON rada_raw_laws(fetched_at)")

    conn.commit()


def _content_hash_for_sections(sections: list[dict]) -> str:
    encoded = json.dumps(sections, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stage_raw_law_response(
    conn: psycopg.Connection,
    *,
    law_id: str,
    source_url: str,
    response_body: str,
    http_status: int | None = None,
    response_headers: dict | None = None,
    source_kind: str = "law_html",
) -> str:
    """Persist raw source payload first and return the stored response body."""
    now = datetime.now(timezone.utc)
    response_sha256 = hashlib.sha256(response_body.encode("utf-8")).hexdigest()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rada_raw_laws (
                law_id, source_url, source_kind, http_status,
                response_headers, response_body, response_sha256,
                fetched_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            ON CONFLICT (law_id) DO UPDATE SET
                source_url = EXCLUDED.source_url,
                source_kind = EXCLUDED.source_kind,
                http_status = EXCLUDED.http_status,
                response_headers = EXCLUDED.response_headers,
                response_body = EXCLUDED.response_body,
                response_sha256 = EXCLUDED.response_sha256,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
            RETURNING response_body
            """,
            (
                law_id,
                source_url,
                source_kind,
                http_status,
                json.dumps(response_headers or {}, ensure_ascii=False),
                response_body,
                response_sha256,
                now,
                now,
            ),
        )
        stored_body = cur.fetchone()[0]

    conn.commit()
    return stored_body


def stage_law_with_sections(conn: psycopg.Connection, law: dict, source_catalogue_entry: dict | None = None):
    """Persist a scraped law and all its sections to PostgreSQL staging tables."""
    now = datetime.now(timezone.utc)
    sections = law.get("sections", [])
    content_hash = _content_hash_for_sections(sections)
    raw_json_value = json.dumps(law, ensure_ascii=False) if STAGING_STORE_RAW_JSON else None
    source_catalogue_json_value = (
        json.dumps(source_catalogue_entry, ensure_ascii=False)
        if source_catalogue_entry is not None
        else None
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rada_staging_laws (
                law_id, title, url, law_number, enacted_date, status, issuer,
                category, catalogue_date, extraction_mode, section_count,
                content_hash, raw_json, source_catalogue_json, needs_chunking, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, TRUE, %s, %s)
            ON CONFLICT (law_id) DO UPDATE SET
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                law_number = EXCLUDED.law_number,
                enacted_date = EXCLUDED.enacted_date,
                status = EXCLUDED.status,
                issuer = EXCLUDED.issuer,
                category = EXCLUDED.category,
                catalogue_date = EXCLUDED.catalogue_date,
                extraction_mode = EXCLUDED.extraction_mode,
                section_count = EXCLUDED.section_count,
                content_hash = EXCLUDED.content_hash,
                raw_json = EXCLUDED.raw_json,
                source_catalogue_json = COALESCE(EXCLUDED.source_catalogue_json, rada_staging_laws.source_catalogue_json),
                needs_chunking = TRUE,
                updated_at = EXCLUDED.updated_at
            """,
            (
                law["id"],
                law.get("title", ""),
                law.get("url", ""),
                law.get("law_number"),
                law.get("enacted_date"),
                law.get("status"),
                law.get("issuer"),
                law.get("category", ""),
                law.get("catalogue_date", ""),
                law.get("extraction_mode", "unknown"),
                int(law.get("section_count", len(sections))),
                content_hash,
                raw_json_value,
                source_catalogue_json_value,
                now,
                now,
            ),
        )

        # Replace prior section snapshot atomically so section indexes stay exact.
        cur.execute("DELETE FROM rada_staging_sections WHERE law_id = %s", (law["id"],))

        section_rows = []
        for section_index, section in enumerate(sections):
            section_text = section.get("text", "")
            text_hash = hashlib.sha256(section_text.encode("utf-8")).hexdigest()
            section_rows.append(
                (
                    law["id"],
                    section_index,
                    section.get("heading", ""),
                    section_text,
                    text_hash,
                    now,
                    now,
                )
            )

        cur.executemany(
            """
            INSERT INTO rada_staging_sections (
                law_id, section_index, heading, text, text_hash, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            section_rows,
        )

    conn.commit()


def stage_chunks_for_law(conn: psycopg.Connection, law: dict, chunks: list[dict], mark_qdrant_synced: bool):
    """Persist chunk rows for a law and optionally mark them as already synced to Qdrant."""
    now = datetime.now(timezone.utc)
    law_id = law["id"]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM rada_staging_chunks WHERE law_id = %s", (law_id,))

        chunk_rows = []
        for chunk in chunks:
            chunk_index = int(chunk["chunk_index"])
            chunk_text = chunk.get("text", "")
            text_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
            qdrant_point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{law_id}:{chunk_index}"))
            chunk_id = f"{law_id}:{chunk_index}"
            chunk_rows.append(
                (
                    chunk_id,
                    law_id,
                    chunk_index,
                    chunk.get("section_heading", ""),
                    chunk_text,
                    text_hash,
                    chunk.get("title", ""),
                    chunk.get("url", ""),
                    chunk.get("category", ""),
                    chunk.get("enacted_date", ""),
                    mark_qdrant_synced,
                    qdrant_point_id,
                    now,
                    now,
                )
            )

        cur.executemany(
            """
            INSERT INTO rada_staging_chunks (
                chunk_id, law_id, chunk_index, section_heading, text, text_hash,
                title, url, category, enacted_date, qdrant_synced, qdrant_point_id,
                last_error, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s)
            """,
            chunk_rows,
        )

    conn.commit()
