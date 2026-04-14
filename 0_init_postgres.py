"""
Initialize PostgreSQL staging schema for the ingestion pipeline.

This creates tables used to store extracted law text and metadata before
chunk embedding and Qdrant upsert.
"""

from config import DATABASE_URL
from staging_db import ensure_staging_schema, get_postgres_connection


def main():
    print("=== Init PostgreSQL staging schema ===")

    if not DATABASE_URL:
        print("✗ DATABASE_URL is not configured in .env")
        return

    try:
        conn = get_postgres_connection()
    except Exception as e:
        print(f"✗ Could not connect to PostgreSQL: {e}")
        return

    try:
        ensure_staging_schema(conn)
    except Exception as e:
        print(f"✗ Failed to initialize staging schema: {e}")
        return
    finally:
        conn.close()

    print("✓ Staging schema is ready")


if __name__ == "__main__":
    main()
