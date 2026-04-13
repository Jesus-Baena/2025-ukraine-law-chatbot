"""
Step 3: Chunk law texts, generate embeddings, upsert to Qdrant

Reads data/laws/*.json, chunks each section, embeds with
multilingual-e5-large, and upserts to Qdrant.

No translation needed — e5-large handles Ukrainian natively.
Cross-lingual queries (English → Ukrainian docs) work out of the box.
"""

import json
import uuid
import torch
from pathlib import Path
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    PayloadSchemaType, TextIndexParams, TokenizerType
)
from config import (
    LAWS_DIR, QDRANT_URL, QDRANT_COLLECTION,
    EMBED_MODEL, EMBED_DIM, CHUNK_SIZE, CHUNK_OVERLAP,
    PASSAGE_PREFIX
)


def load_model() -> SentenceTransformer:
    """Load multilingual-e5-large. Uses GPU if available."""
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Loading {EMBED_MODEL} on {device}...")
    model = SentenceTransformer(EMBED_MODEL, device=device)
    print(f"✓ Model loaded ({device})")
    return model


def setup_qdrant(client: QdrantClient):
    """Create Qdrant collection if it doesn't exist."""
    existing = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION in existing:
        print(f"✓ Collection '{QDRANT_COLLECTION}' exists")
        return

    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
    )

    # Create payload indexes for efficient filtering
    client.create_payload_index(
        collection_name=QDRANT_COLLECTION,
        field_name="law_id",
        field_schema=PayloadSchemaType.KEYWORD,
    )
    client.create_payload_index(
        collection_name=QDRANT_COLLECTION,
        field_name="category",
        field_schema=PayloadSchemaType.KEYWORD,
    )
    client.create_payload_index(
        collection_name=QDRANT_COLLECTION,
        field_name="enacted_date",
        field_schema=PayloadSchemaType.KEYWORD,
    )
    # Full-text index on chunk text for hybrid search
    client.create_payload_index(
        collection_name=QDRANT_COLLECTION,
        field_name="text",
        field_schema=TextIndexParams(
            type="text",
            tokenizer=TokenizerType.WORD,
            min_token_len=2,
            max_token_len=40,
            lowercase=True,
        ),
    )
    print(f"✓ Created collection '{QDRANT_COLLECTION}'")


def chunk_section(heading: str, text: str, chunk_size: int, overlap: int) -> list[str]:
    """
    Chunk a section's text into overlapping word-based chunks.
    Preserves the heading context in each chunk.
    """
    words = text.split()
    if not words:
        return []

    chunks = []
    start = 0
    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunk_words = words[start:end]
        chunk_text = " ".join(chunk_words)

        # Prepend heading for context
        if heading:
            chunk_text = f"{heading}\n{chunk_text}"

        chunks.append(chunk_text)

        if end >= len(words):
            break
        start += chunk_size - overlap  # slide with overlap

    return chunks


def law_to_chunks(law: dict) -> list[dict]:
    """
    Convert a law JSON to a list of chunk dicts ready for embedding.
    
    Each chunk carries:
      - text: the passage text (with section heading)
      - law_id, title, url, category, enacted_date: metadata for retrieval
      - chunk_index: position within the law
      - section_heading: for context display
    """
    chunks = []
    chunk_idx = 0

    for section in law.get("sections", []):
        heading = section.get("heading", "")
        text = section.get("text", "")

        if not text.strip():
            continue

        section_chunks = chunk_section(heading, text, CHUNK_SIZE, CHUNK_OVERLAP)

        for chunk_text in section_chunks:
            if len(chunk_text.strip()) < 20:  # skip very short chunks
                continue
            chunks.append({
                "text": chunk_text,
                "law_id": law["id"],
                "title": law.get("title", ""),
                "url": law.get("url", ""),
                "category": law.get("category", ""),
                "enacted_date": law.get("enacted_date") or law.get("catalogue_date", ""),
                "section_heading": heading,
                "chunk_index": chunk_idx,
            })
            chunk_idx += 1

    return chunks


def embed_chunks(model: SentenceTransformer, chunks: list[dict],
                 batch_size: int = 32) -> list[list[float]]:
    """
    Embed chunk texts using e5-large passage prefix.
    e5 models require "passage: " prefix for documents,
    "query: " prefix for queries.
    """
    texts = [PASSAGE_PREFIX + c["text"] for c in chunks]
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        normalize_embeddings=True,  # cosine similarity
    )
    return embeddings.tolist()


def upsert_to_qdrant(client: QdrantClient, chunks: list[dict],
                     embeddings: list[list[float]]):
    """Upsert chunk vectors + payloads to Qdrant."""
    points = []
    for chunk, vector in zip(chunks, embeddings):
        point_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{chunk['law_id']}:{chunk['chunk_index']}"
        ))
        payload = {k: v for k, v in chunk.items() if k != "text"}
        payload["text"] = chunk["text"]  # store text in payload for retrieval

        points.append(PointStruct(
            id=point_id,
            vector=vector,
            payload=payload,
        ))

    client.upsert(
        collection_name=QDRANT_COLLECTION,
        points=points,
    )


def get_processed_ids(client: QdrantClient) -> set[str]:
    """Get set of law IDs already in Qdrant (for resumability)."""
    processed = set()
    offset = None
    while True:
        result, next_offset = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=None,
            limit=1000,
            offset=offset,
            with_payload=["law_id"],
            with_vectors=False,
        )
        for point in result:
            if point.payload and "law_id" in point.payload:
                processed.add(point.payload["law_id"])
        if next_offset is None:
            break
        offset = next_offset
    return processed


def main():
    print("=== Step 3: Chunking, embedding, upserting to Qdrant ===\n")

    # Load all valid law files
    law_files = [
        f for f in LAWS_DIR.glob("*.json")
        if not json.loads(f.read_text(encoding="utf-8")).get("error")
    ]
    print(f"Valid law files: {len(law_files)}")

    if not law_files:
        print("✗ No valid law files found. Run 2_scrape_laws.py first.")
        return

    # Setup Qdrant
    client = QdrantClient(url=QDRANT_URL)
    setup_qdrant(client)

    # Check what's already processed (resumability)
    try:
        already_done = get_processed_ids(client)
        print(f"Already in Qdrant: {len(already_done)} laws")
    except Exception:
        already_done = set()

    todo_files = [
        f for f in law_files
        if json.loads(f.read_text(encoding="utf-8")).get("id") not in already_done
    ]
    print(f"To process: {len(todo_files)}\n")

    if not todo_files:
        print("✓ All laws already embedded.")
        return

    # Load embedding model
    model = load_model()
    print()

    # Process
    total_chunks = 0
    EMBED_BATCH = 20  # laws per embedding batch

    for i in tqdm(range(0, len(todo_files), EMBED_BATCH), desc="Embedding batches"):
        batch_files = todo_files[i:i + EMBED_BATCH]
        batch_chunks = []

        for f in batch_files:
            law = json.loads(f.read_text(encoding="utf-8"))
            chunks = law_to_chunks(law)
            batch_chunks.extend(chunks)

        if not batch_chunks:
            continue

        # Embed
        embeddings = embed_chunks(model, batch_chunks)

        # Upsert
        upsert_to_qdrant(client, batch_chunks, embeddings)
        total_chunks += len(batch_chunks)

    # Final stats
    collection_info = client.get_collection(QDRANT_COLLECTION)
    print(f"\n=== Done ===")
    print(f"  Total chunks upserted: {total_chunks}")
    print(f"  Qdrant collection size: {collection_info.points_count} vectors")
    print(f"  Collection: {QDRANT_COLLECTION} @ {QDRANT_URL}")


if __name__ == "__main__":
    main()
