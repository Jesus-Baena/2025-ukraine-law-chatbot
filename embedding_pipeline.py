import json
import time
import uuid

import re
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PayloadSchemaType,
    PointStruct,
    TextIndexParams,
    TokenizerType,
    VectorParams,
)

from config import (
    CHUNK_OVERLAP,
    CHUNK_SIZE,
    EMBED_DIM,
    EMBED_MODEL,
    OLLAMA_BASE_URL,
    PASSAGE_PREFIX,
    QDRANT_COLLECTION,
    QUERY_PREFIX,
    REQUEST_TIMEOUT,
)


LOW_SIGNAL_CHUNK_SNIPPETS = [
    "верховна рада україни",
    "законодавство україни",
]


def _is_low_information_chunk(text: str) -> bool:
    normalized = " ".join(text.lower().split())
    if len(normalized) < 20:
        return True

    # Common portal boilerplate without legal body.
    if all(snippet in normalized for snippet in LOW_SIGNAL_CHUNK_SNIPPETS) and len(normalized) < 120:
        return True

    return False


def embed_via_ollama(texts: list[str], max_retries: int = 20, retry_wait: int = 30) -> list[list[float]]:
    """Embed a batch of texts using mxbai-embed-large on Ollama.

    Retries up to max_retries times with retry_wait-second pauses on
    connection errors (the remote Ollama server can go offline temporarily).
    """
    base_url = OLLAMA_BASE_URL.rstrip("/")
    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{base_url}/api/embed",
                json={"model": EMBED_MODEL, "input": texts, "truncate": True},
                timeout=120,
            )
            response.raise_for_status()
            return response.json()["embeddings"]
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt + 1 >= max_retries:
                raise
            print(f"\n  [embed] Ollama unreachable ({exc.__class__.__name__}), "
                  f"retry {attempt + 1}/{max_retries} in {retry_wait}s …", flush=True)
            time.sleep(retry_wait)


def embed_query(query: str) -> list[float]:
    """Embed a single query string with the mxbai retrieval prefix."""
    return embed_via_ollama([QUERY_PREFIX + query])[0]



def setup_qdrant(client: QdrantClient):
    """Create Qdrant collection if it doesn't exist."""
    existing = [collection.name for collection in client.get_collections().collections]
    if QDRANT_COLLECTION in existing:
        print(f"✓ Collection '{QDRANT_COLLECTION}' exists")
        return

    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
    )

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


_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")   # [text](url) → text
_MD_STYLE_RE = re.compile(r"[*_]{1,3}([^*_]+)[*_]{1,3}")  # *italic* / **bold** → text
_MD_MISC_RE = re.compile(r"[`~#>]+")


def _strip_markdown(text: str) -> str:
    """Remove markdown syntax so URLs don't inflate the token count."""
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _MD_STYLE_RE.sub(r"\1", text)
    text = _MD_MISC_RE.sub("", text)
    return text.strip()


def chunk_section(heading: str, text: str, chunk_size: int, overlap: int) -> list[str]:
    """Chunk a section's text into overlapping character-based chunks.

    chunk_size and overlap are in characters.  Splits are aligned to the
    nearest word boundary so we never embed half-words.
    """
    text = _strip_markdown(text)
    if not text:
        return []

    # Reserve space for heading so final chunks stay within chunk_size
    heading_prefix = f"{heading}\n" if heading else ""
    effective_size = max(100, chunk_size - len(heading_prefix))

    chunks = []
    start = 0
    while start < len(text):
        end = min(start + effective_size, len(text))
        # Extend to next whitespace boundary (don't cut mid-word)
        if end < len(text) and not text[end].isspace():
            while end < len(text) and not text[end].isspace():
                end += 1
        chunk_text = text[start:end].strip()
        if heading_prefix:
            chunk_text = heading_prefix + chunk_text
        # Hard cap: if still over limit (long headings + wrapping), trim at word boundary
        if len(chunk_text) > chunk_size:
            chunk_text = chunk_text[:chunk_size]
            last_space = chunk_text.rfind(" ")
            if last_space > 0:
                chunk_text = chunk_text[:last_space]
        chunks.append(chunk_text)
        if end >= len(text):
            break
        # Retreat start by overlap, aligning to a word boundary
        next_start = end - overlap
        if next_start > start:
            # Snap to the start of the nearest word
            while next_start < end and not text[next_start].isspace():
                next_start -= 1
            start = max(start + 1, next_start)
        else:
            start = end  # safety: always advance

    return chunks


def law_to_chunks(law: dict) -> list[dict]:
    """Convert a law JSON object into the chunk format used for embedding."""
    chunks = []
    chunk_index = 0

    for section in law.get("sections", []):
        heading = section.get("heading", "")
        text = section.get("text", "")
        if not text.strip():
            continue

        section_chunks = chunk_section(heading, text, CHUNK_SIZE, CHUNK_OVERLAP)
        for chunk_text in section_chunks:
            if _is_low_information_chunk(chunk_text):
                continue
            chunks.append(
                {
                    "text": chunk_text,
                    "law_id": law["id"],
                    "title": law.get("title", ""),
                    "url": law.get("url", ""),
                    "category": law.get("category", ""),
                    "enacted_date": law.get("enacted_date") or law.get("catalogue_date", ""),
                    "section_heading": heading,
                    "chunk_index": chunk_index,
                }
            )
            chunk_index += 1

    return chunks


def embed_chunks(chunks: list[dict], batch_size: int = 8) -> list[list[float]]:
    """Embed chunk texts in batches using Ollama mxbai-embed-large."""
    texts = [PASSAGE_PREFIX + chunk["text"] for chunk in chunks]
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        all_embeddings.extend(embed_via_ollama(texts[i : i + batch_size]))
    return all_embeddings


def upsert_to_qdrant(
    client: QdrantClient,
    chunks: list[dict],
    embeddings: list[list[float]],
):
    """Upsert chunk vectors and payloads to Qdrant."""
    points = []
    for chunk, vector in zip(chunks, embeddings):
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{chunk['law_id']}:{chunk['chunk_index']}"))
        payload = {key: value for key, value in chunk.items() if key != "text"}
        payload["text"] = chunk["text"]
        points.append(PointStruct(id=point_id, vector=vector, payload=payload))

    # Upload in batches to avoid Qdrant write-timeout on large payloads
    batch_size = 200
    for i in range(0, len(points), batch_size):
        client.upsert(collection_name=QDRANT_COLLECTION, points=points[i:i + batch_size])


def get_processed_ids(client: QdrantClient) -> set[str]:
    """Get the set of law IDs already present in Qdrant."""
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


def delete_law_from_qdrant(client: QdrantClient, law_id: str):
    """Remove all chunks for a law before re-indexing it."""
    client.delete(
        collection_name=QDRANT_COLLECTION,
        points_selector=Filter(
            must=[FieldCondition(key="law_id", match=MatchValue(value=law_id))]
        ),
    )
