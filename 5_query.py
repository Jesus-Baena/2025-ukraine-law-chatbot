"""
Step 5: RAG query interface

Retrieves relevant law chunks from Qdrant and uses Ollama to answer.

Cross-lingual: query in English → retrieves Ukrainian law text → Ollama answers.
Or query in Ukrainian directly.

Usage:
  python 5_query.py "права внутрішньо переміщених осіб"
  python 5_query.py "IDP rights during martial law"
  python 5_query.py --filter-date 2022-02-24 "compensation for destroyed housing"
"""

import sys
import argparse
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range

from config import (
    QDRANT_COLLECTION, EMBED_MODEL,
    QUERY_PREFIX, OLLAMA_BASE_URL, OLLAMA_MODEL, REQUEST_TIMEOUT
)
from service_clients import get_qdrant_client
from embedding_pipeline import embed_query


TOP_K = 6  # number of chunks to retrieve

SYSTEM_PROMPT = """You are a legal research assistant specializing in Ukrainian legislation.
You answer questions about Ukrainian law based on retrieved legal text excerpts.

Guidelines:
- Base your answer strictly on the provided legal excerpts
- Cite the specific law title and article/section when possible  
- If the excerpts don't fully answer the question, say so clearly
- You may answer in English even if the source texts are in Ukrainian
- Note the enactment date of relevant laws, especially for martial law context
- Be precise about legal rights, obligations, and procedures"""


def preflight_ollama() -> None:
    """Validate Ollama connectivity and confirm the configured model is available."""
    base_url = OLLAMA_BASE_URL.rstrip("/")
    timeout = max(20, REQUEST_TIMEOUT)

    try:
        response = requests.get(f"{base_url}/api/tags", timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            "Ollama is unreachable. Check OLLAMA_BASE_URL and network access. "
            f"Endpoint: {base_url}/api/tags"
        ) from exc

    data = response.json() if response.content else {}
    models = data.get("models", []) if isinstance(data, dict) else []
    available = set()
    for model in models:
        if isinstance(model, dict) and model.get("name"):
            available.add(model["name"])

    if available and OLLAMA_MODEL not in available:
        example = ", ".join(sorted(list(available))[:5])
        raise RuntimeError(
            f"Configured OLLAMA_MODEL '{OLLAMA_MODEL}' is not available on the server. "
            f"Available models: {example}"
        )

    if available and EMBED_MODEL not in available:
        example = ", ".join(sorted(list(available))[:5])
        raise RuntimeError(
            f"Configured EMBED_MODEL '{EMBED_MODEL}' is not available on the server. "
            f"Available models: {example}"
        )


def retrieve(query: str,
             client: QdrantClient, date_from: str = None,
             category: str = None, top_k: int = TOP_K) -> list[dict]:
    """
    Retrieve top-K relevant chunks from Qdrant.
    
    Embeds the query via Ollama mxbai-embed-large.
    Optional filters: date_from, category.
    """
    # Embed the query with the mxbai retrieval prefix
    query_vector = embed_query(query)

    # Build optional filters
    filters = []
    if date_from:
        filters.append(FieldCondition(
            key="enacted_date",
            range=Range(gte=date_from)
        ))
    if category:
        from qdrant_client.models import MatchValue
        filters.append(FieldCondition(
            key="category",
            match=MatchValue(value=category)
        ))

    search_filter = Filter(must=filters) if filters else None

    if hasattr(client, "query_points"):
        query_result = client.query_points(
            collection_name=QDRANT_COLLECTION,
            query=query_vector,
            query_filter=search_filter,
            limit=top_k,
            with_payload=True,
        )
        results = query_result.points
    else:
        results = client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=query_vector,
            query_filter=search_filter,
            limit=top_k,
            with_payload=True,
        )

    chunks = []
    for hit in results:
        p = hit.payload
        chunks.append({
            "score": round(hit.score, 3),
            "text": p.get("text", ""),
            "title": p.get("title", ""),
            "law_id": p.get("law_id", ""),
            "url": p.get("url", ""),
            "enacted_date": p.get("enacted_date", ""),
            "section_heading": p.get("section_heading", ""),
        })

    return chunks


def format_context(chunks: list[dict]) -> str:
    """Format retrieved chunks into a context block for the LLM."""
    parts = []
    for i, c in enumerate(chunks, 1):
        heading = f" — {c['section_heading']}" if c['section_heading'] else ""
        source = f"[{i}] {c['title']}{heading} (enacted: {c['enacted_date'] or 'n/a'}, score: {c['score']})"
        parts.append(f"{source}\n{c['text']}\nURL: {c['url']}")
    return "\n\n---\n\n".join(parts)


def ask_ollama(query: str, context: str) -> str:
    """Send query + retrieved context to Ollama for answer generation."""
    prompt = f"""Based on the following excerpts from Ukrainian legislation, please answer this question:

Question: {query}

Retrieved legal excerpts:

{context}

Please provide a clear, accurate answer citing the relevant laws and articles."""

    base_url = OLLAMA_BASE_URL.rstrip("/")
    request_timeout = max(60, REQUEST_TIMEOUT * 3)

    payload = {
        "model": OLLAMA_MODEL,
        "system": SYSTEM_PROMPT,
        "prompt": prompt,
        "stream": False,
    }
    response = requests.post(
        f"{base_url}/api/generate",
        json=payload,
        timeout=request_timeout,
    )
    response.raise_for_status()
    data = response.json()
    answer = data.get("response", "").strip()
    if not answer:
        raise RuntimeError("Ollama returned an empty response")
    return answer


def main():
    parser = argparse.ArgumentParser(description="Query Ukrainian legislation RAG")
    parser.add_argument("query", nargs="+", help="Your legal question")
    parser.add_argument("--filter-date", help="Only laws enacted after this date (YYYY-MM-DD)")
    parser.add_argument("--category", help="Filter by law category")
    parser.add_argument("--top-k", type=int, default=TOP_K, help="Number of chunks to retrieve")
    parser.add_argument("--show-sources", action="store_true", help="Print retrieved chunks")
    args = parser.parse_args()

    query = " ".join(args.query)
    print(f"\n🔍 Query: {query}")
    if args.filter_date:
        print(f"   Date filter: ≥ {args.filter_date}")

    # Preflight checks before loading heavy local models
    print("\nChecking Ollama availability...")
    preflight_ollama()
    print(f"Ollama reachable at {OLLAMA_BASE_URL}, chat={OLLAMA_MODEL}, embed={EMBED_MODEL}")

    # Client
    client = get_qdrant_client()

    # Retrieve
    print("Retrieving relevant law excerpts...")
    chunks = retrieve(
        query, client,
        date_from=args.filter_date,
        category=args.category,
        top_k=args.top_k
    )

    if not chunks:
        print("✗ No relevant excerpts found.")
        return

    print(f"Found {len(chunks)} relevant chunks (top score: {chunks[0]['score']})")

    if args.show_sources:
        print("\n--- Retrieved excerpts ---")
        for c in chunks:
            print(f"\n[score={c['score']}] {c['title']}")
            print(f"  {c['text'][:200]}...")

    # Generate answer
    print("\nGenerating answer with Ollama...\n")
    context = format_context(chunks)
    answer = ask_ollama(query, context)

    print("=" * 60)
    print(answer)
    print("=" * 60)

    # Show source URLs
    print("\nSources:")
    seen = set()
    for c in chunks:
        if c["url"] not in seen:
            print(f"  • {c['title']} — {c['url']}")
            seen.add(c["url"])


if __name__ == "__main__":
    main()
