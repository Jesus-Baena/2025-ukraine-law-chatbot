"""
Step 5: RAG query interface

Retrieves relevant law chunks from Qdrant and uses Claude to answer.

Cross-lingual: query in English → retrieves Ukrainian law text → Claude answers.
Or query in Ukrainian directly.

Usage:
  python 5_query.py "права внутрішньо переміщених осіб"
  python 5_query.py "IDP rights during martial law"
  python 5_query.py --filter-date 2022-02-24 "compensation for destroyed housing"
"""

import sys
import argparse
import anthropic
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range

from config import (
    QDRANT_URL, QDRANT_COLLECTION, EMBED_MODEL,
    QUERY_PREFIX, ANTHROPIC_API_KEY, LLM_MODEL
)


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


def retrieve(query: str, model: SentenceTransformer,
             client: QdrantClient, date_from: str = None,
             category: str = None, top_k: int = TOP_K) -> list[dict]:
    """
    Retrieve top-K relevant chunks from Qdrant.
    
    Uses e5 query prefix for semantic search.
    Optional filters: date_from, category.
    """
    # Embed the query with "query: " prefix (required by e5 models)
    query_vector = model.encode(
        QUERY_PREFIX + query,
        normalize_embeddings=True
    ).tolist()

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


def ask_claude(query: str, context: str) -> str:
    """Send query + retrieved context to Claude for answer generation."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model=LLM_MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": f"""Based on the following excerpts from Ukrainian legislation, please answer this question:

**Question:** {query}

**Retrieved legal excerpts:**

{context}

Please provide a clear, accurate answer citing the relevant laws and articles."""
        }]
    )

    return message.content[0].text


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

    # Load model and client
    print("\nLoading embedding model...")
    model = SentenceTransformer(EMBED_MODEL)
    client = QdrantClient(url=QDRANT_URL)

    # Retrieve
    print("Retrieving relevant law excerpts...")
    chunks = retrieve(
        query, model, client,
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
    print("\nGenerating answer with Claude...\n")
    context = format_context(chunks)
    answer = ask_claude(query, context)

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
