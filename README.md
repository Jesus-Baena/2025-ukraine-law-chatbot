# rada-rag: Ukrainian Legislation RAG Pipeline

Updatable RAG knowledge base built from Verkhovna Rada legislation.

## Architecture

```
data.rada.gov.ua (catalogue JSON)
        ↓  [1_fetch_catalogue.py]
    catalogue.json  (law IDs + metadata)
        ↓  [2_scrape_laws.py]
    laws/  (raw HTML -> structured JSON per law)
        ↓  [3_chunk_embed.py]
    Qdrant collection  (vectors + payloads)
        ↑  [4_incremental_update.py]  <- run via n8n cron
data.rada.gov.ua (new IDs since last run)
```

## Stack

- Scraper: `requests` + `BeautifulSoup` (`lxml`)
- Extraction: Docling service (`DOCLING_API_URL`) with HTML fallback parser
- Embeddings: `mxbai-embed-large` via Ollama
- Vector store: Qdrant
- RAG query: Ollama chat model
- Orchestration: n8n (incremental updates)

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env: set QDRANT_URL, QDRANT_API_KEY,
# OLLAMA_BASE_URL, OLLAMA_MODEL, EMBED_MODEL, DOCLING_API_URL

# 3. (Optional) run local Qdrant
docker compose up -d
```

Notes:
- `.env` is ignored by git. Commit only placeholder values in `.env.example`.
- `QDRANT_URL` is the canonical variable name.

## Usage

```bash
# Full bootstrap
python 1_fetch_catalogue.py
python 2_scrape_laws.py
python 3_chunk_embed.py

# Incremental update (daily via n8n)
python 4_incremental_update.py

# Query
python 5_query.py "права внутрішньо переміщених осіб"
python 5_query.py "IDP rights during martial law"
```

## Scope Filtering

Set optional filters in `.env`:
- `CATEGORY_FILTER=humanitarian`
- `DATE_FROM=2022-02-24`
- `MAX_LAWS=5000`

## Indexed Laws Tracker

The list of laws currently embedded in the vector collection is maintained in [INDEXED_LAWS.md](INDEXED_LAWS.md), including English titles, law IDs, section counts, and chunk counts.

## Files

| File | Purpose |
|------|---------|
| `1_fetch_catalogue.py` | Download law ID catalogue from open data portal |
| `2_scrape_laws.py` | Scrape full text from zakon.rada.gov.ua |
| `3_chunk_embed.py` | Chunk, embed, upsert to Qdrant |
| `4_incremental_update.py` | Delta updates (new laws since last run) |
| `5_query.py` | RAG query interface |
| `config.py` | Shared config and constants |
| `docker-compose.yml` | Qdrant service |
