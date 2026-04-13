# rada-rag: Ukrainian Legislation RAG Pipeline

Updatable RAG knowledge base built from Verkhovna Rada legislation.

## Architecture

```
data.rada.gov.ua (catalogue JSON)
        ↓  [1_fetch_catalogue.py]
    catalogue.json  (~150k law IDs + metadata)
        ↓  [2_scrape_laws.py]
    laws/  (raw HTML → structured JSON per law)
        ↓  [3_chunk_embed.py]
    Qdrant collection  (vectors + payloads)
        ↑  [4_incremental_update.py]  ← run via n8n cron
data.rada.gov.ua (new IDs since last run)
```

## Stack

- **Scraper**: `requests` + `BeautifulSoup` (lxml)
- **Embeddings**: `intfloat/multilingual-e5-large` (Ukrainian-native, no translation)
- **Vector store**: Qdrant (Docker)
- **RAG query**: LangChain + Claude via Anthropic API
- **Orchestration**: n8n (incremental updates)

## Setup

```bash
# 1. Start Qdrant
docker run -d -p 6333:6333 -v qdrant_storage:/qdrant/storage qdrant/qdrant

# 2. Install Python deps
pip install requests beautifulsoup4 lxml sentence-transformers qdrant-client \
            langchain langchain-anthropic tqdm python-dotenv

# 3. Configure
cp .env.example .env
# Edit .env: set ANTHROPIC_API_KEY, QDRANT_URL, optional CATEGORY_FILTER
```

## Usage

```bash
# Full bootstrap (one-time)
python 1_fetch_catalogue.py          # ~2 min
python 2_scrape_laws.py              # hours depending on scope
python 3_chunk_embed.py              # 4h CPU / 10min GPU

# Incremental update (run daily via n8n)
python 4_incremental_update.py

# Query
python 5_query.py "права внутрішньо переміщених осіб"
python 5_query.py "IDP rights during martial law"  # cross-lingual works
```

## Scope filtering

Edit `.env` to limit scraping scope:
- `CATEGORY_FILTER=humanitarian` — only humanitarian/IDP-relevant laws
- `DATE_FROM=2022-02-24` — only post-invasion legislation
- `MAX_LAWS=5000` — cap for testing

## Files

| File | Purpose |
|------|---------|
| `1_fetch_catalogue.py` | Download law ID catalogue from open data portal |
| `2_scrape_laws.py` | Scrape full text from zakon.rada.gov.ua |
| `3_chunk_embed.py` | Chunk, embed, upsert to Qdrant |
| `4_incremental_update.py` | Delta updates (new laws since last run) |
| `5_query.py` | RAG query interface |
| `config.py` | Shared config and constants |
| `docker-compose.yml` | Qdrant + optional Qdrant Dashboard |
