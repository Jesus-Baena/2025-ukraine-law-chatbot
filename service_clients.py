import os
from urllib.parse import urlparse

import httpx
from qdrant_client import QdrantClient

from config import DOCLING_API_URL, QDRANT_API_KEY, QDRANT_URL


def get_qdrant_client() -> QdrantClient:
    """Create a Qdrant client using env-provided authentication when present.

    Supports two auth layers:
    - QDRANT_USER / QDRANT_PASS: HTTP Basic auth for a reverse proxy in front of Qdrant
    - QDRANT_API_KEY: Qdrant's own API key header (passed through the proxy)
    """
    if not QDRANT_URL:
        raise RuntimeError("QDRANT_URL is not configured")

    qdrant_user = os.getenv("QDRANT_USER", "").strip()
    qdrant_pass = os.getenv("QDRANT_PASS", "").strip()

    parsed = urlparse(QDRANT_URL)
    if not parsed.scheme or not parsed.hostname:
        raise RuntimeError("QDRANT_URL must include scheme and hostname, e.g. https://host")

    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or (443 if parsed.scheme == "https" else 80),
        "https": parsed.scheme == "https",
        "check_compatibility": False,
    }
    if QDRANT_API_KEY:
        kwargs["api_key"] = QDRANT_API_KEY
    if qdrant_user or qdrant_pass:
        kwargs["http_client"] = httpx.Client(auth=(qdrant_user, qdrant_pass))
    return QdrantClient(**kwargs)


def require_docling_url() -> str:
    """Return the configured Docling endpoint or fail clearly."""
    if not DOCLING_API_URL:
        raise RuntimeError("DOCLING_API_URL is not configured")
    return DOCLING_API_URL
