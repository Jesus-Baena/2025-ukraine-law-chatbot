import os
import importlib
from pathlib import Path


def load_dotenv():
    try:
        return importlib.import_module("dotenv").load_dotenv()
    except ModuleNotFoundError:
        return False

load_dotenv()


def _first_env(*names: str, default: str = "") -> str:
    """Return the first non-empty environment variable from a list of names."""
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LAWS_DIR = DATA_DIR / "laws"
CATALOGUE_PATH = DATA_DIR / "catalogue.json"
STATE_PATH = DATA_DIR / "state.json"  # tracks last_updated for incremental runs

DATA_DIR.mkdir(exist_ok=True)
LAWS_DIR.mkdir(exist_ok=True)

# Rada open data portal — legislation catalogue (CSV/JSON)
# Full catalogue of all law IDs, titles, dates, categories
CATALOGUE_URL = "https://data.rada.gov.ua/open/data/zak"

# Full-text law base URL
LAW_BASE_URL = "https://zakon.rada.gov.ua/laws/show/{law_id}"

# Qdrant
QDRANT_URL = _first_env("QDRANT_URL", "QDRANT_API_URL", "QDRANT_UR", default="http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "").strip()
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "rada_legislation")

# Docling
DOCLING_API_URL = os.getenv("DOCLING_API_URL", "").strip()

# Embedding model — mxbai-embed-large on Ollama (1024-dim)
EMBED_MODEL = "mxbai-embed-large:latest"
EMBED_DIM = 1024  # mxbai-embed-large output dimension
# mxbai uses a retrieval prefix for queries only; passages need no prefix
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
PASSAGE_PREFIX = ""

# Chunking
CHUNK_SIZE = 400        # characters — safe for mxbai-embed-large on Ollama 0.20.2 (512-token context limit)
CHUNK_OVERLAP = 80      # character overlap between chunks

# Scraping
REQUEST_DELAY = 1.2     # seconds between requests — be polite
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
BATCH_SIZE = 50         # laws per batch before saving progress

# Scope filters (from .env)
DATE_FROM = os.getenv("DATE_FROM", "2000-01-01")   # filter by enactment date
MAX_LAWS = int(os.getenv("MAX_LAWS", "999999"))     # cap for testing
CATEGORY_FILTER = os.getenv("CATEGORY_FILTER", "")  # optional keyword filter
FORCE_RESCRAPE = _env_bool("FORCE_RESCRAPE", default=False)

# Ollama
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma4:e4b").strip()

# Humanitarian-relevant law keywords for category filtering
HUMANITARIAN_KEYWORDS = [
    "внутрішньо переміщен",   # internally displaced
    "біженц",                  # refugees
    "гуманітарн",              # humanitarian
    "воєнний стан",            # martial law
    "соціальний захист",       # social protection
    "допомога",                # assistance
    "евакуац",                 # evacuation
    "цивільн",                 # civilian
    "медичн",                  # medical
    "захист населення",        # population protection
]
