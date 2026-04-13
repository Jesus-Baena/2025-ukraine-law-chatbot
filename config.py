import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

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
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "rada_legislation")

# Embedding model — multilingual, Ukrainian-native, no translation needed
EMBED_MODEL = "intfloat/multilingual-e5-large"
EMBED_DIM = 1024  # multilingual-e5-large output dimension
# Prefix required by e5 models
QUERY_PREFIX = "query: "
PASSAGE_PREFIX = "passage: "

# Chunking
CHUNK_SIZE = 400        # tokens — below 512 limit of e5-large
CHUNK_OVERLAP = 50      # token overlap between chunks

# Scraping
REQUEST_DELAY = 1.2     # seconds between requests — be polite
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
BATCH_SIZE = 50         # laws per batch before saving progress

# Scope filters (from .env)
DATE_FROM = os.getenv("DATE_FROM", "2000-01-01")   # filter by enactment date
MAX_LAWS = int(os.getenv("MAX_LAWS", "999999"))     # cap for testing
CATEGORY_FILTER = os.getenv("CATEGORY_FILTER", "")  # optional keyword filter

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
LLM_MODEL = "claude-sonnet-4-20250514"

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
