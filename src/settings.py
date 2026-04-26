# --- Scraper ---
URL = "https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/"
ADS_PER_PAGE = 5
MAX_CONCURRENT_TABS = 3
MAX_CONCURRENT_CATALOG_PAGES = 3

# --- Browser ---
VIEWPORT = {'width': 1280, 'height': 800}
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# --- Timeouts (ms) ---
PAGE_LOAD_TIMEOUT = 20000
CATALOG_LOAD_TIMEOUT = 60000
SELECTOR_TIMEOUT = 15000
ELEMENT_TIMEOUT = 2000

# --- Google Sheets ---
SERVICE_ACCOUNT_FILE = "service_account_creds.json"
SHEET_NAME = "OLX_RealEstate_Data"
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]