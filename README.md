# OLX Real Estate ETL Pipeline

## Overview
An asynchronous ETL pipeline that scrapes, cleans, and loads real estate listings from OLX Ukraine to Google Sheets. Designed to be resilient to layout changes and basic bot-detection systems.

## Tech Stack
| Layer | Tool | Purpose |
|---|---|---|
| **Extract** | `requests` + `BeautifulSoup` (catalog), `Playwright` (ads) | HTTP for listing pages, browser automation for individual ad pages |
| **Transform** | `Pandas`, `re` | Data cleaning, regex parsing, missing value handling |
| **Load** | `gspread`, `Google Sheets API` | Automated cloud upload |
| **Concurrency** | `asyncio` | Parallel catalog and ad page fetches with semaphore-based load control |
| **Runtime** | `Python 3.11+` | |

## Project Structure
```
olx_scraper_etl/
├── main.py                  # Entry point — orchestrates the ETL pipeline
├── requirements.txt
├── service_account_creds.json   # Google service account key (git-ignored)
└── src/
    ├── scraper.py           # Extract phase: requests+BeautifulSoup for catalog, Playwright for ads
    ├── transformer.py       # Transform phase: cleaning, parsing, fallback logic
    ├── loader.py            # Load phase: Google Sheets upload
    ├── settings.py          # All configurable constants
    └── utils/
        └── logger.py        # Logging setup (reads LOG_LEVEL env var)
```

## Implementation Details

### 1. Hybrid Scraping Architecture
**Catalog pages (requests + BeautifulSoup):**
- Lightweight HTTP requests to fetch catalog pages
- BeautifulSoup parses static HTML to extract ad links
- Limited by `MAX_CONCURRENT_CATALOG_PAGES` semaphore, executed in thread pool via `asyncio.run_in_executor()`

**Individual ads (Playwright):**
- Async Playwright page loads for each ad to extract title, price, location, and full page text
- Headless browser execution for JavaScript-enabled rendering and Cloudflare bypass
- Concurrency controlled via `asyncio.Semaphore` (up to `MAX_CONCURRENT_TABS` concurrent tabs)

**Result:** Optimized extraction (lightweight catalog fetches, full-featured ad pages), resilient to bot detection, controlled concurrency.

### 2. Concurrency Control
Two independent async semaphores manage concurrency:
- **Catalog fetches:** `asyncio.Semaphore(MAX_CONCURRENT_CATALOG_PAGES)` — limits parallel HTTP requests via thread pool executor
- **Ad fetches:** `asyncio.Semaphore(MAX_CONCURRENT_TABS)` — limits parallel Playwright browser tabs

This throttles request rate to avoid Cloudflare HTTP 429 blocks while maintaining high throughput. Both limits can be scaled up when a Proxy Pool is added.

### 3. Graceful Degradation & Fallback
If primary CSS/testid selectors fail to extract a field, a text-mining fallback activates: the full page body text is stored during scraping, and `transformer.py` uses regex patterns to recover price, area, floor, and location. Handles A/B design tests and dynamic CSS class names.

### 4. Data Quality & Error Handling
- **Failed parses included in output:** Ads with critical errors are marked with `title="ERROR"` and error message in `full_text`. This allows quality assessment of how many ads failed to parse.
- **Prices stripped and cast to `float`**
- **Currency extracted and stored in a separate column** (UAH / USD / EUR)
- **Location parsed to city name only** (street addresses filtered out)

## Configuration

All constants are in `src/settings.py`:

| Constant | Default | Description |
|---|---|---|
| `URL` | OLX Kyiv apartments | Base catalog URL to scrape |
| `ADS_PER_PAGE` | `5` | Max ads collected per catalog page |
| `MAX_CONCURRENT_TABS` | `3` | Parallel ad pages open at once |
| `MAX_CONCURRENT_CATALOG_PAGES` | `3` | Parallel catalog pages open at once |
| `PAGE_LOAD_TIMEOUT` | `20000` ms | Timeout for a single ad page |
| `CATALOG_LOAD_TIMEOUT` | `60000` ms | Timeout for a catalog page |
| `SELECTOR_TIMEOUT` | `15000` ms | Timeout waiting for a CSS selector |
| `ELEMENT_TIMEOUT` | `2000` ms | Timeout for reading a single element |
| `SHEET_NAME` | `OLX_RealEstate_Data` | Target Google Spreadsheet name |

## Environment Variables

| Variable | Values | Default | Description |
|---|---|---|---|
| `LOG_LEVEL` | `DEBUG` `INFO` `WARNING` `ERROR` | `INFO` | Log verbosity |

## Setup & Run

**Prerequisites:** Python 3.11+

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd olx_scraper_etl
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

3. Place `service_account_creds.json` (Google Cloud Service Account Key) in the project root.

4. Share your Google Spreadsheet with the service account email address (Editor access).

5. Run the pipeline:
   ```bash
   python main.py

   # with debug logging
   LOG_LEVEL=DEBUG python main.py
   ```
