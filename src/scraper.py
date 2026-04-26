import asyncio
import logging
import random
from playwright.async_api import async_playwright, BrowserContext
from src.settings import (
    URL, MAX_CONCURRENT_TABS, MAX_CONCURRENT_CATALOG_PAGES, ADS_PER_PAGE,
    VIEWPORT, USER_AGENT,
    PAGE_LOAD_TIMEOUT, CATALOG_LOAD_TIMEOUT, SELECTOR_TIMEOUT, ELEMENT_TIMEOUT
)

logger = logging.getLogger(__name__)


async def fetch_total_pages(page) -> int:
    """Determines the total number of catalog pages from the pagination element.

    Reads the ``aria-label`` attribute of the last
    ``[data-testid="pagination-list-item"]`` element. OLX sets this attribute
    to ``"Page N"``, where N is the last available page number.

    Args:
        page (playwright.async_api.Page): An already-loaded Playwright page
            object pointing to the first catalog page.

    Returns:
        int: Total number of pages. Returns ``1`` if pagination is not found
            or cannot be parsed.

    Example:
        Input:  page loaded at https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/
                last pagination element has aria-label="Page 25"
        Output: 25
    """
    try:
        last_item = page.locator('[data-testid="pagination-list-item"]').last
        aria_label = await last_item.get_attribute('aria-label', timeout=SELECTOR_TIMEOUT)
        # aria-label format is "Page 25"
        total = int(aria_label.split()[-1])
        logger.info("Total catalog pages found: %d", total)
        return total
    except Exception as e:
        logger.warning("Could not determine page count: %s. Defaulting to 1.", e)
    return 1


async def fetch_catalog_page(context: BrowserContext, page_num: int, semaphore: asyncio.Semaphore) -> list:
    """Loads a single catalog page and returns a list of ad URLs.

    Opens a new browser tab, navigates to the catalog page, waits for ad cards
    to appear, then extracts up to ``ADS_PER_PAGE`` links. The tab is always
    closed in the ``finally`` block, even on error.

    Args:
        context (BrowserContext): Shared Playwright browser context used to
            open new tabs.
        page_num (int): Catalog page number to load. Page 1 uses the base URL;
            pages 2+ append ``?page=N``.
        semaphore (asyncio.Semaphore): Limits the number of catalog pages
            opened simultaneously to avoid rate-limiting (HTTP 429).

    Returns:
        list[str]: List of absolute ad URLs collected from this page.
            Returns an empty list ``[]`` if the page fails to load.

    Example:
        Input:  page_num=2, ADS_PER_PAGE=5
        Output: [
                  "https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/d/ad-title-1234.html",
                  "https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/d/ad-title-5678.html",
                  ...  # up to 5 links
                ]
    """
    async with semaphore:
        page = await context.new_page()
        try:
            # Page 1 uses the base URL, pages 2+ use the ?page=N parameter
            current_url = URL if page_num == 1 else f"{URL}?page={page_num}"
            logger.info("Loading catalog (page %d): %s", page_num, current_url)

            await page.goto(current_url, wait_until="domcontentloaded", timeout=CATALOG_LOAD_TIMEOUT)
            await page.wait_for_selector('div[data-cy="l-card"]', timeout=SELECTOR_TIMEOUT)
            await asyncio.sleep(random.uniform(1, 3))

            cards = await page.locator('div[data-cy="l-card"]').all()

            links = []
            for card in cards[:ADS_PER_PAGE]:
                href = await card.locator('a').first.get_attribute('href')
                if href and href.startswith('/'):
                    href = "https://www.olx.ua" + href
                if href and href not in links:
                    links.append(href)

            logger.info("Parsed %d links from page %d.", len(links), page_num)
            return links

        except Exception as e:
            logger.error("Error loading catalog page %d: %s", page_num, e)
            return []

        finally:
            await page.close()


async def process_single_ad(context: BrowserContext, link: str, semaphore: asyncio.Semaphore):
    """Scrapes a single ad page and returns structured raw data.

    Opens a new browser tab, navigates to the ad URL, and extracts: title,
    price, location, and full page text. Location is extracted using a
    structural CSS anchor (``img[alt="Location"] + div p``); if the first
    paragraph contains digits (street address), the second paragraph (region)
    is used instead. The tab is always closed in the ``finally`` block.

    Args:
        context (BrowserContext): Shared Playwright browser context used to
            open new tabs.
        link (str): Absolute URL of the ad page to scrape.
        semaphore (asyncio.Semaphore): Limits the number of ad pages open at
            the same time to avoid Cloudflare/OLX rate-limiting.

    Returns:
        dict | None: Dictionary with raw scraped fields, or ``None`` if a
            critical error occurred::

                {
                    "id":           "123456789",          # ad ID from URL
                    "title":        "2-кімнатна квартира",
                    "raw_price":    "35 000 $",           # unparsed price string
                    "raw_location": "Київ, Дарницький",   # unparsed location string
                    "url":          "https://www.olx.ua/...",
                    "full_text":    "..."                 # full body text for fallback
                }

    Example:
        Input:  link="https://www.olx.ua/uk/.../prodazh-2k-kvartiry-ID123.html"
        Output: {
                    "id": "ID123",
                    "title": "2-кімнатна квартира, 65 м²",
                    "raw_price": "65 000 $",
                    "raw_location": "Київ, Голосіївський",
                    "url": "https://www.olx.ua/uk/.../prodazh-2k-kvartiry-ID123.html",
                    "full_text": "2-кімнатна квартира..."
                }
    """
    async with semaphore:
        page = await context.new_page()
        try:
            ad_id = link.split('-')[-1].replace('.html', '')
            logger.info("Parsing adv: %s", ad_id)

            await page.goto(link, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(random.uniform(1, 3))

            try:
                raw_page_title = await page.title()
                title = raw_page_title.split(' - ')[0].strip()
            except Exception:
                title = "Not found"

            try:
                raw_price = await page.locator('[data-testid="ad-price-container"] h3').first.inner_text(timeout=ELEMENT_TIMEOUT)
            except Exception:
                raw_price = None

            try:
                # Anchor: img[alt="Location"], next to which there is always a div with two <p>:
                # p.nth(0) — city/district, p.nth(1) — region
                loc_ps = page.locator('img[alt="Location"] + div p')
                city_text = await loc_ps.nth(0).inner_text(timeout=ELEMENT_TIMEOUT)
                if any(ch.isdigit() for ch in city_text):
                    # p.nth(0) contains a street address with a number — use region from p.nth(1)
                    raw_location = await loc_ps.nth(1).inner_text(timeout=ELEMENT_TIMEOUT)
                else:
                    raw_location = city_text
            except Exception:
                raw_location = "Unknown"

            try:
                full_page_text = await page.locator('body').inner_text(timeout=ELEMENT_TIMEOUT)
            except Exception:
                full_page_text = ""

            return {
                "id": ad_id,
                "title": title,
                "raw_price": raw_price,
                "raw_location": raw_location,
                "url": link,
                "full_text": full_page_text
            }

        except Exception as e:
            logger.error("Critical error on page %s: %s", link, e)
            return None

        finally:
            await page.close()


async def extract_data() -> list:
    """Orchestrates the full Extract phase of the ETL pipeline.

    Workflow:
        1. Load page 1 → read total page count from pagination.
        2. Fetch all catalog pages in parallel (limited by ``MAX_CONCURRENT_CATALOG_PAGES``).
        3. Flatten and deduplicate collected links.
        4. Scrape all ad pages in parallel (limited by ``MAX_CONCURRENT_TABS``).
        5. Filter out failed results (``None``).

    Returns:
        list[dict]: List of raw ad dictionaries. Each item has the shape
            returned by :func:`process_single_ad`. Failed ads are excluded::

                [
                    {
                        "id": "123456",
                        "title": "3-кімнатна квартира",
                        "raw_price": "120 000 $",
                        "raw_location": "Київ, Печерський",
                        "url": "https://www.olx.ua/...",
                        "full_text": "..."
                    },
                    ...
                ]

    Example:
        Input:  OLX catalog has 25 pages, ADS_PER_PAGE=5
        Output: up to 125 raw ad dicts (minus failed/None results)
    """
    logger.info("Initializing Playwright (Extract phase)...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT
        )

        # Load page 1 to determine the total number of catalog pages
        first_page = await context.new_page()
        await first_page.goto(URL, wait_until="domcontentloaded", timeout=CATALOG_LOAD_TIMEOUT)
        await first_page.wait_for_selector('div[data-cy="l-card"]', timeout=SELECTOR_TIMEOUT)
        total_pages = await fetch_total_pages(first_page)
        await first_page.close()

        logger.info("Starting parallel link collection from %d catalog pages...", total_pages)

        # Collect links from all catalog pages in parallel
        # range(1, total_pages + 1): OLX pagination starts from 1
        catalog_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CATALOG_PAGES)
        catalog_tasks = [
            fetch_catalog_page(context, page_num, catalog_semaphore)
            for page_num in range(1, total_pages + 1)
        ]
        all_ads_links = await asyncio.gather(*catalog_tasks)

        # Flatten list of lists into a single list of links :)
        all_links = []
        for page_links in all_ads_links:
            for link in page_links:
                all_links.append(link)
        links = list(dict.fromkeys(all_links))
        duplicates = len(all_links) - len(links)
        logger.debug("Total parsed: %d links, duplicates skipped: %d", len(all_links), duplicates)
        logger.info("Found %d unique links. Launching %d parallel threads...", len(links), MAX_CONCURRENT_TABS)

        # Process all ads in parallel
        ad_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        tasks = [process_single_ad(context, link, ad_semaphore) for link in links]
        results = await asyncio.gather(*tasks)

        await browser.close()

        raw_data = [res for res in results if res is not None]
        return raw_data