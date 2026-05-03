import asyncio
import logging
import random
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, BrowserContext
from src.settings import (
    URL, MAX_CONCURRENT_TABS, MAX_CONCURRENT_CATALOG_PAGES, ADS_PER_PAGE,
    VIEWPORT, USER_AGENT,
    PAGE_LOAD_TIMEOUT, CATALOG_LOAD_TIMEOUT, ELEMENT_TIMEOUT
)

logger = logging.getLogger(__name__)


def _fetch_total_pages_sync() -> int:
    """Synchronous helper: fetches catalog page and extracts total page count."""
    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(URL, headers=headers, timeout=CATALOG_LOAD_TIMEOUT / 1000)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        last_pagination = soup.select('[data-testid="pagination-list-item"]')

        if not last_pagination:
            logger.warning("Pagination not found. Defaulting to 1 page.")
            return 1

        aria_label = last_pagination[-1].get('aria-label', '')
        total = int(aria_label.split()[-1])
        logger.info("Total catalog pages found: %d", total)
        return total
    except Exception as e:
        logger.warning("Could not determine page count: %s. Defaulting to 1.", e)
    return 1


async def fetch_total_pages() -> int:
    """Determines the total number of catalog pages from the pagination element.

    Fetches the first catalog page via HTTP (requests) and parses with BeautifulSoup.
    Reads the ``aria-label`` attribute of the last ``[data-testid="pagination-list-item"]``
    element. OLX sets this to ``"Page N"``, where N is the last available page number.

    Executes synchronously in a thread pool to avoid blocking the event loop.

    Returns:
        int: Total number of pages. Returns ``1`` if pagination is not found
            or cannot be parsed.

    Example:
        Input:  First catalog page fetched from https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/
                last pagination element has aria-label="Page 25"
        Output: 25
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_total_pages_sync)


def _fetch_catalog_page_sync(page_num: int) -> list:
    """Synchronous helper: fetches a single catalog page and extracts ad links."""
    try:
        current_url = URL if page_num == 1 else f"{URL}?page={page_num}"
        logger.info("Loading catalog (page %d): %s", page_num, current_url)

        headers = {"User-Agent": USER_AGENT}
        response = requests.get(current_url, headers=headers, timeout=CATALOG_LOAD_TIMEOUT / 1000)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        cards = soup.select('div[data-cy="l-card"]')

        links = []
        for card in cards[:ADS_PER_PAGE]:
            link_elem = card.find('a')
            if link_elem:
                href = link_elem.get('href')
                if href and href.startswith('/'):
                    href = "https://www.olx.ua" + href
                if href and href not in links:
                    links.append(href)

        logger.debug("Parsed %d links from page %d.", len(links), page_num)
        return links

    except Exception as e:
        logger.error("Error loading catalog page %d: %s", page_num, e)
        return []


async def fetch_catalog_page(page_num: int, semaphore: asyncio.Semaphore) -> list:
    """Loads a single catalog page and returns a list of ad URLs.

    Fetches the catalog page via HTTP (requests) and parses with BeautifulSoup.
    Extracts up to ``ADS_PER_PAGE`` ad links from card elements.

    Execution controlled by semaphore to limit concurrent requests and avoid
    rate-limiting (HTTP 429) from OLX/Cloudflare.

    Args:
        page_num (int): Catalog page number to load. Page 1 uses the base URL;
            pages 2+ append ``?page=N``.
        semaphore (asyncio.Semaphore): Limits the number of concurrent catalog page
            requests to ``MAX_CONCURRENT_CATALOG_PAGES``.

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
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch_catalog_page_sync, page_num)


async def process_single_ad(context: BrowserContext, link: str, semaphore: asyncio.Semaphore) -> dict | None:
    """Scrapes a single ad page using Playwright.

    Opens a new browser tab, loads the ad page, and extracts: title, price,
    location, and full page text. Controlled by semaphore to limit concurrent tabs.

    Args:
        context (BrowserContext): Shared Playwright browser context.
        link (str): Absolute URL of the ad page to scrape.
        semaphore (asyncio.Semaphore): Limits concurrent ad page parsing.

    Returns:
        dict: Dictionary with raw scraped fields. On critical error, returns dict with
            ``title="ERROR"`` and error message in ``full_text`` field for quality assessment.
    """
    async with semaphore:
        page = await context.new_page()
        try:
            ad_id = link.split('-')[-1].replace('.html', '')
            logger.debug("Parsing adv: %s", ad_id)

            await page.goto(link, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(random.uniform(1, 3))

            # Extract title from page title
            title = "Not found"
            try:
                raw_page_title = await page.title()
                title = raw_page_title.split(' - ')[0].strip()
            except Exception:
                pass

            # Extract price from [data-testid="ad-price-container"] h3
            raw_price = None
            try:
                price_locator = page.locator('[data-testid="ad-price-container"] h3').first
                raw_price = await price_locator.inner_text(timeout=ELEMENT_TIMEOUT)
            except Exception:
                pass

            # Extract location from img[alt="Location"] + div p
            raw_location = "Unknown"
            try:
                img_locator = page.locator('img[alt="Location"]').first
                if await img_locator.count() > 0:
                    parent_div = img_locator.locator('xpath=../..').first
                    paragraphs = await parent_div.locator('p').all()
                    if paragraphs:
                        city_text = await paragraphs[0].inner_text()
                        if any(ch.isdigit() for ch in city_text) and len(paragraphs) > 1:
                            raw_location = await paragraphs[1].inner_text()
                        else:
                            raw_location = city_text
            except Exception:
                pass

            # Extract full body text
            full_page_text = ""
            try:
                full_page_text = await page.locator('body').inner_text(timeout=ELEMENT_TIMEOUT)
            except Exception:
                pass

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
            ad_id = link.split('-')[-1].replace('.html', '')
            return {
                "id": ad_id,
                "title": "ERROR",
                "raw_price": None,
                "raw_location": "ERROR",
                "url": link,
                "full_text": str(e)
            }

        finally:
            await page.close()


async def extract_data() -> list:
    """Orchestrates the full Extract phase of the ETL pipeline.

    Workflow:
        1. Determine total catalog pages via HTTP (requests + BeautifulSoup).
        2. Fetch all catalog pages in parallel via HTTP (limited by ``MAX_CONCURRENT_CATALOG_PAGES``).
        3. Flatten and deduplicate collected ad links.
        4. Scrape all ad pages in parallel via Playwright (limited by ``MAX_CONCURRENT_TABS``).
        5. Return all results (including errors marked with ``title="ERROR"``) for quality assessment.

    Returns:
        list[dict]: List of raw ad dictionaries. Each item has the shape returned by
            :func:`process_single_ad`. Failed ads included with ``title="ERROR"``::

                [
                    {
                        "id": "123456",
                        "title": "3-кімнатна квартира",
                        "raw_price": "120 000 $",
                        "raw_location": "Київ, Печерський",
                        "url": "https://www.olx.ua/...",
                        "full_text": "..."
                    },
                    {
                        "id": "789012",
                        "title": "ERROR",
                        "raw_price": null,
                        "raw_location": "ERROR",
                        "url": "https://www.olx.ua/...",
                        "full_text": "Connection timeout"
                    },
                    ...
                ]

    Example:
        Input:  OLX catalog has 25 pages, ADS_PER_PAGE=5
        Output: up to 125 raw ad dicts (including error rows for quality assessment)
    """
    logger.info("Initializing Extract phase...")

    # Determine total catalog pages (requests + BeautifulSoup)
    total_pages = await fetch_total_pages()

    logger.info("Starting parallel link collection from %d catalog pages...", total_pages)

    # Collect links from all catalog pages in parallel
    catalog_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CATALOG_PAGES)
    catalog_tasks = [
        fetch_catalog_page(page_num, catalog_semaphore)
        for page_num in range(1, total_pages + 1)
    ]
    all_ads_links = await asyncio.gather(*catalog_tasks)

    # Flatten list of lists into a single list of links
    all_links = []
    for page_links in all_ads_links:
        for link in page_links:
            all_links.append(link)
    links = list(dict.fromkeys(all_links))
    duplicates = len(all_links) - len(links)
    logger.debug("Total parsed: %d links, duplicates skipped: %d", len(all_links), duplicates)
    logger.info("Found %d unique links. Launching Playwright for ad pages...", len(links))

    # Initialize Playwright only for scraping individual ad pages
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT
        )

        # Process all ads in parallel using asyncio with Playwright
        ad_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        ad_tasks = [process_single_ad(context, link, ad_semaphore) for link in links]
        results = await asyncio.gather(*ad_tasks)

        await browser.close()

    raw_data = []
    for res in results:
        raw_data.append(res)

    logger.info("Extract phase complete. Collected %d ads (including errors).", len(raw_data))
    return raw_data