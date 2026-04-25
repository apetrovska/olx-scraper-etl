import asyncio
import random
from playwright.async_api import async_playwright, BrowserContext

URL = "https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/"

# Ліміт одночасних потоків для уникнення блокування (HTTP 429) від Cloudflare/OLX
MAX_CONCURRENT_TABS = 3

# Кількість сторінок каталогу, які ми хочемо обійти
PAGES_TO_SCRAPE = 2


async def process_single_ad(context: BrowserContext, link: str, semaphore: asyncio.Semaphore):
    """Обробляє одну сторінку оголошення асинхронно"""
    async with semaphore:
        page = await context.new_page()
        try:
            ad_id = link.split('-')[-1].replace('.html', '')
            print(f"Парсимо оголошення: {ad_id}")

            # Переходимо за посиланням
            await page.goto(link, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(random.uniform(1, 3))

            # 1. Заголовок (Беремо мета-тег сторінки. Працює миттєво - таймаут не потрібен)
            try:
                raw_page_title = await page.title()
                # Відрізаємо хвіст " - продаж квартир у Києві..."
                title = raw_page_title.split(' - ')[0].strip()
            except Exception:
                title = "Не знайдено"

            # 2. Витягуємо ціну (жорсткий таймаут 2 секунди)
            try:
                price_locator = page.locator('[data-testid="ad-price"], h3.css-90xrc0').first
                raw_price = await price_locator.inner_text(timeout=2000)
            except Exception:
                raw_price = None

            # 3. Витягуємо локацію та дату (жорсткий таймаут 2 секунди)
            try:
                loc_locator = page.locator('[data-testid="location-date"], .css-1g5nan, .css-1b24pxk').first
                raw_location = await loc_locator.inner_text(timeout=2000)
            except Exception:
                raw_location = "Невідомо"

            # 4. Витягуємо весь текст сторінки для Fallback-парсингу (жорсткий таймаут 2 секунди)
            try:
                full_page_text = await page.locator('body').inner_text(timeout=2000)
            except Exception:
                full_page_text = ""

            # Повертаємо зібрані дані
            return {
                "id": ad_id,
                "title": title,
                "raw_price": raw_price,
                "raw_location": raw_location,
                "url": link,
                "full_text": full_page_text
            }

        except Exception as e:
            print(f"Критична помилка на сторінці {link}: {e}")
            return None

        finally:
            # Гарантоване закриття вкладки для звільнення оперативної пам'яті
            await page.close()


async def extract_data():
    """Основний потік: збирає посилання з декількох сторінок каталогу та запускає їх паралельну обробку."""
    print("Ініціалізація Playwright (Extract phase)...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()
        links = []

        # Цикл пагінації: обходимо вказану кількість сторінок
        for page_num in range(1, PAGES_TO_SCRAPE + 1):
            current_url = URL if page_num == 1 else f"{URL}?page={page_num}"
            print(f"Завантаження каталогу (сторінка {page_num}): {current_url}")

            # Чекаємо лише на завантаження domcontentloaded), таймаут 60 секунд
            await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector('div[data-cy="l-card"]', timeout=15000)

            # Пауза між сторінками каталогу, щоб імітувати людину
            await asyncio.sleep(random.uniform(2, 4))

            link_elements = await page.locator('div[data-cy="l-card"] a').all()

            # В умовах тестового завдання - по 5 посилань з кожної сторінки
            page_links_count = 0
            for el in link_elements[:5]:
                href = await el.get_attribute('href')
                if href and href.startswith('/'):
                    href = "https://www.olx.ua" + href
                if href not in links:
                    links.append(href)
                    page_links_count += 1

            print(f"Зібрано {page_links_count} посилань зі сторінки {page_num}.")

        await page.close()

        print(f"Загалом знайдено {len(links)} унікальних посилань. Запуск {MAX_CONCURRENT_TABS} паралельних потоків...")

        # Створюємо семафор для контролю конкурентності
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)

        # Формуємо список задач і запускаємо їх одночасно
        tasks = [process_single_ad(context, link, semaphore) for link in links]
        results = await asyncio.gather(*tasks)

        await browser.close()

        # Відфільтровуємо порожні результати (де виникли помилки)
        raw_data = [res for res in results if res is not None]
        return raw_data