import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)


def clean_price(price_text: str) -> float | None:
    """Parses a raw price string and returns a numeric float value.

    Strips all currency symbols, whitespace, and non-numeric characters,
    then converts the remaining string to a float.

    Args:
        price_text (str): Raw price string as scraped from the page.

    Returns:
        float | None: Numeric price value, or ``None`` if the string is empty,
            ``None``, or cannot be parsed.

    Example:
        Input:  "35 000 $"
        Output: 35000.0

        Input:  "1 200 грн."
        Output: 1200.0

        Input:  None
        Output: None
    """
    if not price_text: return None
    try:
        text = price_text.lower().replace('грн.', '').replace('грн', '').replace('$', '').replace('€', '')
        text = text.replace(' ', '').replace('\xa0', '')
        text = re.sub(r'[^\d.,]', '', text)
        text = text.replace(',', '.').strip('.')
        if text: return float(text)
        return None
    except Exception:
        return None


def extract_currency(price_text: str) -> str:
    """Detects currency from a raw price string and returns the ISO 4217 code.

    Checks for ``$`` or Ukrainian word "долар" → USD;
    ``€`` or "євро" → EUR; anything else (including ``None``) → UAH.

    Args:
        price_text (str | None): Raw price string as scraped from the page.

    Returns:
        str: ISO 4217 currency code: ``"USD"``, ``"EUR"``, or ``"UAH"``.

    Example:
        Input:  "35 000 $"
        Output: "USD"

        Input:  "150 000 грн."
        Output: "UAH"

        Input:  None
        Output: "UAH"
    """
    if not price_text:
        return "UAH"  # default value

    text = price_text.lower()
    if '$' in text or 'долар' in text:
        return "USD"
    elif '€' in text or 'євро' in text:
        return "EUR"
    else:
        return "UAH"


def extract_fallback_data(item: dict) -> dict:
    """Recovers missing fields by searching the full page body text.

    Used when the primary CSS/testid selectors failed to find price, location,
    or title. Applies regex patterns to ``item["full_text"]`` and fills in any
    missing values in-place.

    Args:
        item (dict): Raw ad dictionary from :func:`scraper.process_single_ad`.
            Expected keys: ``"raw_price"``, ``"raw_location"``, ``"title"``,
            ``"full_text"``.

    Returns:
        dict: The same dictionary with missing fields filled in where possible.
            Fields that cannot be recovered remain unchanged.

    Example:
        Input: {
                   "raw_price": None,
                   "raw_location": "Unknown",
                   "title": "Not found",
                   "full_text": "...МІСЦЕЗНАХОДЖЕННЯ\\nКиїв...35 000 $..."
               }
        Output: {
                    "raw_price": "35 000 $",
                    "raw_location": "Київ",
                    "title": "Not found",
                    "full_text": "..."
                }
    """
    full_text = item.get("full_text", "")

    # 1. Extract price (look for digits followed by $, €, or грн)
    if not item.get("raw_price"):
        price_match = re.search(r'([\d\s.,]+)\s*(грн\.|грн|\$|€)', full_text, re.IGNORECASE)
        if price_match:
            logger.debug("raw_price was updated from fallback, %s -> %s", item["raw_price"], price_match.group(0))
            item["raw_price"] = price_match.group(0)

    # 2. Extract location (look for text after the word МІСЦЕЗНАХОДЖЕННЯ)
    if item.get("raw_location") == "Unknown" or not item.get("raw_location"):
        loc_match = re.search(r'МІСЦЕЗНАХОДЖЕННЯ\s*\n+([^\n]+)', full_text)
        if loc_match:
            logger.debug("raw_location was updated from fallback, %s -> %s", item["raw_location"], loc_match.group(1))
            item["raw_location"] = loc_match.group(1)

    # 3. Extract title (look for the line right after the word "Опубліковано")
    if item.get("title") == "Not found" or not item.get("title"):
        title_match = re.search(r'Опубліковано[^\n]*\n+([^\n]+)', full_text)
        if title_match:
            logger.debug("title was updated from fallback, %s -> %s", item["title"], title_match.group(1).strip())
            item["title"] = title_match.group(1).strip()

    return item


def extract_parameters(full_text: str) -> dict:
    """Extracts property parameters (area, floor, total floors) from page text.

    Uses Ukrainian-language regex patterns to find structured property data
    embedded in the full page body text.

    Args:
        full_text (str): Full body text of the ad page
            (from ``item["full_text"]``).

    Returns:
        dict: Dictionary with three keys. Any value not found is ``None``::

                {
                    "Area_sqm":    float | None,  # e.g. 65.5
                    "Floor":       int   | None,  # e.g. 3
                    "Total_Floors":int   | None   # e.g. 9
                }

    Example:
        Input:  "...Загальна площа: 65 м²...Поверх: 3...Поверховість: 9..."
        Output: {"Area_sqm": 65.0, "Floor": 3, "Total_Floors": 9}

        Input:  ""
        Output: {"Area_sqm": None, "Floor": None, "Total_Floors": None}
    """
    params = {
        "Area_sqm": None,
        "Floor": None,
        "Total_Floors": None
    }

    if not full_text: return params

    # Search for "Загальна площа: 45 м²" or "45.5"
    area_match = re.search(r'Загальна площа:\s*([\d.,]+)', full_text)
    if area_match:
        params["Area_sqm"] = float(area_match.group(1).replace(',', '.'))

    # Search for "Поверх: 3"
    floor_match = re.search(r'Поверх:\s*(\d+)', full_text)
    if floor_match:
        params["Floor"] = int(floor_match.group(1))

    # Search for "Поверховість: 9"
    total_floors_match = re.search(r'Поверховість:\s*(\d+)', full_text)
    if total_floors_match:
        params["Total_Floors"] = int(total_floors_match.group(1))

    return params


def extract_city(location_text: str) -> str:
    """Extracts only the city name from a raw location string.

    OLX location strings typically contain city, district, and date separated
    by commas and dashes. This function returns the first token before any
    comma or dash.

    Args:
        location_text (str | None): Raw location string as scraped from the
            page.

    Returns:
        str: City name with surrounding whitespace stripped, or ``"Unknown"``
            if input is empty or ``None``.

    Example:
        Input:  "Київ, Голосіївський - Сьогодні о 17:28"
        Output: "Київ"

        Input:  "Львів - Вчора о 10:00"
        Output: "Львів"

        Input:  None
        Output: "Unknown"
    """
    if not location_text:
        return "Unknown"

    # Example: "Київ, Голосіївський - Сьогодні о 17:28"
    # 1. Split on comma to get "Київ"
    city = location_text.split(',')[0]

    # 2. If no comma, split on dash
    city = city.split('-')[0]

    # 3. Strip leading/trailing whitespace
    return city.strip()


def transform_data(raw_data: list) -> pd.DataFrame:
    """Transforms a list of raw ad dicts into a clean, typed Pandas DataFrame.

    For each item:
        - Runs fallback recovery via :func:`extract_fallback_data`.
        - Parses area/floor data via :func:`extract_parameters`.
        - Cleans price via :func:`clean_price`.
        - Detects currency via :func:`extract_currency`.
        - Extracts city via :func:`extract_city`.
        - Strips query params from the ID.
    Rows where ``Price`` is ``None`` (i.e. price could not be found even via
    fallback) are dropped.

    Args:
        raw_data (list[dict]): List of raw ad dictionaries returned by
            :func:`scraper.extract_data`.

    Returns:
        pd.DataFrame: Clean DataFrame with the following columns:

            ============  =======  ====================================
            Column        Type     Description
            ============  =======  ====================================
            ID            str      Ad identifier extracted from URL
            Title         str      Ad title
            Price         float    Numeric price value
            Currency      str      ISO 4217 code (USD / EUR / UAH)
            Area_sqm      float    Total area in square metres
            Floor         int      Floor number
            Total_Floors  int      Total floors in the building
            City          str      City name
            URL           str      Full ad URL
            ============  =======  ====================================

    Example:
        Input: [
                   {"id": "ID123", "title": "2-кімн.", "raw_price": "65 000 $",
                    "raw_location": "Київ, Дарницький", "url": "https://...",
                    "full_text": "...Загальна площа: 65...Поверх: 5..."}
               ]
        Output:
               ID    Title     Price Currency  Area_sqm  Floor  Total_Floors  City   URL
               ID123 2-кімн.  65000.0  USD      65.0      5      NaN          Київ   https://...
    """
    logger.info("Starting data transformation...")
    cleaned_data = []

    for item in raw_data:
        # Recover missing data before processing
        item = extract_fallback_data(item)

        # Parse area and floors from page text
        params = extract_parameters(item.get("full_text", ""))

        # Strip extra query params from ID (e.g. ?search_reason=...)
        clean_id = str(item.get("id", "")).split('?')[0]

        cleaned_item = {
            "ID": clean_id,
            "Title": item.get("title"),
            "Price": clean_price(item.get("raw_price")),
            "Currency": extract_currency(item.get("raw_price")),
            "Area_sqm": params["Area_sqm"],
            "Floor": params["Floor"],
            "Total_Floors": params["Total_Floors"],
            "City": extract_city(item.get("raw_location")),
            "URL": item.get("url")
        }
        cleaned_data.append(cleaned_item)

    df = pd.DataFrame(cleaned_data)

    # Drop rows where price could not be found
    # Safe drop: check that DataFrame is not empty and the column exists
    if not df.empty and 'Price' in df.columns:
        df = df.dropna(subset=['Price'])
    logger.info("Transformation complete. Rows ready: %d", len(df))
    return df