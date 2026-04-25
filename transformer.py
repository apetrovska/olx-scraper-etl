import re
import pandas as pd


def clean_price(price_text: str):
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
    """Визначає валюту з сирого рядка ціни та повертає стандартний код ISO"""
    if not price_text:
        return "UAH"  # Дефолтне значення

    text = price_text.lower()
    if '$' in text or 'долар' in text:
        return "USD"
    elif '€' in text or 'євро' in text:
        return "EUR"
    else:
        return "UAH"

def extract_fallback_data(item: dict):
    """Якщо скрапер не знайшов теги, шукаємо дані в тексті сторінки"""
    full_text = item.get("full_text", "")

    # 1. Відновлюємо ціну (шукаємо цифри, після яких йде $, €, або грн)
    if not item.get("raw_price"):
        price_match = re.search(r'([\d\s.,]+)\s*(грн\.|грн|\$|€)', full_text, re.IGNORECASE)
        if price_match:
            item["raw_price"] = price_match.group(0)

    # 2. Відновлюємо локацію (шукаємо текст після слова МІСЦЕЗНАХОДЖЕННЯ)
    if item.get("raw_location") == "Невідомо" or not item.get("raw_location"):
        loc_match = re.search(r'МІСЦЕЗНАХОДЖЕННЯ\s*\n+([^\n]+)', full_text)
        if loc_match:
            item["raw_location"] = loc_match.group(1)

    # 3. Відновлюємо заголовок (шукаємо рядок відразу після слова "Опубліковано")
        if item.get("title") == "Не знайдено" or not item.get("title"):
            title_match = re.search(r'Опубліковано[^\n]*\n+([^\n]+)', full_text)
            if title_match:
                item["title"] = title_match.group(1).strip()

    return item

def extract_parameters(full_text: str):
    """Шукає площу, поверх та поверховість у загальному тексті."""
    params = {
        "Area_sqm": None,
        "Floor": None,
        "Total_Floors": None
    }

    if not full_text: return params

    # Шукаємо "Загальна площа: 45 м²" або "45.5"
    area_match = re.search(r'Загальна площа:\s*([\d.,]+)', full_text)
    if area_match:
        params["Area_sqm"] = float(area_match.group(1).replace(',', '.'))

    # Шукаємо "Поверх: 3"
    floor_match = re.search(r'Поверх:\s*(\d+)', full_text)
    if floor_match:
        params["Floor"] = int(floor_match.group(1))

    # Шукаємо "Поверховість: 9"
    total_floors_match = re.search(r'Поверховість:\s*(\d+)', full_text)
    if total_floors_match:
        params["Total_Floors"] = int(total_floors_match.group(1))

    return params


def extract_city(location_text: str) -> str:
    """Витягує назву міста з сирого рядка локації"""
    if not location_text:
        return "Невідомо"

    # Приклад: "Київ, Голосіївський - Сьогодні о 17:28"
    # 1. Відрізаємо все після коми (отримаємо "Київ")
    city = location_text.split(',')[0]

    # 2. На випадок, якщо коми немає, відрізаємо після дефіса
    city = city.split('-')[0]

    # 3. Прибираємо зайві пробіли по краях
    return city.strip()


def transform_data(raw_data: list) -> pd.DataFrame:
    print("Починаємо трансформацію даних...")
    cleaned_data = []

    for item in raw_data:
        # Відновлюємо втрачені дані перед обробкою
        item = extract_fallback_data(item)

        # Парсимо площу і поверхи з тексту
        params = extract_parameters(item.get("full_text", ""))

        # Очищаємо ID від зайвих параметрів (наприклад ?search_reason=...)
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

    # Видаляємо лише ті, де ціну так і не вдалося знайти
    # БЕЗПЕЧНЕ ВИДАЛЕННЯ: перевіряємо, чи DataFrame не пустий і чи є там наша колонка
    if not df.empty and 'Price' in df.columns:
        df = df.dropna(subset=['Price'])
    print(f"Трансформація завершена. Готово рядків: {len(df)}")
    return df