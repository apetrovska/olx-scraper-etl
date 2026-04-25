import gspread
from google.oauth2.service_account import Credentials
import pandas as pd


def load_to_sheets(df: pd.DataFrame, sheet_name: str = "OLX_RealEstate_Data"):
    """Завантажує Pandas DataFrame у вказану Google Таблицю."""
    print("Починаємо завантаження в Google Sheets...")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    try:
        # Авторизація
        creds = Credentials.from_service_account_file("service_account_creds.json", scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name).sheet1

        # Очищаємо аркуш перед новим завантаженням (щоб уникнути дублікатів при перезапусках)
        sheet.clear()

        # Щоб gspread міг "з'їсти" DataFrame, нам треба перетворити його на список списків
        # Додаємо заголовки колонок першим рядком
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()

        # Вивантажуємо всі дані одним запитом (це набагато швидше, ніж по одному рядку)
        sheet.update(range_name="A1", values=data_to_upload)

        print(f"Успіх! {len(df)} рядків успішно завантажено в таблицю '{sheet_name}'.")

    except Exception as e:
        print(f"Помилка при завантаженні: {e}")