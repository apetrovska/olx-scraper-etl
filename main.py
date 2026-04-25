import asyncio
from scraper import extract_data
from transformer import transform_data
from loader import load_to_sheets

async def main_pipeline():
    print("=== Старт ETL процесу ===")

    # 1. EXTRACT
    raw_data = await extract_data()

    # ДЕБАГ РЯДОК:
    print(f"DEBUG: Перше сире оголошення: {raw_data[0] if raw_data else 'Пусто'}")

    if not raw_data:
        print("Не вдалося зібрати дані")
        return

    # 2. TRANSFORM
    clean_df = transform_data(raw_data)

    # Виводимо результат
    print("Фінальний DataFrame перед відправкою:")
    print(clean_df.head())

    # 3. LOAD
    load_to_sheets(clean_df)

    print("ETL процес успішно завершено!")


if __name__ == "__main__":
    asyncio.run(main_pipeline())