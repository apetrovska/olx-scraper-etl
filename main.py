import asyncio
import logging

from src.utils.logger import setup_logging
from src.scraper import extract_data
from src.transformer import transform_data
from src.loader import load_to_sheets

logger = logging.getLogger(__name__)


async def main_pipeline():
    logger.info("=== ETL pipeline started ===")

    # 1. EXTRACT
    raw_data = await extract_data()

    logger.debug("First raw adv: %s", raw_data[0] if raw_data else "Empty")

    if not raw_data:
        logger.warning("No data collected")
        return

    # 2. TRANSFORM
    clean_df = transform_data(raw_data)

    logger.debug("Final DataFrame before loading:\n%s", clean_df.head().to_string())

    # 3. LOAD
    load_to_sheets(clean_df)

    logger.info("ETL pipeline completed successfully")


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main_pipeline())