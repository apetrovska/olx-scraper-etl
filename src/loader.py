import logging

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from src.settings import GOOGLE_SCOPES, SERVICE_ACCOUNT_FILE, SHEET_NAME

logger = logging.getLogger(__name__)


def load_to_sheets(df: pd.DataFrame, sheet_name: str = SHEET_NAME) -> None:
    """Uploads a Pandas DataFrame to the first sheet of a Google Spreadsheet.

    Authenticates via a service account JSON file, clears the target sheet,
    then writes all data (headers + rows) in a single API request.

    Args:
        df (pd.DataFrame): Clean DataFrame produced by
            :func:`transformer.transform_data`. Expected columns:
            ``ID``, ``Title``, ``Price``, ``Currency``, ``Area_sqm``,
            ``Floor``, ``Total_Floors``, ``City``, ``URL``.
        sheet_name (str, optional): Name of the Google Spreadsheet to open.
            Defaults to ``SHEET_NAME`` from ``settings.py``
            (``"OLX_RealEstate_Data"``).

    Returns:
        None

    Raises:
        gspread.exceptions.SpreadsheetNotFound: If the spreadsheet with
            ``sheet_name`` does not exist or the service account has no access.
        google.auth.exceptions.DefaultCredentialsError: If
            ``service_account_creds.json`` is missing or invalid.

    Example:
        Input:  df with 42 rows, sheet_name="OLX_RealEstate_Data"
        Output: None  (side effect: 43 rows written to Google Sheets —
                       1 header row + 42 data rows)

        Input:  df with 0 rows
        Output: None  (side effect: only the header row is written)
    """
    logger.info("Start loading to Google Sheets...")

    try:
        # Authenticate
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=GOOGLE_SCOPES)
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name).sheet1

        # Clean the sheet before loading data to avoid duplicates on re-runs
        sheet.clear()

        # Convert DataFrame to list of lists; gspread requires this format
        # First row contains column headers
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()

        # Load all data in a single request (much faster than row by row)
        sheet.update(range_name="A1", values=data_to_upload)

        logger.info("%d rows uploaded to Google Sheet '%s'.", len(df), sheet_name)

    except Exception as e:
        logger.error("Upload error: %s", e)