import os
import requests
import polars as pl
from loguru import logger
from dotenv import load_dotenv
from typing import Optional


def get_indicators(base_url: str, username: str, password: str) -> Optional[pl.DataFrame]:
    """
    Fetch DHIS2 indicators metadata and return it as a Polars DataFrame.

    Parameters
    ----------
    base_url : str
        Base URL of the DHIS2 instance.
    username : str
        DHIS2 username.
    password : str
        DHIS2 password.

    Returns
    -------
    Optional[pl.DataFrame]
        A Polars DataFrame containing indicators metadata, or None if parsing fails.

    Raises
    ------
    RuntimeError
        If the DHIS2 API request fails or returns a non-200 response.
    """
    url = f"{base_url}/api/indicators?fields=name,id&paging=false"
    logger.info(f"Requesting indicators from DHIS2: {url}")

    # --- API request ---
    try:
        response = requests.get(url, auth=(username, password))
    except requests.RequestException as e:
        raise RuntimeError(f"Network error while requesting indicators: {e}")

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to retrieve indicators. Status: {response.status_code} | Response: {response.text}"
        )

    logger.success("Indicators successfully retrieved.")

    # --- Parse JSON ---
    try:
        json_data: dict = response.json()
    except ValueError:
        raise RuntimeError("DHIS2 returned an invalid JSON response for indicators.")

    indicators = json_data.get("indicators", [])

    if not isinstance(indicators, list):
        raise RuntimeError(
            f"Unexpected JSON structure: 'indicators' is not a list. Keys: {json_data.keys()}"
        )

    # --- Convert to DataFrame ---
    try:
        df = pl.DataFrame(indicators)
        logger.info(f"Indicators DataFrame created with {df.shape[0]} rows.")

        return df
    except Exception as e:
        logger.exception(f"Failed to build Polars DataFrame for indicators: {e}")
        return None


if __name__ == "__main__":
    load_dotenv()

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")

    df = get_indicators(DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD)

    if df is not None:
        logger.success("Indicators loaded and processed successfully.")
        print(df.head())
