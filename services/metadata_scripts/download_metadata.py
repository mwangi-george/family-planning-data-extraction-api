import os
import time

from loguru import logger
from dotenv import load_dotenv

from services.metadata_scripts.get_indicators import get_indicators
from services.metadata_scripts.get_data_elements import get_data_elements
from services.metadata_scripts.get_organisation_units import get_organisation_units
from services.metadata_scripts.helpers import save_df_to_db


def load_env_variables() -> tuple[str, str, str, str]:
    """
    Load required environment variables needed to connect to DHIS2 and the database.

    Returns
    -------
    tuple[str, str, str, str]
        A tuple containing:
        - DHIS2 base URL
        - DHIS2 username
        - DHIS2 password
        - Database connection URL

    Raises
    ------
    ValueError
        If any required environment variable is missing.
    """
    load_dotenv()

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    FP_DB_URL = os.getenv("FP_DB_URL")

    if not all([DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD, FP_DB_URL]):
        raise ValueError("One or more required environment variables are missing. "
                         "Please check your .env file.")

    return DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD, FP_DB_URL


def extract_and_store_dhis2_metadata() -> str:
    """
    Extract DHIS2 metadata objects (organisation units, data elements, and indicators)
    and save them into a relational database.

    This function:
    1. Loads required environment variables.
    2. Downloads metadata from DHIS2 using API helper functions.
    3. Measures the total extraction time.
    4. Stores each metadata DataFrame into the database using `save_df_to_db()`.

    Logs
    ----
    - Extraction runtime
    - Success messages for each dataset stored
    - Errors if any step fails
    """
    try:
        # Load environment configuration
        DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD, FP_DB_URL = load_env_variables()

        logger.info("Starting DHIS2 metadata extraction...")

        start_time = time.time()

        # --- Download metadata from DHIS2 ---
        logger.info("Downloading organisation units...")
        organisation_units_df = get_organisation_units(
            DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD
        )

        logger.info("Downloading data elements...")
        data_elements_df = get_data_elements(
            DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD
        )

        logger.info("Downloading indicators...")
        indicators_df = get_indicators(
            DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD
        )

        # --- Measure extraction time ---
        elapsed = time.time() - start_time
        logger.info(f"Metadata extraction completed in {elapsed:.2f} seconds.")

        # --- Store metadata into DB ---
        logger.info("Saving organisation units to database...")
        save_df_to_db(organisation_units_df, FP_DB_URL, "organisation_units", "replace")

        logger.info("Saving data elements to database...")
        save_df_to_db(data_elements_df, FP_DB_URL, "data_elements", "replace")

        logger.info("Saving indicators to database...")
        save_df_to_db(indicators_df, FP_DB_URL, "indicators", "replace")

        msg = "All metadata successfully saved to the database."
        logger.success(msg)
        return msg

    except Exception as e:
        logger.exception(f"Failed during metadata extraction or storage: {e}")
        return "Metadata extraction or storage failed. Check your logs"


if __name__ == "__main__":
    extract_and_store_dhis2_metadata()
