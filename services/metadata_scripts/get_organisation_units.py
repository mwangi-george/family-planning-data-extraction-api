import os
from typing import Optional

import requests
import polars as pl
from dotenv import load_dotenv
from loguru import logger

from services.metadata_scripts.helpers import make_orgunits_hierarchy


def get_organisation_units(base_url: str, username: str, password: str) -> Optional[pl.DataFrame]:
    """
    Fetch DHIS2 organisation units and return them as a cleaned Polars DataFrame.

    The function:
    - Downloads organisation units from DHIS2 API
    - Extracts 'parent.id' into a separate column 'parent_id'
    - Converts long-format hierarchy to wide format using `make_orgunits_hierarchy`
    - Cleans location name suffixes (Ward, Sub County, County)

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
        A cleaned Polars DataFrame with organisation units and hierarchy, or None on failure.

    Raises
    ------
    RuntimeError
        If the DHIS2 API request fails or the response is invalid.
    """
    url = f"{base_url}/api/organisationUnits?fields=name,id,parent,level,code&paging=false"
    logger.info(f"Requesting organisation units from DHIS2: {url}")

    # --- API request ---
    try:
        response = requests.get(url, auth=(username, password))
    except requests.RequestException as e:
        raise RuntimeError(f"Network error while requesting organisation units: {e}")

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to retrieve organisation units. Status: {response.status_code} | Response: {response.text}"
        )

    logger.success("Organisation units successfully retrieved from DHIS2.")

    # --- Parse JSON ---
    try:
        json_data = response.json()
    except ValueError:
        raise RuntimeError("DHIS2 returned an invalid JSON response for organisation units.")

    org_units = json_data.get("organisationUnits", [])
    if not isinstance(org_units, list):
        raise RuntimeError("'organisationUnits' key missing or not a list in DHIS2 response.")

    # --- Convert to DataFrame and clean ---
    try:
        df = (
            pl.DataFrame(org_units)
            # Extract 'parent.id' to 'parent_id' column
            .with_columns(parent_id=pl.col("parent").struct.field("id"))
            .drop("parent")
        )

        # Convert from long format to wide hierarchy
        df = make_orgunits_hierarchy(df)

        # Clean location name suffixes
        df = df.with_columns(
            ward_name=pl.col("ward_name").str.replace_all(" Ward", ""),
            sub_county_name=pl.col("sub_county_name").str.replace_all(" Sub County", ""),
            county_name=pl.col("county_name").str.replace_all(" County", ""),
        )

        logger.info(f"Organisation units DataFrame created with {df.shape[0]} rows.")
        return df

    except Exception as e:
        logger.exception(f"Failed to transform organisation units after extraction: {e}")
        return None


if __name__ == "__main__":
    load_dotenv()

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")

    organizations_df = get_organisation_units(DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD)

    if organizations_df is not None:
        logger.success("Organisation units loaded and processed successfully.")
        print(organizations_df.head())
