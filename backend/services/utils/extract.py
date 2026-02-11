import io
from datetime import date, datetime
from typing import Literal

import requests
from dateutil.relativedelta import relativedelta
from loguru import logger
import polars as pl

from backend.core.enums import Program


def first_day_of_month(d: date) -> str:
    logger.debug(f"Getting the first day of month for {d}")
    return str(d.replace(day=1))


def generate_period_strings(start: date, end: date) -> list[str]:
    """
    Generates a list of date strings (first day of each month) between start and end.
    """
    periods = []
    while start <= end:
        # Assuming your DB stores periods as 'YYYY-MM-DD'.
        # If DHIS2 returns '202310', change this line to: current.strftime("%Y%m")
        periods.append(start)

        # Increment to next month
        if start.month == 12:
            start = date(start.year + 1, 1, 1)
        else:
            start = date(start.year, start.month + 1, 1)
    #
    logger.debug(f"Periods: {periods}")
    #
    # # normalize dates
    periods = [first_day_of_month(item) for item in periods if periods]
    return periods



def get_org_units_ids_df(db_connection_uri: str) -> pl.DataFrame:
    """
    This function retrieves the Org Units IDs as a dataframe from the database.

    Assumptions:
        - organisation_units table exists in the db_connection_uri

    Parameters
        - db_connection_uri (str): The database connection string

    Returns
        - One column polars data frame containing the Org Units IDs
    """
    query = "SELECT facility_id FROM organisation_units"

    logger.info("Fetching org units ids...")
    org_units_df = pl.read_database_uri(query=query, uri=db_connection_uri)

    logger.debug(f"Shape of Org units metadata: {org_units_df.shape}")
    return org_units_df


def make_api_call(url: str, username: str, password: str) -> requests.Response:
    # --- API request ---
    try:
        response = requests.get(url, auth=(username, password))
    except requests.RequestException as e:
        raise RuntimeError(f"Network error while requesting data from DHIS2: {e}")

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to retrieve data. Status: {response.status_code} | Response: {response.text}"
        )
    return response


def generate_khis_data_api_url(
        base_url: str,
        data_element_ids: list[str],
        org_unit_ids: list[str],
        start_date: str,
        end_date: str,
        output_id_scheme: Literal["UID", "NAME", "CODE"] = "UID",
) -> str:
    """
    Constructs a DHIS2 Analytics API URL to export data in CSV format.

    This function generates a URL based on the 'dimension' strategy used by DHIS2.
    It automatically calculates monthly periods between the start and end dates.

    Args:
        base_url (str): The root URL of the DHIS2 instance (e.g., 'https://play.dhis2.org/2.39').
        data_element_ids (List[str]): A list of DHIS2 Data Element UIDs (e.g., ['J6qnTev1LXw']).
        org_unit_ids (List[str]): A list of Organisation Unit UIDs.
            Note: 'USER_ORGUNIT' is automatically prepended to this list.
        start_date (str): The start date string in 'YYYY-MM-DD' format.
        end_date (str): The end date string in 'YYYY-MM-DD' format.
        output_id_scheme (Literal["UID", "NAME", "CODE"], optional): Defines how data_extraction
            should be identified in the response. Defaults to "UID".

    Returns:
        str: A fully formatted and encoded API URL string ready for a GET request.
    """
    # Base analytics endpoint for CSV export
    analytics_endpoint = f"{base_url}/api/analytics.csv?"

    # --- Construct Data Elements Dimension (dx) ---
    # DHIS2 uses URL encoding: %3A is ':' and %3B is ';'
    # Format: dimension=dx:ITEM_1;ITEM_2;ITEM_3
    data_elements_spec = "dimension=dx%3A" + "%3B".join(data_element_ids) + "&"

    # --- Construct Org Unit Dimension (ou) ---
    org_units_spec = "dimension=ou%3A" + "%3B".join(org_unit_ids) + "&"

    # --- Generate Period Dimension (pe) ---
    # Convert 'YYYY-MM-DD' range into DHIS2 monthly ISO format (YYYYMM)
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    periods = []
    current_period = start

    # Loop through months and create specific period strings
    while current_period <= end:
        periods.append(current_period.strftime("%Y%m"))
        current_period += relativedelta(months=1)

    periods_spec = "dimension=pe%3A" + "%3B".join(periods) + "&"

    # --- Default Parameters ---
    default_spec = (
        f"showHierarchy=false&hierarchyMeta=false&includeMetadataDetails=true&includeNumDen=false"
        f"&skipRounding=false&completedOnly=false&outputIdScheme={output_id_scheme}"
    )

    # Combine all parts
    full_api_url = (
            analytics_endpoint
            + data_elements_spec
            + org_units_spec
            + periods_spec
            + default_spec
    )

    logger.info(f"Successfully generated API URL")
    return full_api_url



def extract_historical_data_from_khis(
        base_url: str,
        username: str,
        password: str,
        org_unit_ids: list[str],
        data_element_ids: list[str],
        start_date: str,
        end_date: str,
        output_id_scheme: Literal["UID", "NAME", "CODE"] = "UID"
) -> pl.DataFrame:
    """
    Fetches, parses, and cleans historical data from a DHIS2 instance (KHIS).

    This function orchestrates the API call, handles network or parsing errors gracefully
    by returning an empty schema-compliant DataFrame, and performs standard cleaning steps
    (renaming columns, formatting dates, and removing suffix artifacts).

    Args:
        base_url (str): The root URL of the DHIS2 instance.
        username (str): DHIS2 username for authentication.
        password (str): DHIS2 password for authentication.
        org_unit_ids (List[str]): List of Organisation Unit IDs to query.
        data_element_ids (List[str]): List of Data Element IDs to query.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        output_id_scheme (Literal["UID", "NAME", "CODE"], optional):
            The identifier scheme for metadata. Defaults to "UID".

    Returns:
        pl.DataFrame: A Polars DataFrame containing the cleaned data.

        If the query fails or returns no data, returns an empty DataFrame with
        the columns: ['analytic', 'org_unit', 'period', 'value'].
    """

    # Generate the specific API URL using the helper function
    url = generate_khis_data_api_url(
        base_url,
        data_element_ids,
        org_unit_ids,
        start_date,
        end_date,
        output_id_scheme
    )

    logger.info(f"Initiating data fetch for period: {start_date} to {end_date}")

    # --- Define Fallback DataFrame ---
    # This empty DataFrame ensures downstream code doesn't break if the API fails.
    # It defines the expected schema (types) for the pipeline.
    sample_df_if_query_fails = pl.DataFrame(schema={
        "analytic": pl.String,
        "org_unit": pl.String,
        "period": pl.Date,  # Note: Changed to Date to match success schema
        "value": pl.Float64
    })

    # --- 1. Execute API Request ---
    try:
        response = requests.get(url, auth=(username, password), timeout=60)
        response.raise_for_status()  # Raise error for 4xx/5xx status codes
    except requests.exceptions.RequestException as e:
        logger.exception(f"Network error requesting data from KHIS: {e}")
        return sample_df_if_query_fails

    logger.info(f"API request successful. Status: {response.status_code}")

    # --- 2. Parse and Clean Data ---
    try:
        # Check if the response content is empty before trying to parse
        if not response.content:
            logger.warning("API returned empty content.")
            return sample_df_if_query_fails

        # Read CSV directly from bytes (more memory efficient than converting to string first)
        response_data = pl.read_csv(
            source=io.BytesIO(response.content),
            schema_overrides={"Value": pl.Float64},  # ensures decimals are valid
            infer_schema_length = 5000,  # optional, helps with large KHIS exports
        )
        logger.debug(f"Raw data shape: {response_data.shape}")

        if response_data.height == 0:
            logger.warning(f"No rows returned in data for {start_date} to {end_date}")
            return sample_df_if_query_fails

        # --- 3. Transformation Pipeline ---
        transformed_df = (
            response_data
            # Standardize column names for internal consistency
            .rename({
                "Data": "analytic",
                "Organisation unit": "org_unit",
                "Period": "period",
                "Value": "value"
            })
            .with_columns(
                # Period Conversion:
                # 1. Cast integer '202501' to String
                # 2. Parse 'YYYYMM' format into a Date object (defaults to 1st of month)
                period=pl.col("period").cast(pl.String).str.strptime(pl.Date, "%Y%m"),

                # Analytic Cleaning:
                # Removes the category option combo UID suffix
                # likely representing "Dispensed" or "Default".
                analytic=pl.col("analytic").str.replace_all(r"\.to0Pssxkq4S|\.hDCmaVTXH7W", ""),
            )
        )

        logger.info(f"Successfully processed {transformed_df.height} rows.")

        return transformed_df

    except Exception as e:
        # Catch parsing errors (e.g., malformed CSV, missing columns)
        logger.exception(f"Error parsing or transforming CSV data: {e}")
        return sample_df_if_query_fails


def validate_source_to_destination(
    program: Program,
    dest_table: str,
) -> None:
    """
    Validate that the selected program aligns with the destination table prefix.

    This is a safety guard to prevent accidental writes to the wrong destination
    table (e.g., attempting to write MNCH data into an FP table).

    Args:
        program: Program identifier ("FP" or "MNCH").
        dest_table: Destination table name.

    Raises:
        ValueError: If the destination table does not match the expected prefix.
    """
    if program == Program.MNCH and not dest_table.startswith("mnch_"):
        logger.warning(
            f"Program {program.value} does not match destination table {dest_table} "
            f"Aborting to avoid corrupting destination table."
        )
        raise ValueError("Program does not align with destination table.")

    if program == Program.FP and not dest_table.startswith("fp_"):
        logger.warning(
            f"Program {program.value} does not match destination table {dest_table} "
            f"Aborting to avoid corrupting destination table."
        )
        raise ValueError("Program does not align with destination table.")
