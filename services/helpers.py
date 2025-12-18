import io
import os

import requests
import polars as pl
from dotenv import load_dotenv
from datetime import datetime, date
from typing import Literal, List
from loguru import logger
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text


def generate_khis_data_api_url(
        base_url: str,
        data_element_ids: List[str],
        org_unit_ids: List[str],
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


def save_df_to_db(
        df: pl.DataFrame,
        db_url: str,
        table_name: str,
        if_table_exists:  Literal["replace", "append", "fail"]
) -> None:
    """
    Saves cleaned dataframe to database
    """
    try:
        logger.info(f"Saving cleaned dataframe to database: {table_name}")
        df.write_database(
            table_name=table_name,
            connection=db_url,
            if_table_exists=if_table_exists,
        )
        logger.success(f"Data elements successfully saved to {table_name}")
    except Exception as e:
        logger.error(f"Error saving cleaned dataframe to database: {str(e)}")


def make_orgunits_hierarchy(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert organisation units hierarchy from long format to wide format.

    This function:
    - Splits the input DataFrame by levels (1-5)
    - Renames columns according to the level
    - Joins all levels to create a single wide-format hierarchy DataFrame

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame containing organisation units with columns:
        ['id', 'name', 'parent_id', 'level', 'code'].

    Returns
    -------
    pl.DataFrame
        A wide-format Polars DataFrame containing hierarchical organisation unit information.
    """
    level_5_df = (
        df.filter(pl.col("level") == 5)
        .drop("level")
        .rename({"id": "facility_id", "name": "facility_name", "parent_id": "ward_id", "code": "mfl_code"})
    )

    logger.debug("Level 5 dataframe")
    logger.info(level_5_df.head())

    level_4_df = (
        df.filter(pl.col("level") == 4)
        .drop("level")
        .rename({"name": "ward_name", "id": "ward_id", "parent_id": "sub_county_id", "code": "ward_code"})
    )

    logger.debug("Level 4 dataframe")
    logger.info(level_4_df.head())

    level_3_df = (
        df.filter(pl.col("level") == 3)
        .drop("level")
        .rename({"name": "sub_county_name", "id": "sub_county_id", "parent_id": "county_id", "code": "sub_county_code"})
    )

    logger.debug("Level 3 dataframe")
    logger.info(level_3_df.head())

    level_2_df = (
        df.filter(pl.col("level") == 2)
        .drop("level")
        .rename({"name": "county_name", "id": "county_id", "parent_id": "country_id", "code": "county_code"})
    )

    logger.debug("Level 2 dataframe")
    logger.info(level_2_df.head())

    level_1_df = (
        df.filter(pl.col("level") == 1)
        .drop(["level", "parent_id"])
        .rename({"name": "country_name", "id": "country_id", "code": "country_code"})
    )

    logger.debug("Level 1 dataframe")
    logger.info(level_1_df.head())

    # join the data frames together
    organized_df = (
        level_5_df
        .join(level_4_df, on="ward_id", how="inner")
        .join(level_3_df, on="sub_county_id", how="inner")
        .join(level_2_df, on="county_id", how="inner")
        .join(level_1_df, on="country_id", how="inner")
    )

    return organized_df


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


def extract_historical_data_from_khis(
        base_url: str,
        username: str,
        password: str,
        org_unit_ids: List[str],
        data_element_ids: List[str],
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
                # Removes the category option combo UID suffix (e.g., ".to0Pssxkq4S")
                # likely representing "Dispensed" or "Default".
                analytic=pl.col("analytic").str.replace_all(r"\.to0Pssxkq4S", ""),
            )
        )

        logger.info(f"Successfully processed {transformed_df.height} rows.")

        return transformed_df

    except Exception as e:
        # Catch parsing errors (e.g., malformed CSV, missing columns)
        logger.exception(f"Error parsing or transforming CSV data: {e}")
        return sample_df_if_query_fails


def get_fp_data_elements_ids(db_connection_uri: str) -> dict[str, pl.DataFrame]:
    """
    Retrieves Family Planning (FP) product IDs for consumption and service elements
    from the database.

    Args:
        db_connection_uri (str): The database connection string.

    Returns:
        Dict[str, pl.DataFrame]: A dictionary containing two Polars DataFrames:
            - 'consumption_ids': DataFrame of consumption elements.
            - 'service_ids': DataFrame of service elements.
            Returns {} if an error occurs.
    """
    try:
        # Define the IDs for consumption and service elements
        consumption_ids = [
            "hH9gmEmEhH4", "bGGT0F7iRxt", "J6qnTev1LXw", "hXa1xyUMfTa", "qaBPR9wbWku",
            "dl4JcBnxu0X", "AR7RhdC90IV", "zXbxl6y97mi", "MsS41X1GEFr", "XgJfT71Unkn",
            "APbXNRovb5w", "AVDzuypqGt9", "tfPZ6sGgh4q"
        ]

        service_ids = [
            "cV4qoKSYiBs", "Fxb4iVJdw2g", "paDQStynGGD", "BQmcVE8fex4", "uHM6lzLXDBd",
            "fYCo4peO0yE", "PgQIx7Hq1kp", "NMCIxSeGpS3", "CJdFYcZ1zOq", "TUHzoPGLM3t",
            "Wv02gixbRpT", "hRktPfPEegP"
        ]

        # Format list for SQL IN clause: 'id1', 'id2', 'id3'
        consumption_ids_str = "', '".join(consumption_ids)
        service_ids_str = "', '".join(service_ids)

        # Construct SQL queries
        # Note: Added params for cleaner query construction, though f-string is used here
        # to match the logic of pasting IDs directly into the IN clause.
        consumption_query = f"""
            SELECT
                name,
                CONCAT(id, '.to0Pssxkq4S') AS id
            FROM
                data_elements
            WHERE
                id IN ('{consumption_ids_str}')
        """

        service_query = f"""
            SELECT
                name,
                id
            FROM data_elements
            WHERE
                id IN ('{service_ids_str}')
        """

        # Execute SQL queries using Polars
        logger.info("Fetching FP consumption elements...")
        consumption_df = pl.read_database_uri(query=consumption_query, uri=db_connection_uri)

        logger.info("Fetching FP service elements...")
        service_df = pl.read_database_uri(query=service_query, uri=db_connection_uri)

        response = {
            "consumption_ids": consumption_df,
            "service_ids": service_df,
            "combined_consumption_service_ids": pl.concat([consumption_df, service_df], how="vertical"),
        }

        logger.debug(f"Response: {response['combined_consumption_service_ids'].shape}")
        return response

    except Exception as e:
        logger.exception(f"An error occurred while fetching FP data elements: {e}")
        return {}

def get_org_units_ids(db_connection_uri: str) -> pl.DataFrame:
    query = "SELECT facility_id FROM organisation_units"

    logger.info("Fetching org units ids...")
    org_units_df = pl.read_database_uri(query=query, uri=db_connection_uri)

    logger.debug(f"Shape of Org units metadata: {org_units_df.shape}")
    return org_units_df



def delete_existing_data_for_periods(
        periods: list[str],
        connection_uri: str,
        table_name: str
) -> None:
    """
    Deletes rows from the database that match the provided list of periods.
    This ensures we don't insert duplicate data for the same timeframe.
    """
    if not periods:
        return


    # Create engine (SQLAlchemy is best for executing raw DELETE statements)
    engine = create_engine(connection_uri)

    # Format periods for SQL IN clause: '2024-01-01', '2024-02-01'
    # Note: Ensure your DB 'period' column format matches these values (Date vs String)
    periods_str = "', '".join(periods)

    delete_query = text(f"DELETE FROM {table_name} WHERE period IN ('{periods_str}')")

    try:
        with engine.begin() as conn:  # 'begin' automatically handles commit/rollback
            result = conn.execute(delete_query)
            logger.warning(f"Deleted existing {result.rowcount} rows "
                           f"matching periods: {periods}")
    except Exception as e:
        logger.error(f"Failed to delete existing periods: {e}")
        raise e

def first_day_of_month(d: date) -> str:
    logger.debug(f"Getting the first day of month for {d}")
    return str(d.replace(day=1))

if __name__ == "__main__":
    load_dotenv()

    # Ensure env var exists to prevent runtime errors later
    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    FP_DB_URL = os.getenv("FP_DB_URL")

    generate_khis_data_api_url(
        base_url=DHIS2_BASE_URL,
        data_element_ids=["J6qnTev1LXw", "hXa1xyUMfTa", "AVDzuypqGt9"],
        org_unit_ids=["vvOK1BxTbet",],
        start_date="2025-01-01",
        end_date="2025-03-01",
        output_id_scheme="NAME",
    )


    extract_historical_data_from_khis(
        base_url=DHIS2_BASE_URL,
        username=DHIS2_USERNAME,
        password=DHIS2_PASSWORD,
        data_element_ids=["J6qnTev1LXw.to0Pssxkq4S", "J6qnTev1LXw", "hXa1xyUMfTa", "AVDzuypqGt9"],
        org_unit_ids=["vvOK1BxTbet", "HfVjCurKxh2"],
        start_date="2025-01-01",
        end_date="2025-03-01",
        output_id_scheme="UID",
    )

    get_fp_data_elements_ids(FP_DB_URL)
    get_org_units_ids(FP_DB_URL)