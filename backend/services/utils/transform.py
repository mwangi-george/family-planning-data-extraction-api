from typing import Generator, Literal

import polars as pl
from loguru import logger

from backend.core.enums import Program


def get_data_elements_ids(program: Program) -> list[str]:
    """
    Retrieves program specific product IDs for consumption and service elements
    from the database as a polars dataframe.

    Args:
        program (Program): The program to retrieve data elements from.

    Returns:
        list[str]: A list of the program specific product IDs.
    """
    if program == Program.FP.value:
        # Define the IDs for consumption and service elements
        data_element_ids = [
            # Consumption data ids with .dispensed id--> MOH 747A
            "hH9gmEmEhH4.to0Pssxkq4S", "bGGT0F7iRxt.to0Pssxkq4S", "J6qnTev1LXw.to0Pssxkq4S",
            "hXa1xyUMfTa.to0Pssxkq4S", "qaBPR9wbWku.to0Pssxkq4S", "dl4JcBnxu0X.to0Pssxkq4S",
            "AR7RhdC90IV.to0Pssxkq4S", "zXbxl6y97mi.to0Pssxkq4S", "MsS41X1GEFr.to0Pssxkq4S",
            "XgJfT71Unkn.to0Pssxkq4S", "APbXNRovb5w.to0Pssxkq4S", "AVDzuypqGt9.to0Pssxkq4S",
            "tfPZ6sGgh4q.to0Pssxkq4S",

            # Service data ids --> MOH 711
            "cV4qoKSYiBs", "Fxb4iVJdw2g", "paDQStynGGD", "BQmcVE8fex4", "uHM6lzLXDBd",
            "fYCo4peO0yE", "PgQIx7Hq1kp", "NMCIxSeGpS3", "CJdFYcZ1zOq", "TUHzoPGLM3t",
            "Wv02gixbRpT", "hRktPfPEegP"
        ]
        return data_element_ids

    elif program == program.MNCH.value:
        data_element_ids = [
            # Consumption data ids with .dispensed id--> MOH 647
            "GOFxghdlf5n.hDCmaVTXH7W", "qoEFejcajz1.hDCmaVTXH7W", "WbDKZsPHAOK.hDCmaVTXH7W",
            "pxdnKL8X8aP.hDCmaVTXH7W", "BT8vV7Z7anH.hDCmaVTXH7W", "QYT9nPwdqOz.hDCmaVTXH7W",
            "rEQd6IDeXWT.hDCmaVTXH7W"
        ]
        return data_element_ids
    else:
        logger.error(f"Unknown program: {program}")
        return []


def iter_df_chunks(
        df: pl.DataFrame,
        size: int
) -> Generator[pl.DataFrame, None, None]:
    """
    Yield successive row-based chunks from a Polars DataFrame.

    This generator splits a given ``polars.DataFrame`` into smaller
    DataFrame slices of a specified size. It is useful for batch
    processing large datasets (e.g., bulk database inserts, API uploads,
    or memory-efficient transformations).

    Args:
        df (pl.DataFrame):
            The input Polars DataFrame to be chunked.

        size (int):
            The maximum number of rows per chunk. Must be a positive integer.

    Yields:
        pl.DataFrame:
            A Polars DataFrame containing up to ``size`` rows.
    """
    for i in range(0, df.height, size):
        yield df.slice(i, size)


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
