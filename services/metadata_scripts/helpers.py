from typing import Literal
from loguru import logger
import polars as pl


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
