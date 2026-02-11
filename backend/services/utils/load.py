from typing import Literal

import polars as pl
from loguru import logger
from sqlalchemy import create_engine, inspect, text


def check_table_exists(database_uri: str, table_name: str, schema=None):
    """
    Checks if a table exists in the database using the Inspector.

    :param database_uri: The database connection string for the Inspector.
    :param table_name: The name of the table (string).
    :param schema: Optional schema name (string).
    :return: Boolean indicating if the table exists.
    """
    engine = create_engine(database_uri)

    inspector = inspect(engine)

    # check for table existence
    return inspector.has_table(table_name, schema=schema)


def save_df_to_db(
        df: pl.DataFrame,
        database_uri: str,
        table_name: str,
        if_table_exists: Literal["replace", "append", "fail"]
) -> None:
    """
    Saves cleaned dataframe to database
    """
    if not check_table_exists(database_uri, table_name):
        logger.info(
            f"Table {table_name} does not exists in database..."
            f"Creating table {table_name}..."
        )

    try:
        logger.info(f"Saving cleaned dataframe to database: {table_name}")
        df.write_database(
            table_name=table_name,
            connection=database_uri,
            if_table_exists=if_table_exists,
        )
        logger.success(f"Data elements successfully saved to {table_name}")
    except Exception as e:
        logger.error(f"Error saving cleaned dataframe to database: {str(e)}")


def delete_existing_data_for_periods(
        periods: list[str],
        database_uri: str,
        table_name: str
) -> None:
    """
    Deletes rows from the database that match the provided list of periods.
    This ensures we don't insert duplicate data for the same timeframe.
    """
    if not periods:
        return

    if not check_table_exists(database_uri, table_name):
        logger.info(f"Table {table_name} does not exists in database")
        return

    # Create engine (SQLAlchemy is best for executing raw DELETE statements)
    engine = create_engine(database_uri)

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



