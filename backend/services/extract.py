import asyncio
from datetime import date

import polars as pl
from loguru import logger

from backend.core.enums import Program
from backend.schemas.shared import KhisCredentials
from backend.services.utils.extract import (
    extract_historical_data_from_khis,
    first_day_of_month,
    generate_period_strings,
    get_org_units_ids_df,
    validate_source_to_destination,
)
from backend.services.utils.load import (
    delete_existing_data_for_periods,
    save_df_to_db
)
from backend.services.utils.transform import (
    get_data_elements_ids,
    iter_df_chunks
)



class KhisHistoricalDataExtractor:
    """
    Extract historical KHIS data in facility batches and persist to Postgres.

    What it does (simple ETL):
    1) Builds target periods from a date range
    2) Loads facility org units from the DB
    3) Resolves data element IDs by program (FP/MNCH)
    4) Deletes existing rows for the target periods (one-time cleanup)
    5) Extracts data from KHIS in facility chunks and appends to the output table
    """

    def __init__(
            self,
            creds: KhisCredentials,
            program: Program,
            db_connection_uri: str,
            output_table_name: str,
            facility_chunk_size: int = 400,
    ) -> None:
        """Initialize extractor and validate output table naming safety."""
        self.creds = creds
        self.db_connection_uri = db_connection_uri
        self.output_table_name = output_table_name
        self.facility_chunk_size = facility_chunk_size
        self.program = program

        # Safety guard: prevent accidentally writing FP into mnch_* tables (and vice versa)
        validate_source_to_destination(self.program, self.output_table_name)

    def _prepare_inputs(
            self,
            start_date: date,
            end_date: date
    ) -> tuple[list[str], list[str], str, str, pl.DataFrame]:
        """
        Prepare extraction inputs.

        Returns:
            target_periods: List of DHIS2 period strings (e.g., 202501, 202502, ...)
            data_element_ids: Program-specific data element IDs
            start_date_str: First day of month (YYYY-MM-DD) for API query
            end_date_str: First day of month (YYYY-MM-DD) for API query
            org_units_df: Org units dataframe
        """
        # Normalize dates to first day of month for the API payload
        start_date_str = first_day_of_month(start_date)
        end_date_str = first_day_of_month(end_date)

        # Periods for cleanup + downstream traceability
        target_periods = generate_period_strings(start_date, end_date)

        # Data elements are program-specific
        data_element_ids = get_data_elements_ids(self.program)

        # Organisation units as a polars df
        org_units_df = get_org_units_ids_df(self.db_connection_uri)

        return target_periods, data_element_ids, start_date_str, end_date_str, org_units_df

    def _pre_cleanup(self, target_periods: list[str]) -> None:
        """
        Delete existing rows for the target periods.

        We do cleanup once before processing any batch, then only APPEND.
        """
        if not target_periods:
            logger.warning("No target periods computed; skipping cleanup.")
            return

        logger.info("Performing pre-computation cleanup (delete existing rows for target periods)...")
        delete_existing_data_for_periods(
            periods=target_periods,
            database_uri=self.db_connection_uri,
            table_name=self.output_table_name,
        )

    async def run(self, start_date: date, end_date: date) -> None:
        """Run the extraction pipeline end-to-end."""
        # Prepare shared inputs
        (
            target_periods,
            data_element_ids,
            start_date_str,
            end_date_str,
            org_units_df
        ) = self._prepare_inputs(start_date, end_date)

        if org_units_df.height == 0:
            logger.warning("No Organisation Units IDs found in DB. Aborting.")
            return

        # One-time cleanup (fail-fast if it errors)
        try:
            self._pre_cleanup(target_periods)
        except Exception as exc:
            logger.exception("Critical error: failed to clean existing data. Aborting. Error: {}", exc)
            return

        logger.info(
            f"Starting {self.program.value,} KHIS historical data extraction:"
            f" {start_date_str} to {end_date_str} (table={self.output_table_name})"
        )

        total_chunks = (org_units_df.height // self.facility_chunk_size) + 1
        total_rows_processed = 0

        # Process facilities in batches to avoid huge API payloads
        for index, chunk_df in enumerate(iter_df_chunks(org_units_df, self.facility_chunk_size)):
            current_batch_num = index + 1

            try:
                facility_ids = chunk_df.select("facility_id").to_series().to_list()
                logger.info(
                    f"Processing batch {current_batch_num}/{total_chunks} ({len(facility_ids)} facilities)"
                )

                # KHIS extraction (sync function) — if it blocks heavily in an async server,
                # you can wrap it in asyncio.to_thread(...) later without changing calling code.
                batch_data = extract_historical_data_from_khis(
                    base_url=self.creds.base_url,
                    username=self.creds.username,
                    password=self.creds.password,
                    org_unit_ids=facility_ids,
                    data_element_ids=data_element_ids,
                    start_date=start_date_str,
                    end_date=end_date_str,
                )

                # Incremental DB append (cleanup already happened)
                if batch_data.height > 0:
                    save_df_to_db(
                        df=batch_data,
                        database_uri=self.db_connection_uri,
                        table_name=self.output_table_name,
                        if_table_exists="append",
                    )
                    total_rows_processed += batch_data.height
                    logger.info("Batch {} saved: {} rows.", current_batch_num, batch_data.height)
                else:
                    logger.warning("Batch {} returned no data.", current_batch_num)

            except Exception as exc:
                logger.exception("Failed to process batch {}: {}", current_batch_num, exc)
                return

        logger.success(
            "Completed KHIS extraction for {}. Total rows processed: {}",
            self.program.value,
            total_rows_processed,
        )

    def run_in_bg(self, *, start_date: date, end_date: date) -> asyncio.Task:
        """
        Schedule the extraction on the current running event loop.

        Use this in FastAPI/background contexts where a loop already exists.
        """
        return asyncio.create_task(self.run(start_date=start_date, end_date=end_date))


# --------------------------------------------------------------------------------------
# Example CLI usage
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    from backend.core.env_config import config

    extractor = KhisHistoricalDataExtractor(
        creds=KhisCredentials(
            base_url=config.DHIS2_BASE_URL,
            username=config.DHIS2_USERNAME,
            password=config.DHIS2_PASSWORD,
        ),
        db_connection_uri=config.DATABASE_URL,
        output_table_name=config.MNCH_KHIS_RAW_DATA_TABLE_NANE,
        facility_chunk_size=400,
        program=Program.MNCH,
    )

    # For CLI scripts, run directly (no background task needed)
    asyncio.run(
        extractor.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31)
        )
    )
