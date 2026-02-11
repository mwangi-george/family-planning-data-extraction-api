import asyncio
from typing import Any

import polars as pl
from loguru import logger

from backend.schemas.shared import KhisCredentials
from backend.services.utils.extract import make_api_call
from backend.services.utils.load import save_df_to_db
from backend.services.utils.transform import make_orgunits_hierarchy



class KhisMetadataExtractor:
    """
    Extract and persist DHIS2 metadata (org units, data elements, indicators).

    Keeps extraction logic in one place and exposes:
      - extract_*() methods that return Polars DataFrames (or None on recoverable failures)
      - run() to extract everything and write to DB
      - run_in_bg() to schedule on an existing event loop (FastAPI use case)
    """

    def __init__(self, *, creds: KhisCredentials, database_url: str) -> None:
        """Create a metadata extractor."""
        self.creds = creds
        self.database_url = database_url

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _api_get(self, path: str) -> dict[str, Any]:
        """
        Make a DHIS2 API call and return parsed JSON.

        Raises:
            RuntimeError: if response is not valid JSON.
        """
        # Ensure we don't end up with double slashes
        base = str(self.creds.base_url).rstrip("/")
        url = f"{base}{path}"

        logger.info("Requesting DHIS2 metadata: {}", url)
        response = make_api_call(url, self.creds.username, self.creds.password)

        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(f"DHIS2 returned invalid JSON for {url}") from exc

    @staticmethod
    def _ensure_list(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
        """Validate payload shape and return list content under a key."""
        value = payload.get(key, [])
        if not isinstance(value, list):
            raise RuntimeError(
                f"Unexpected JSON structure: '{key}' is not a list. Keys: {list(payload.keys())}"
            )
        return value

    # ---------------------------------------------------------------------
    # Extraction methods
    # ---------------------------------------------------------------------
    def extract_data_elements(self) -> pl.DataFrame | None:
        """Fetch DHIS2 data elements as a Polars DataFrame."""
        try:
            payload = self._api_get(
                "/api/dataElements?fields=name,id,shortName,displayName&paging=false"
            )
            data = self._ensure_list(payload, "dataElements")

            df = (
                pl.DataFrame(data)
                .rename({"shortName": "short_name", "displayName": "display_name"})
            )

            logger.success("Data elements retrieved: rows={}", df.height)
            return df
        except Exception as exc:
            logger.exception("Failed to extract data elements: {}", exc)
            return None

    def extract_indicators(self) -> pl.DataFrame | None:
        """Fetch DHIS2 indicators as a Polars DataFrame."""
        try:
            payload = self._api_get("/api/indicators?fields=name,id&paging=false")
            data = self._ensure_list(payload, "indicators")

            df = pl.DataFrame(data)
            logger.success("Indicators retrieved: rows={}", df.height)
            return df
        except Exception as exc:
            logger.exception("Failed to extract indicators: {}", exc)
            return None

    def extract_organisation_units(self) -> pl.DataFrame | None:
        """
        Fetch DHIS2 organisation units and return a cleaned hierarchy DataFrame.

        Notes:
          - Extracts parent.id into `parent_id`
          - Removes common suffixes from names (Ward/Sub County/County)
          - Converts long hierarchy to wide using `make_orgunits_hierarchy`
        """
        try:
            payload = self._api_get(
                "/api/organisationUnits?fields=name,id,parent,level,code&paging=false"
            )
            data = self._ensure_list(payload, "organisationUnits")

            df = (
                pl.DataFrame(data)
                .with_columns(parent_id=pl.col("parent").struct.field("id"))
                .drop("parent")
                .with_columns(
                    name=pl.col("name").str.replace_all(r" Ward| Sub County| County", "")
                )
            )

            # Long -> wide hierarchy
            df = make_orgunits_hierarchy(df)

            logger.success("Organisation units retrieved: rows={}", df.height)
            return df
        except Exception as exc:
            logger.exception("Failed to extract organisation units: {}", exc)
            return None

    # ---------------------------------------------------------------------
    # Orchestration / DB write
    # ---------------------------------------------------------------------
    async def run(self) -> None:
        """
        Extract all metadata and save to DB.

        Uses replace mode to ensure tables are refreshed as a single source of truth.
        """
        logger.info("Starting DHIS2 metadata ETL...")

        # Extraction
        org_units_df = self.extract_organisation_units()
        data_elements_df = self.extract_data_elements()
        indicators_df = self.extract_indicators()

        logger.info("Metadata extraction completed")

        # Persist (only save datasets that were successfully extracted)
        if org_units_df is not None:
            logger.info("Saving organisation units to DB...")
            save_df_to_db(org_units_df, self.database_url, "organisation_units", "replace")
        else:
            logger.warning("Skipping DB save for organisation_units (extraction failed).")

        if data_elements_df is not None:
            logger.info("Saving data elements to DB...")
            save_df_to_db(data_elements_df, self.database_url, "data_elements", "replace")
        else:
            logger.warning("Skipping DB save for data_elements (extraction failed).")

        if indicators_df is not None:
            logger.info("Saving indicators to DB...")
            save_df_to_db(indicators_df, self.database_url, "indicators", "replace")
        else:
            logger.warning("Skipping DB save for indicators (extraction failed).")

        logger.success("Metadata ETL completed.")

    def run_in_bg(self) -> asyncio.Task:
        """
        Schedule metadata ETL on the current running loop.

        Use inside FastAPI/background contexts where an event loop already exists.
        """
        return asyncio.create_task(self.run())


# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    from backend.core.env_config import config

    extractor = KhisMetadataExtractor(
        creds=KhisCredentials(
            base_url=config.DHIS2_BASE_URL,
            username=config.DHIS2_USERNAME,
            password=config.DHIS2_PASSWORD,
        ),
        database_url=config.DATABASE_URL,
    )

    asyncio.run(extractor.run())
