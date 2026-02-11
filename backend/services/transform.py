import asyncio
import gc
from typing import Any

import polars as pl
from loguru import logger

from backend.core.enums import Program
from backend.services.utils.extract import validate_source_to_destination
from backend.services.utils.load import save_df_to_db


class DataTransformationPipeline:
    """
    Minimal ETL pipeline for FP / MNCH data:
    - Extract raw + metadata tables from DB
    - Transform to county aggregation (plus optional business rules)
    - Add national aggregates
    - Load results back to DB
    """

    # Analytic ID -> standardized analytic name used in downstream reporting.
    ANALYTIC_ID_MAP: dict[str, str] = {
        # FP
        "dl4JcBnxu0X": "POPs", "uHM6lzLXDBd": "POPs",
        "tfPZ6sGgh4q": "Non-Hormonal IUCD", "hRktPfPEegP": "Non-Hormonal IUCD",
        "cV4qoKSYiBs": "Male Condoms", "AVDzuypqGt9": "Male Condoms",
        "APbXNRovb5w": "Levoplant",
        "CJdFYcZ1zOq": "Implanon", "XgJfT71Unkn": "Implanon",
        "MsS41X1GEFr": "Jadelle",
        "zXbxl6y97mi": "Hormonal IUCD", "Wv02gixbRpT": "Hormonal IUCD",
        "Fxb4iVJdw2g": "Female Condoms", "AR7RhdC90IV": "Female Condoms",
        "paDQStynGGD": "EC Pills", "qaBPR9wbWku": "EC Pills",
        "NMCIxSeGpS3": "DMPA-SC", "hXa1xyUMfTa": "DMPA-SC",
        "PgQIx7Hq1kp": "DMPA-IM", "J6qnTev1LXw": "DMPA-IM",
        "fYCo4peO0yE": "Cycle Beads", "bGGT0F7iRxt": "Cycle Beads",
        "BQmcVE8fex4": "COCs", "hH9gmEmEhH4": "COCs",
        "TUHzoPGLM3t": "2 Rod",

        # MNCH
        "GOFxghdlf5n": "Chlorhexidine Gel",
        "qoEFejcajz1": "Tetracycline Eye Ointment",
        "WbDKZsPHAOK": "Magnesium Sulphate Injection",
        "pxdnKL8X8aP": "Iron and Folic Acid Supplementation",
        "BT8vV7Z7anH": "Gentamicin Injection",
        "QYT9nPwdqOz": "Benzyl Penicillin Injection",
        "rEQd6IDeXWT": "Oxytocin Injection",
    }

    # Multipliers applied ONLY to FP service data for selected methods.
    FP_SERVICE_ADJUSTMENTS: dict[str, float] = {
        "COCs": 1.25,
        "POPs": 0.5,
        "Female Condoms": 10.0,
        "Male Condoms": 10.0,
    }

    def __init__(
        self,
        database_url: str,
        program: Program,
        input_table_name: str,
        output_table_name: str,
    ) -> None:
        """Create a pipeline instance and validate table alignment."""
        self.database_uri = database_url
        self.program = program
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name

        # Safety guard: prevent cross-program writes (e.g., FP -> mnch_* tables)
        validate_source_to_destination(self.program.value, self.input_table_name)
        validate_source_to_destination(self.program.value, self.output_table_name)

    def _fetch_pipeline_data_from_db(self) -> dict[str, pl.DataFrame]:
        """Load raw data + reference metadata required for transformations."""
        logger.info("Extracting raw + metadata tables from DB...")

        queries = {
            "raw_df": (
                f"SELECT analytic as analytic_id, org_unit, period, value "
                f"FROM {self.input_table_name}"
            ),
            "org_units": "SELECT facility_id as org_unit, county_name FROM organisation_units",
            "elements": "SELECT id as analytic_id, name as analytic_name FROM data_elements",
        }

        datasets: dict[str, pl.DataFrame] = {}
        for key, query in queries.items():
            df = pl.read_database_uri(uri=self.database_uri, query=query)
            logger.info("Loaded {}: rows={} cols={}", key, df.height, df.width)
            datasets[key] = df

        # Simple schema sanity checks (fail early with readable errors)
        required_raw_cols = {"analytic_id", "org_unit", "period", "value"}
        if not required_raw_cols.issubset(set(datasets["raw_df"].columns)):
            missing = required_raw_cols - set(datasets["raw_df"].columns)
            raise ValueError(f"raw_df missing required columns: {sorted(missing)}")

        return datasets

    @staticmethod
    def _audit_duplicates(df: pl.DataFrame, dataset_name: str) -> None:
        """Log duplicate rows across all columns (helps spot double-counting)."""
        logger.info("Auditing {} for duplicate rows...", dataset_name)

        dup_mask = df.is_duplicated()              # boolean Series: True where row duplicates occur
        dup_count = int(dup_mask.sum())            # explicit int for safe logging

        if dup_count == 0:
            logger.info("No duplicates found in {}.", dataset_name)
            return

        logger.warning("Found {} duplicate rows in {}.", dup_count, dataset_name)

        # Show a small sample of org_unit + period affected (if those columns exist)
        sample_cols = [c for c in ["org_unit", "period"] if c in df.columns]
        if sample_cols:
            summary = df.filter(dup_mask).select(sample_cols).unique().sort(sample_cols)
            logger.info("Affected {} sample:\n{}", dataset_name, summary.head(5))

    def _apply_transformations(
        self,
        raw_df: pl.DataFrame,
        org_units: pl.DataFrame,
        elements: pl.DataFrame,
    ) -> pl.DataFrame:
        """Join metadata, derive method, apply FP service adjustments, aggregate to county."""
        logger.info("Transforming raw data to county aggregates...")

        # Join raw -> county and raw -> data element metadata
        joined_df = (
            raw_df
            .join(org_units, on="org_unit", how="inner")
            .join(elements, on="analytic_id", how="inner")
            .drop("org_unit")
        )

        # Map analytic_id -> standardized analytic name used for reporting
        joined_df = joined_df.with_columns(
            analytic=pl.col("analytic_id").replace_strict(
                self.ANALYTIC_ID_MAP,
                default="Unknown Product",
            )
        )

        # Identify reporting method based on element naming conventions
        # NOTE: Keep the same heuristics, just make them explicit & readable.
        joined_df = joined_df.with_columns(
            method=(
                pl.when(pl.col("analytic_name").str.contains("711"))
                .then(pl.lit("Service"))
                .when(pl.col("analytic_name").str.contains(r"(747|647)"))
                .then(pl.lit("Consumption"))
                .otherwise(pl.lit("Unknown Method"))
            )
        )

        # Apply multipliers ONLY to FP Service rows for specific analytics
        adjusted_df = joined_df.with_columns(
            value=pl.when(pl.col("method") == "Service")
            .then(
                pl.col("value") * pl.col("analytic").replace_strict(
                    self.FP_SERVICE_ADJUSTMENTS,
                    default=1.0,
                    return_dtype=pl.Float64,
                )
            )
            .otherwise(pl.col("value"))
        )

        # County aggregation: analytic + method + county + period
        county_df = (
            adjusted_df
            .rename({"county_name": "org_unit"})
            .select(["analytic", "method", "org_unit", "period", "value"])
            .group_by(["analytic", "method", "org_unit", "period"])
            .agg(pl.col("value").sum())
            .sort(["analytic", "method", "org_unit", "period"])
        )

        logger.info("County transformation complete: rows={}", county_df.height)
        return county_df

    @staticmethod
    def _generate_national_aggregates(county_df: pl.DataFrame) -> pl.DataFrame:
        """Sum all counties into national rows (org_unit='Kenya')."""
        logger.info("Generating national aggregates...")

        return (
            county_df
            .group_by(["analytic", "method", "period"])
            .agg(pl.col("value").sum())
            .with_columns(org_unit=pl.lit("Kenya"))
            .select(["analytic", "method", "org_unit", "period", "value"])
        )

    @staticmethod
    def _process_two_rod_split(df: pl.DataFrame, jadelle_ratio: float = 0.8) -> pl.DataFrame:
        """
        Split FP '2 Rod' service values into Jadelle and Levoplant.

        Only applies to rows where method == 'Service' and analytic == '2 Rod'.
        """
        logger.info("Applying '2 Rod' split rule (jadelle_ratio={})...", jadelle_ratio)

        if not (0.0 <= jadelle_ratio <= 1.0):
            raise ValueError("jadelle_ratio must be between 0.0 and 1.0")

        target = (pl.col("method") == "Service") & (pl.col("analytic") == "2 Rod")
        two_rod_df = df.filter(target)

        if two_rod_df.is_empty():
            logger.info("No '2 Rod' Service rows found; skipping split.")
            return df

        # Direct calculation is simpler than pivot/unpivot here.
        jadelle_df = two_rod_df.with_columns(
            analytic=pl.lit("Jadelle"),
            value=pl.col("value") * jadelle_ratio,
        )
        levoplant_df = two_rod_df.with_columns(
            analytic=pl.lit("Levoplant"),
            value=pl.col("value") * (1 - jadelle_ratio),
        )

        # Replace '2 Rod' rows with split rows
        return pl.concat([df.filter(~target), jadelle_df, levoplant_df], how="vertical").select(df.columns)

    async def run(self) -> None:
        """Run extract -> transform -> aggregate -> load, then clean up memory."""
        data: dict[str, pl.DataFrame] | None = None
        county_df: pl.DataFrame | None = None
        national_df: pl.DataFrame | None = None
        final_df: pl.DataFrame | None = None

        try:
            # NOTE: DB calls are blocking. If running under an async web server,
            # running these in a thread prevents event-loop blockage.
            data = await asyncio.to_thread(self._fetch_pipeline_data_from_db)

            self._audit_duplicates(data["raw_df"], dataset_name=self.input_table_name)

            county_df = self._apply_transformations(
                raw_df=data["raw_df"],
                org_units=data["org_units"],
                elements=data["elements"],
            )

            national_df = self._generate_national_aggregates(county_df)
            final_df = pl.concat([county_df, national_df], how="vertical")

            # Apply FP-only rule(s)
            if self.program == Program.FP:
                final_df = self._process_two_rod_split(final_df)

            logger.info("Final dataset ready: rows={} cols={}", final_df.height, final_df.width)

            # Save to DB (also blocking)
            await asyncio.to_thread(
                save_df_to_db,
                df=final_df,
                database_uri=self.database_uri,
                table_name=self.output_table_name,
                if_table_exists="replace",
            )

            logger.success(
                "{} pipeline complete. Saved to: {}",
                self.program.value,
                self.output_table_name,
            )

        except Exception as exc:
            logger.exception("{} pipeline failed.", self.program.value)
            raise RuntimeError(f"{self.program.value} pipeline failed: {exc}") from exc

        finally:
            # Keep cleanup lightweight and safe
            logger.info("Cleaning up memory...")
            data = None
            county_df = None
            national_df = None
            final_df = None
            gc.collect()
            logger.info("Cleanup complete.")

    def run_in_bg(self) -> asyncio.Task:
        """
        Schedule pipeline execution on the current event loop.

        Use this inside FastAPI/background contexts where a loop already exists.
        """
        return asyncio.create_task(self.run())


# --------------------------------------------------------------------------------------------
# CLI entry point (simple + correct)
# --------------------------------------------------------------------------------------------
if __name__ == "__main__":
    from backend.core.env_config import config

    pipeline = DataTransformationPipeline(
        database_url=config.DATABASE_URL,
        program=Program.FP,
        input_table_name=config.FP_KHIS_RAW_DATA_TABLE_NANE,
        output_table_name=config.FP_NATIONAL_SUMMARY_TABLE_NAME,
    )

    # In CLI, run the pipeline directly (no create_task needed)
    asyncio.run(pipeline.run())
