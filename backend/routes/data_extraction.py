from datetime import date, timedelta

from fastapi import APIRouter, status, Request, BackgroundTasks
from loguru import logger

from backend.schemas.shared import APIResponse
from backend.services.data_extraction.download_metadata import extract_and_store_dhis2_metadata_in_bg
from backend.services.data_extraction.download_historical_data import extract_and_store_historical_data_in_bg

def create_data_extraction_router() -> APIRouter:
    """
    Creates a FastAPI router for DHIS2 data_extraction operations.
    """

    router = APIRouter(prefix="/data-extraction", tags=["Data Extraction"])

    @router.post(
        path="/metadata",
        summary="Download and store DHIS2 metadata",
        status_code=status.HTTP_202_ACCEPTED,
        description=(
            "Downloads DHIS2 metadata (indicators, data elements, and organisation units) "
            "and stores it in the configured database."
        )
    )
    async def download_metadata(request: Request, bg_tasks: BackgroundTasks) -> APIResponse:
        trace_id = request.state.trace_id
        logger.info(f"[{trace_id}] Downloading metadata from DHIS2")

        message = await extract_and_store_dhis2_metadata_in_bg(trace_id, bg_tasks)
        return message

    @router.post(
        path="/historical-data",
        summary="Download and store historical data from DHIS2",
        description=(
            "Downloads historical consumption and service data from DHIS2 "
            "and stores it in the configured database."
        ),
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def download_historical_data(
            request: Request,
            bg_tasks: BackgroundTasks,
            start_date: date = date.today() - timedelta(days=90),
            end_date: date = date.today(),
    ) -> APIResponse:
        """
        Download historical data from DHIS2
        """
        trace_id = request.state.trace_id
        message = await extract_and_store_historical_data_in_bg(
            trace_id = trace_id,
            start_date=start_date,
            end_date=end_date,
            bg_tasks=bg_tasks
        )
        return message

    return router
