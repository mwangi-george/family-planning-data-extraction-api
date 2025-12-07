from fastapi import APIRouter, status, Request
from asyncer import asyncify
from loguru import logger

from schemas.shared import APIResponse
from services.metadata_scripts.download_metadata import extract_and_store_dhis2_metadata


def create_metadata_router() -> APIRouter:
    """
    Creates a FastAPI router for DHIS2 metadata operations.

    Endpoints:
    1. POST /metadata/download - Downloads metadata (indicators, data elements, organisation units)
       and stores it in the configured database.
    """
    router = APIRouter(prefix="/metadata", tags=["metadata"])

    @router.post(
        path="/download",
        summary="Download and store DHIS2 metadata",
        description=(
            "Downloads DHIS2 metadata (indicators, data elements, and organisation units) "
            "and stores it in the configured database."
        ),
        status_code=status.HTTP_201_CREATED,
    )
    async def download_metadata(request: Request) -> APIResponse:
        """
        Endpoint to trigger metadata extraction and storage.

        Uses `asyncify` to run the synchronous `extract_and_store_dhis2_metadata`
        function asynchronously without blocking the event loop.

        Returns
        -------
        str
            Message indicating success or failure of the operation.
        """
        trace_id = request.state.trace_id
        logger.info(f"[{trace_id}] Downloading metadata from DHIS2")

        # Execute the sync metadata extraction asynchronously
        message = await asyncify(extract_and_store_dhis2_metadata)(trace_id)
        return message

    return router
