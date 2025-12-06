from fastapi import APIRouter, status
from asyncer import asyncify

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
    async def download_metadata() -> str:
        """
        Endpoint to trigger metadata extraction and storage.

        Uses `asyncify` to run the synchronous `extract_and_store_dhis2_metadata`
        function asynchronously without blocking the event loop.

        Returns
        -------
        str
            Message indicating success or failure of the operation.
        """
        # Execute the sync metadata extraction asynchronously
        message = await asyncify(extract_and_store_dhis2_metadata)()
        return message

    return router
