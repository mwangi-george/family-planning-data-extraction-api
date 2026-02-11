from fastapi import APIRouter, status, Request
from asyncer import asyncify

from backend.core.env_config import config
from backend.schemas.shared import APIResponse
from backend.services.data_cleaning.fp_transformation_pipeline import FamilyPlanningDataTransformationPipeline


def create_data_cleaning_router() -> APIRouter:

    router = APIRouter(prefix=f"{config.PROJECT_VERSION}/data-cleaning", tags=["Data Cleaning"])

    @router.post(
        path="/run",
        summary="Run the data cleaning pipeline",
        response_model=APIResponse,
        status_code=status.HTTP_201_CREATED
    )
    async def data_cleaning_pipeline(request: Request) -> APIResponse:
        trace_id = request.state.trace_id

        # The data cleaning pipeline does not take more than 10 secs so it
        # can be run asynchronous using asyncify -> instant feedback instead of bg tasks

        # initialize transformation pipiline
        pipeline = FamilyPlanningDataTransformationPipeline(trace_id)
        response = await asyncify(pipeline.run)()
        return response

    return router