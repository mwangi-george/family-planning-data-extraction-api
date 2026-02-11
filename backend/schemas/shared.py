from typing import Any
from pydantic import BaseModel, Field


class APIResponse(BaseModel):
    """
    Standard schema for API responses

    Attributes:
        success (bool): Whether the API response was successful
        message (str): The description of the API response
        data (Any): The response data
        trace_id (str): The trace id of the API request
    """

    success: bool = Field(..., description="Indicates if the request was successful")
    message: str | None = Field(None, description="Human-readable success/error message")
    data: Any | None = Field(None, description="Main response payload")
    trace_id: str | None = Field(description="Trace ID for debugging and correlation")


class KhisCredentials(BaseModel):
    """Simple container for KHIS/DHIS2 credentials."""
    base_url: str
    username: str
    password: str