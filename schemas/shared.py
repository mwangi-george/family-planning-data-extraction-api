from typing import Any
from pydantic import BaseModel, Field


class APIResponse(BaseModel):

    success: bool = Field(..., description="Indicates if the request was successful")
    message: str | None = Field(None, description="Human-readable success/error message")
    data: Any | None = Field(None, description="Main response payload")
    trace_id: str | None = Field(description="Trace ID for debugging and correlation")
