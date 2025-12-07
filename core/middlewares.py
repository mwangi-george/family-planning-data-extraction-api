import shortuuid
from typing import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response

from core.context import trace_id_ctx

class TraceIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware that assigns a unique trace identifier (trace_id) to every incoming HTTP request.

    This trace_id serves as a correlation ID that allows developers, logs, and client applications
    to track a specific request across the entire request/response lifecycle. The middleware:

    1. Generates a unique trace ID using `shortuuid` at the start of each request.
    2. Stores the trace ID on `request.state.trace_id` for use in route handlers, services,
       logging, and downstream processes.
    3. Injects the same trace ID into the response headers as `X-Trace-ID`, allowing clients
       to reference it when reporting issues or correlating logs.
    """
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        trace_id = shortuuid.uuid()

        # set trace ID in both request.state and contextvar
        request.state.trace_id = trace_id
        trace_id_ctx.set(trace_id)

        response = await call_next(request)
        response.headers["X-Trace-ID"] = trace_id
        return response
