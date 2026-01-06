from fastapi import FastAPI

from backend.core.logging_config import setup_logging
from backend.core.middlewares import TraceIDMiddleware
from backend.routes import metadata_router, data_cleaning_router


setup_logging()
app = FastAPI(
    title="DHIS2 Data Extractor",
    description="API for extracting data from DHIS2",
)

app.add_middleware(TraceIDMiddleware)
app.include_router(metadata_router())
app.include_router(data_cleaning_router())


