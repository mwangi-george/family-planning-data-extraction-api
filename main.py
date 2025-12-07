from fastapi import FastAPI

from core.logging_config import setup_logging
from core.middlewares import TraceIDMiddleware
from routes import metadata_router


setup_logging()
app = FastAPI(
    title="DHIS2 Data Extractor",
    description="API for extracting data from DHIS2",
)

app.add_middleware(TraceIDMiddleware)
app.include_router(metadata_router())


