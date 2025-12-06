from fastapi import FastAPI
from routes import metadata_router
app = FastAPI(
    title="DHIS2 Data Extractor",
    description="API for extracting data from DHIS2",
)

app.include_router(metadata_router())

