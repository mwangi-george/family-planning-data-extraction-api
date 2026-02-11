
import os
from dotenv import load_dotenv

load_dotenv()

class EnvConfig:
    """
    Gets environment variables from env file
    """

    PROJECT_VERSION = os.getenv("PROJECT_VERSION")

    # DHIS2 variables
    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")

    # Database connection string
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Output table names
    FP_KHIS_RAW_DATA_TABLE_NANE = os.getenv("FP_KHIS_RAW_DATA_TABLE_NANE")
    FP_NATIONAL_SUMMARY_TABLE_NAME = os.getenv("FP_NATIONAL_SUMMARY_TABLE_NAME")

    MNCH_KHIS_RAW_DATA_TABLE_NANE =os.getenv("MNCH_KHIS_RAW_DATA_TABLE_NANE")
    MNCH_NATIONAL_SUMMARY_TABLE_NAME = os.getenv("MNCH_NATIONAL_SUMMARY_TABLE_NAME")



config = EnvConfig()