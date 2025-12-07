from loguru import logger
import sys
from pathlib import Path
from core.context import trace_id_ctx

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


def trace_filter(record):
    """
    Inject the current trace ID into every log record.

    Loguru allows adding dynamic fields via the record["extra"] dictionary.
    """
    record["extra"]["trace_id"] = trace_id_ctx.get()
    return True


def setup_logging():
    """
    Configure Loguru to write logs to both console and a rotating log file.

    - logs/app.log will automatically rotate when size > 10 MB
    - Keeps 10 backup files
    - Older logs are compressed
    """
    logger.remove()

    # Console logs
    logger.add(
        sys.stdout,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<cyan>{extra[trace_id]}</cyan> | "
            "<level>{level}</level> | "
            "{message}"
        ),
        filter=trace_filter,
    )

    # File logs
    logger.add(
        "logs/app.log",
        rotation="10 MB",
        retention=10,
        compression="zip",
        level="DEBUG",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{extra[trace_id]} | "
            "{level} | "
            "{name}:{function}:{line} - "
            "{message}"
        ),
        filter=trace_filter,
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )
