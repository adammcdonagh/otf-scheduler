import logging
import os
from datetime import datetime

OTFS_LOG_FORMAT = (
    "%(asctime)s — %(name)s [%(threadName)s] — %(levelname)s — %(message)s"
)
LOG_DIRECTORY = (
    "logs"
    if os.environ.get("OTFS_LOG_DIRECTORY") is None
    else os.environ.get("OTFS_LOG_DIRECTORY")
)


def _define_log_file_name(run_id):
    global LOG_DIRECTORY
    LOG_DIRECTORY = (
        "logs"
        if os.environ.get("OTFS_LOG_DIRECTORY") is None
        else os.environ.get("OTFS_LOG_DIRECTORY")
    )

    # Set a custom handler to write to a specific file
    # Get the appropriate timestamp for the log file
    prefix = datetime.now().strftime("%Y%m%d-%H%M%S.%f")

    filename = f"{prefix}_{run_id}.log"

    return f"{LOG_DIRECTORY}/{filename}"


def init_logging(name, run_id=None):
    # Check if there's a root logger already
    if not logging.getLogger().hasHandlers():
        # Set the root logger
        logging.basicConfig(
            format=OTFS_LOG_FORMAT,
            level=logging.INFO,
            handlers=[
                logging.StreamHandler(),
            ],
        )

    # Set the log format
    # formatter = logging.Formatter(OTFS_LOG_FORMAT)

    # Create a unique logger object for this task
    if not run_id:
        logger = logging.getLogger(f"{name}")
    else:
        logger = logging.getLogger(f"{name}.{run_id}")

    # Set verbosity
    logger.setLevel(logging.getLogger().getEffectiveLevel())
    # Ensure the logger is at least at INFO level
    if logger.getEffectiveLevel() > logging.INFO:
        logger.setLevel(logging.INFO)

    # If the log level is set in the environment, then use that
    if os.environ.get("OTFS_LOG_LEVEL") is not None:
        logger.setLevel(os.environ.get("OTFS_LOG_LEVEL"))

    # If OTFS_NO_LOG is set, then don't create the handler
    if os.environ.get("OTFS_NO_LOG") is not None or not run_id:
        return logger

    # log_file_name = _define_log_file_name(run_id)

    logger.info("Logging initialised")

    return logger


def _mkdir(path):
    os.makedirs(path, exist_ok=True)


logger = init_logging(__name__)
