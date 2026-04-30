import logging
import os


def setup_logging():
    """Configures Python's root logger with a standard format.

    Reads the ``LOG_LEVEL`` environment variable to set the logging level.
    If not set, defaults to ``INFO``. Applies a consistent timestamp and
    message format to all log output across the application.

    Environment Variables:
        LOG_LEVEL (str): Logging level — one of ``DEBUG``, ``INFO``, ``WARNING``,
            ``ERROR``, ``CRITICAL``. Case-insensitive. Defaults to ``"INFO"``.

    Format:
        ``YYYY-MM-DD HH:MM:SS [LEVEL] module.name: message``

    Example:
        Input:  LOG_LEVEL="DEBUG"
        Output: Logger configured to show all debug messages and above

        Input:  (no env var set)
        Output: Logger configured to INFO level (warnings, errors, critical)

    Raises:
        ValueError: If LOG_LEVEL contains an invalid logging level name.
            Falls back to INFO level in this case.
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )