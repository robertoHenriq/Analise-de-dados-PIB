# src/processing/logging_utils.py

import logging
import structlog

def setup_logging(level: str = "INFO"):
    """
    Configura logging estruturado usando structlog.
    """
    logging.basicConfig(
        format="%(message)s",
        stream=None,  # stdout
        level=getattr(logging, level.upper()),
    )

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.JSONRenderer(indent=2, sort_keys=True)
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()
