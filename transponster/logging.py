"""Setup logging.
This module mostly exists to allow control over logging set up by
other modules."""

import logging
import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
