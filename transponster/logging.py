import logging
import structlog
import progressbar

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
