# logging_setups.py

import logging
from logging.handlers import RotatingFileHandler

def basic_logging_setup(log_file: str = "app.log", level: str = "INFO"):
    fmt = "%(asctime)s %(name)s %(levelname)s %(message)s"
    logging.basicConfig(level=level, format=fmt)

    # optional file handler
    handler = RotatingFileHandler(log_file, maxBytes=5e6, backupCount=3)
    handler.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(handler)