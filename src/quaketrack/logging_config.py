"""UTC console logging without credential-bearing configuration dumps."""
import logging
import time

def configure_logging(level="INFO"):
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s: %(message)s")
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=[handler], force=True)
