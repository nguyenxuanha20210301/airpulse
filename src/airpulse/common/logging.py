"""loggers that carry the job name and run_id on every line.

each job run gets one run_id. it goes into the log, into every output row and into the model
registry, so a row in a dashboard can be traced back to the exact run that produced it.
"""

from __future__ import annotations

import logging
import sys
import uuid
from datetime import datetime, timezone

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s job=%(job)s run_id=%(run_id)s %(message)s"
_configured = False


class _StderrHandler(logging.StreamHandler):
    """write to whatever sys.stderr is at emit time, test runners swap it while capturing."""

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value) -> None:
        pass


class _DefaultFields(logging.Filter):
    """records from third party loggers have no job or run_id, give them a placeholder."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "job"):
            record.job = "-"
        if not hasattr(record, "run_id"):
            record.run_id = "-"
        return True


def new_run_id() -> str:
    """utc timestamp plus a short random suffix, sortable and unique enough for one cluster."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:6]}"


def configure_logging(level: str = "INFO") -> None:
    """install one stream handler on the root logger, later calls only adjust the level."""
    global _configured
    root = logging.getLogger()
    if not _configured:
        handler = _StderrHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        handler.addFilter(_DefaultFields())
        root.addHandler(handler)
        _configured = True
    root.setLevel(level.upper())


def get_logger(job: str, run_id: str, level: str = "INFO") -> logging.LoggerAdapter:
    """return an adapter that injects job and run_id into every record."""
    configure_logging(level)
    return logging.LoggerAdapter(
        logging.getLogger(f"airpulse.{job}"), {"job": job, "run_id": run_id}
    )
