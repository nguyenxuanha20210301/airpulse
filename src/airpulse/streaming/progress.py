"""one log line per micro batch that carried data, the raw numbers behind the stream benchmark.

spark keeps the progress of every query in memory and shows it in its ui, a log line survives the
driver and can be grepped after a run. empty batches are skipped to keep the log readable.
"""

from __future__ import annotations

import logging

from pyspark.sql.streaming import StreamingQueryListener


def progress_line(progress) -> str | None:
    """the log line of one progress report, none when the batch read nothing."""
    if not progress.numInputRows:
        return None
    duration = progress.durationMs.get("triggerExecution", 0)
    return (
        f"query {progress.name} batch {progress.batchId} rows {progress.numInputRows} "
        f"duration_ms {duration} processed_per_second {progress.processedRowsPerSecond:.1f} "
        f"input_per_second {progress.inputRowsPerSecond:.1f}"
    )


class ProgressLogger(StreamingQueryListener):
    def __init__(self, log: logging.Logger | logging.LoggerAdapter) -> None:
        self.log = log

    def onQueryStarted(self, event) -> None:  # noqa: N802
        self.log.info("query %s started, id %s", event.name, event.id)

    def onQueryProgress(self, event) -> None:  # noqa: N802
        line = progress_line(event.progress)
        if line:
            self.log.info(line)

    def onQueryIdle(self, event) -> None:  # noqa: N802
        return None

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        if event.exception:
            self.log.error("query %s stopped, %s", event.id, event.exception.splitlines()[0])
