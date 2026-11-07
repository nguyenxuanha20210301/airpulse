from types import SimpleNamespace

from airpulse.streaming.progress import progress_line


def report(rows):
    return SimpleNamespace(
        name="clean-air",
        batchId=7,
        numInputRows=rows,
        durationMs={"triggerExecution": 4210, "addBatch": 3900},
        processedRowsPerSecond=8211.43,
        inputRowsPerSecond=1200.0,
    )


def test_batches_with_data_become_one_parsable_line() -> None:
    line = progress_line(report(34567))
    tokens = line.split()
    fields = dict(zip(tokens[::2], tokens[1::2], strict=True))

    assert fields["query"] == "clean-air"
    assert fields["batch"] == "7"
    assert fields["rows"] == "34567"
    assert fields["duration_ms"] == "4210"
    assert fields["processed_per_second"] == "8211.4"


def test_empty_batches_are_skipped() -> None:
    assert progress_line(report(0)) is None
