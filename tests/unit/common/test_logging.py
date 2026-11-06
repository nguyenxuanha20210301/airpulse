import logging

from airpulse.common.logging import get_logger, new_run_id


def test_run_ids_are_sortable_and_unique() -> None:
    first, second = new_run_id(), new_run_id()

    assert first != second
    assert first[:8].isdigit()
    assert first[8] == "T"


def test_logger_lines_carry_job_and_run_id(caplog) -> None:
    log = get_logger("unit.test", "run-123", "DEBUG")

    with caplog.at_level(logging.DEBUG):
        log.info("hello %d", 1)

    record = [r for r in caplog.records if r.name == "airpulse.unit.test"][-1]
    assert record.job == "unit.test"
    assert record.run_id == "run-123"
    assert record.getMessage() == "hello 1"
