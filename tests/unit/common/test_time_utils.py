from datetime import date, datetime, timezone

from airpulse.common.time_utils import (
    date_partition,
    date_range,
    floor_to_hour,
    month_chunks,
    parse_iso_utc,
    to_iso_utc,
    to_local,
)


def test_naive_iso_is_read_as_utc() -> None:
    parsed = parse_iso_utc("2026-11-06T17:00")

    assert parsed == datetime(2026, 11, 6, 17, tzinfo=timezone.utc)


def test_offsets_and_z_suffix_are_normalized() -> None:
    assert parse_iso_utc("2026-11-07T00:00:00+07:00") == parse_iso_utc("2026-11-06T17:00:00Z")
    assert to_iso_utc(parse_iso_utc("2026-11-07T00:00:00+07:00")) == "2026-11-06T17:00:00Z"


def test_floor_partition_and_local_time() -> None:
    value = datetime(2026, 11, 6, 17, 42, 9, tzinfo=timezone.utc)

    assert floor_to_hour(value).minute == 0
    assert date_partition(value) == "2026-11-06"
    assert to_local(value, "Asia/Ho_Chi_Minh").hour == 0


def test_date_range_and_month_chunks() -> None:
    assert len(date_range(date(2024, 2, 1), date(2024, 3, 1))) == 30
    assert date_range(date(2024, 3, 1), date(2024, 2, 1)) == []

    chunks = month_chunks(date(2024, 1, 15), date(2024, 3, 10))

    assert chunks == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 10)),
    ]
