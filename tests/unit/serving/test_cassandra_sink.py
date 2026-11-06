from airpulse.serving import cassandra_sink
from airpulse.serving.cassandra_sink import cassandra_options, foreach_batch_to_cassandra


def test_options_follow_the_profile(config) -> None:
    options = cassandra_options(config, "rt_alerts")

    assert options["keyspace"] == "airpulse"
    assert options["table"] == "rt_alerts"
    assert options["spark.cassandra.output.consistency.level"] == "ONE"


def test_foreach_batch_applies_the_transform(monkeypatch, config) -> None:
    written = []
    monkeypatch.setattr(
        cassandra_sink, "write_batch", lambda df, cfg, table: written.append((df, table))
    )
    sink = foreach_batch_to_cassandra(config, "rt_air_latest", transform=lambda df: df + 1)

    sink(41, 0)

    assert written == [(42, "rt_air_latest")]
