from airpulse.common.config import DEFAULT_CONFIG_DIR, load_config
from airpulse.common.spark import session_conf


def test_replication_follows_the_profile() -> None:
    dev = session_conf(load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={}))
    demo = session_conf(load_config(DEFAULT_CONFIG_DIR / "demo.yaml", environ={}))

    assert dev["spark.hadoop.dfs.replication"] == "1"
    assert demo["spark.hadoop.dfs.replication"] == "2"
    assert dev["spark.sql.session.timeZone"] == "UTC"


def test_cluster_settings_only_when_configured() -> None:
    cluster = session_conf(load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={}))
    laptop = session_conf(load_config(DEFAULT_CONFIG_DIR / "base.yaml", environ={}))
    no_packages = session_conf(
        load_config(
            DEFAULT_CONFIG_DIR / "dev.yaml", environ={"AIRPULSE_SPARK__USE_PACKAGES": "false"}
        )
    )

    assert cluster["spark.sql.catalogImplementation"] == "hive"
    assert cluster["spark.sql.warehouse.dir"].startswith("hdfs://")
    assert cluster["spark.cassandra.connection.port"] == "9042"
    assert "hive.metastore.uris" not in laptop
    assert "spark.cassandra.connection.host" not in laptop
    assert "spark.jars.packages" in cluster
    assert "spark.jars.packages" not in no_packages
