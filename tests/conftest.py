import os
import sys

import pytest

_JDK17 = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
if os.name == "nt" and os.path.isdir(_JDK17):  # PySpark 3.5 needs Java 8/11/17 (not 23+)
    os.environ["JAVA_HOME"] = _JDK17
    os.environ["PATH"] = _JDK17 + r"in;" + os.environ["PATH"]
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]").appName("agripulse-tests")
        .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )  # fmt: skip
    yield s
    s.stop()


@pytest.fixture(scope="session")
def spark_delta(tmp_path_factory):
    """Local Spark with Delta Lake (jars fetched on first use) for MERGE/idempotency tests.

    Skipped on Windows unless HADOOP_HOME (winutils) is set: Delta writes need it there; Linux CI runs these tests.
    """
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip("Delta on Windows needs winutils (HADOOP_HOME); runs in Linux CI")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    wh = str(tmp_path_factory.mktemp("warehouse"))
    b = (
        SparkSession.builder.master("local[2]").appName("agripulse-delta-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", wh).config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )  # fmt: skip
    s = configure_spark_with_delta_pip(b).getOrCreate()
    s.sql("CREATE DATABASE IF NOT EXISTS ops")
    s.sql("CREATE DATABASE IF NOT EXISTS bronze")
    yield s
    s.stop()
