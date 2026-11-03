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
