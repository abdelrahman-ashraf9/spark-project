import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark_job import clean_data

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .appName("PySpark-CI-Testing") \
        .master("local[1]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_clean_data(spark):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])

    data = [
        ("Alice", 100.0),
        ("Bob", 0.0),
        ("Charlie", -10.0),
        (None, 50.0),
        ("David", 200.0)
    ]

    df = spark.createDataFrame(data, schema)
    result_df = clean_data(df)
    results = result_df.collect()

    assert len(results) == 2

    records = {row["name"]: row["amount_with_tax"] for row in results}

    assert "Alice" in records
    assert "David" in records
    assert "Bob" not in records
    assert "Charlie" not in records
    assert None not in records

    assert pytest.approx(records["Alice"], 0.01) == 120.0
    assert pytest.approx(records["David"], 0.01) == 240.0