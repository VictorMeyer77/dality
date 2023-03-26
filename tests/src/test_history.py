import shutil
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from datetime import datetime
from src.history import History, HISTORY_SCHEMA

spark = SparkSession.builder.getOrCreate()


class TestQualities(unittest.TestCase):

    def test_save_qualities_raises_error_when_schema_is_invalid(self):
        history = History(spark, "tests/resources/output/history")
        with self.assertRaises(ValueError):
            history.save_qualities(spark.createDataFrame([], schema=StructType([])))

    def test_history_should_reads_and_writes_qualities(self):
        qualities = [
            ("uniqueness", "UniquenessPerson", 2, 2, datetime(2023, 1, 1), 3, "prod", "postgresql", "website", "user"),
            ("uniqueness", "UniquenessId", 3, 2, datetime(2023, 1, 1), 3, "prod", "postgresql", "website", "user"),
            ("completeness", "CompletenessNames", 3, 3, datetime(2023, 1, 1), 3, "prod", "postgresql", "website", "user"),
            ("completeness", "CompletenessAge", 1, 1, datetime(2023, 1, 1), 3, "prod", "postgresql", "website", "user")
        ]
        qualities_df_one = spark.createDataFrame(data=qualities[:2], schema=HISTORY_SCHEMA)
        qualities_df_two = spark.createDataFrame(data=qualities[2:4], schema=HISTORY_SCHEMA)
        history = History(spark, "tests/resources/output/history")
        history.save_qualities(qualities_df_one)
        history.save_qualities(qualities_df_two)
        result = history.get_qualities()
        target_result = spark.createDataFrame(data=qualities, schema=HISTORY_SCHEMA)
        assert set(result.collect()) == set(target_result.collect())
        assert result.schema == HISTORY_SCHEMA
        shutil.rmtree("tests/resources/output/history/qualities.parquet")
