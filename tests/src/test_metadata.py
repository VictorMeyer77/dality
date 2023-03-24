import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, IntegerType

from src.qualities import RESULT_SCHEMA
from src.metadata import *

spark = SparkSession.builder.getOrCreate()


class TestMetadata(unittest.TestCase):

    def test_add_dataframe_info_returns_dataframe_with_table_info(self):
        dataset = [("Xavier", "Dupont", 34),
                   ("Mathilde", "Martin", 49),
                   ("Matthieu", "Timbert", 15)]
        input_df = spark.createDataFrame(data=dataset, schema=["first_name", "last_name", "age"])
        config = {
            "env": "prod",
            "source": "postgresql",
            "schema": "website",
            "table": "user"
        }
        result = add_dataframe_info(input_df, config)
        target_dataset = [("Xavier", "Dupont", 34, "prod", "postgresql", "website", "user"),
                          ("Mathilde", "Martin", 49, "prod", "postgresql", "website", "user"),
                          ("Matthieu", "Timbert", 15, "prod", "postgresql", "website", "user")]
        target_result = spark.createDataFrame(data=target_dataset,
                                              schema=["first_name", "last_name", "age", "env",
                                                      "source", "schema", "table"])
        assert result.columns == target_result.columns and result.collect() == target_result.collect()

    def test_add_current_timestamp_returns_dataframe_with_current_date(self):
        dataset = [("Xavier", "Dupont", 34),
                   ("Mathilde", "Martin", 49),
                   ("Matthieu", "Timbert", 15)]
        input_df = spark.createDataFrame(data=dataset, schema=["first_name", "last_name", "age"])
        result = add_current_timestamp(input_df)
        target_schema = StructType(
            [
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('age', LongType(), True),
                StructField('dt_compute', TimestampType(), False)
            ]
        )
        assert result.schema == target_schema

    def test_add_table_count_returns_dataframe_with_table_count(self):
        dataset = [("Xavier", "Dupont", 34),
                   ("Mathilde", "Martin", 49),
                   ("Matthieu", "Timbert", 15)]
        input_df = spark.createDataFrame(data=dataset, schema=["first_name", "last_name", "age"])
        result = add_table_count(input_df, input_df)
        target_dataset = [("Xavier", "Dupont", 34, 3),
                          ("Mathilde", "Martin", 49, 3),
                          ("Matthieu", "Timbert", 15, 3)]
        target_schema = StructType(
            [
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('age', LongType(), True),
                StructField('table_count', IntegerType(), False)
            ]
        )
        target_result = spark.createDataFrame(data=target_dataset, schema=target_schema)
        assert result.schema == target_result.schema and result.collect() == target_result.collect()

    def test_add_metadata_returns_qualities_with_metadata(self):
        input_df = spark.createDataFrame(data=[("Xavier", "Dupont", 34),
                                               ("Mathilde", "Martin", 49),
                                               ("Matthieu", "Timbert", 15)], schema=["first_name", "last_name", "age"])
        qualities_df = spark.createDataFrame(
            data=[("uniqueness", "UniquenessPerson", 2, 2),
                  ("uniqueness", "UniquenessId", 3, 2),
                  ("completeness", "CompletenessNames", 3, 3),
                  ("completeness", "CompletenessAge", 1, 1)],
            schema=RESULT_SCHEMA
        )
        config = {
            "env": "prod",
            "source": "postgresql",
            "schema": "website",
            "table": "user"
        }
        result = add_metadata(qualities_df, input_df, config)
        target_result = spark.createDataFrame(
            data=[("uniqueness", "UniquenessPerson", 2, 2, 3, "prod", "postgresql", "website", "user"),
                  ("uniqueness", "UniquenessId", 3, 2, 3, "prod", "postgresql", "website", "user"),
                  ("completeness", "CompletenessNames", 3, 3, 3, "prod", "postgresql", "website", "user"),
                  ("completeness", "CompletenessAge", 1, 1, 3, "prod", "postgresql", "website", "user")],
            schema=["quality", "constraint_name", "criticality",
                    "invalid_rows_count", "env", "source", "schema", "table"]
        )
        assert target_result.collect() == result.drop("dt_compute").collect()
        assert "dt_compute" in result.columns
