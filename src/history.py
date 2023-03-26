import os

from pyspark.sql.types import StructField, StringType, StructType, ByteType, TimestampType, IntegerType

HISTORY_SCHEMA = StructType([
    StructField("quality", StringType(), True),
    StructField("constraint_name", StringType(), True),
    StructField("criticality", ByteType(), True),
    StructField("invalid_rows_count", IntegerType(), True),
    StructField("dt_compute", TimestampType(), True),
    StructField("table_count", IntegerType(), True),
    StructField("env", StringType(), True),
    StructField("source", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
])


class History:

    def __init__(self, spark, output_directory):
        self.spark = spark
        self.output_directory = output_directory

    def save_qualities(self, qualities_df):
        if qualities_df.schema != HISTORY_SCHEMA:
            raise ValueError(f"invalid qualities schema, available: {HISTORY_SCHEMA}")
        qualities_df.write.mode("append").parquet(os.path.join(self.output_directory, "qualities.parquet"))

    def get_qualities(self):
        return self.spark.read.parquet(os.path.join(self.output_directory, "qualities.parquet"))
