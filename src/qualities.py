from pyspark.sql.functions import col, date_add, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ByteType, IntegerType

RESULT_SCHEMA = StructType([
    StructField("quality", StringType(), False),
    StructField("constraint_name", StringType(), False),
    StructField("criticality", ByteType(), False),
    StructField("invalid_rows_count", IntegerType(), False)
])


def uniqueness_quality(spark, table, constraints):
    results = []
    for constraint in constraints:
        count_invalid_rows = table.groupby(constraint["condition"]) \
            .count() \
            .filter(col("count") > 1) \
            .count()
        results.append(("uniqueness", constraint["name"], constraint["criticality"], count_invalid_rows))
    return spark.createDataFrame(data=results, schema=RESULT_SCHEMA)


def completeness_quality(spark, table, constraints):
    results = []
    for constraint in constraints:
        filters = []
        for column in constraint["columns"]:
            filter_buffer = f"({column} IS NULL "
            for null_values in (constraint["null_values"] if "null_values" in constraint.keys() else []):
                filter_buffer += f"OR {column} == \"{null_values}\" "
            filters.append(filter_buffer + ")")
        count_invalid_rows = table.filter(" OR ".join(filters)).count()
        results.append(("completeness", constraint["name"], constraint["criticality"], count_invalid_rows))
    return spark.createDataFrame(data=results, schema=RESULT_SCHEMA)


def accuracy_quality(spark, table, constraints):
    results = []
    for constraint in constraints:
        count_invalid_rows = table.filter(constraint["condition"]).count()
        results.append(("accuracy", constraint["name"], constraint["criticality"], count_invalid_rows))
    return spark.createDataFrame(data=results, schema=RESULT_SCHEMA)


def freshness_quality(spark, table, constraints):
    results = []
    for constraint in constraints:
        count_invalid_rows = table \
            .filter(date_add(constraint["date_column"], constraint["days_validity_period"]) < current_timestamp()) \
            .count()
        results.append(("freshness", constraint["name"], constraint["criticality"], count_invalid_rows))
    return spark.createDataFrame(data=results, schema=RESULT_SCHEMA)


def _integrity_quality(left_table, right_table, constraint):
    if len(constraint["left_keys"]) != len(constraint["right_keys"]):
        raise ValueError(f"keys len mismatch for {constraint['name']}: "
                         f"{len(constraint['left_keys'])} != {len(constraint['right_keys'])}")
    for i in range(0, len(constraint["left_keys"])):
        if constraint["left_keys"][i] not in left_table.columns:
            raise ValueError(f"missing column \"{constraint['left_keys'][i]}\" in left table")
        else:
            left_table = left_table.withColumnRenamed(constraint["left_keys"][i], f"key_{i}_dality")
        if constraint["right_keys"][i] not in right_table.columns:
            raise ValueError(f"missing column \"{constraint['right_keys'][i]}\" in right table")
        else:
            right_table = right_table.withColumnRenamed(constraint["right_keys"][i], f"key_{i}_dality")
    count_invalid_rows = left_table \
        .join(right_table, [f"key_{i}_dality" for i in range(0, len(constraint["left_keys"]))], "left_anti") \
        .count()
    return "integrity", constraint["name"], constraint["criticality"], count_invalid_rows


def integrity_quality(spark, left_table, right_tables, constraints):
    if len(right_tables) != len(constraints):
        raise ValueError(f"integrity constraint doesn't match to right_tables array:"
                         f" {len(constraints)} != {len(right_tables)}")
    results = []
    for i in range(0, len(right_tables)):
        results.append(_integrity_quality(left_table, right_tables[i], constraints[i]))
    return spark.createDataFrame(data=results, schema=RESULT_SCHEMA)


def compute_qualities(spark, table, right_tables, config):
    result = spark.createDataFrame(data=[], schema=RESULT_SCHEMA)
    if "uniqueness" in config.keys():
        result = result.union(uniqueness_quality(spark, table, config["uniqueness"]))
    if "completeness" in config.keys():
        result = result.union(completeness_quality(spark, table, config["completeness"]))
    if "accuracy" in config.keys():
        result = result.union(accuracy_quality(spark, table, config["accuracy"]))
    if "freshness" in config.keys():
        result = result.union(freshness_quality(spark, table, config["freshness"]))
    if "integrity" in config.keys():
        result = result.union(integrity_quality(spark, table, right_tables, config["integrity"]))
    return result
