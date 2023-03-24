from pyspark.sql.functions import lit, current_timestamp


def add_dataframe_info(qualities, dataframe_info):
    return qualities.withColumn("env", lit(dataframe_info["env"])) \
        .withColumn("source", lit(dataframe_info["source"])) \
        .withColumn("schema", lit(dataframe_info["schema"])) \
        .withColumn("table", lit(dataframe_info["table"]))


def add_current_timestamp(qualities):
    return qualities.withColumn("dt_compute", current_timestamp())


def add_table_count(qualities, table):
    return qualities.withColumn("table_count", lit(table.count()))


def add_metadata(qualities, table, dataframe_info):
    qualities_with_timestamp = add_current_timestamp(qualities)
    qualities_with_count = add_table_count(qualities_with_timestamp, table)
    qualities_with_df_info = add_dataframe_info(qualities_with_count, dataframe_info)
    return qualities_with_df_info
