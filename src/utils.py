from pyspark.sql import SparkSession, DataFrame


def read_json(spark: SparkSession, schema, path) -> DataFrame:
    df = spark.read.schema(schema).option("multiLine", "true") \
        .json(path)

    return df


def read_csv(spark: SparkSession, schema, path) -> DataFrame:
    df = spark.read.schema(schema).option("multiLine", "true") \
        .csv(path)

    return df


def read_parquet(spark: SparkSession, schema, path) -> DataFrame:
    df = spark.read.schema(schema).option("multiLine", "true") \
        .parquet(path)

    return df
