def read_json(spark, schema, path):
    df = spark.read.schema(schema) \
        .json(path)

    return df


def read_csv(spark, schema, path):
    df = spark.read.schema(schema) \
        .csv(path)

    return df


def read_parquet(spark, schema, path):
    df = spark.read.schema(schema) \
        .parquet(path)

    return df
