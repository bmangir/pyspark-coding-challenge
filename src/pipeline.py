from pyspark.sql import SparkSession

from src.transformations import Transformer
from src.utils import read_json
from src.config import click_schema, add_to_cart_schema, impression_schema, order_schema


def main():
    spark = SparkSession.builder \
        .appName("spark-job-0001") \
        .getOrCreate()

    impressions_df = read_json(spark, impression_schema, "data/impressions.json")
    clicks_df = read_json(spark, click_schema, "data/clicks.json")
    carts_df = read_json(spark, add_to_cart_schema, "data/add_to_carts.json")
    orders_df = read_json(spark, order_schema, "data/previous_orders.json")

    transformer = Transformer("ExampleTransformer")
    impressions_df, clicks_df, carts_df, orders_df = transformer.prepare_data(impressions_df, clicks_df, carts_df, orders_df)


if __name__ == "__main__":
    main()