from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformations import Transformer
from src.utils import read_json
from src.config import click_schema, add_to_cart_schema, impression_schema, order_schema


def main(target_date=None, verbose=True):
    spark = SparkSession.builder \
        .appName("spark-job-0001") \
        .getOrCreate()

    impressions_df = read_json(spark, impression_schema, "data/impressions.json")
    clicks_df = read_json(spark, click_schema, "data/clicks.json")
    carts_df = read_json(spark, add_to_cart_schema, "data/add_to_carts.json")
    orders_df = read_json(spark, order_schema, "data/previous_orders.json")

    if target_date:
        impressions_df = impressions_df.filter(F.col("dt") == target_date)

    transformer = Transformer("ExampleTransformer")
    (impressions_df, clicks_df, carts_df, orders_df) = transformer.prepare_data(impressions_df, clicks_df, carts_df, orders_df)

    actions_df = transformer.build_action_history(clicks_df, carts_df, orders_df)

    final_df = transformer.join_impressions_with_actions(actions_df, impressions_df)

    if verbose:
        print("Final DataFrame Schema:")
        final_df.printSchema()
        print("Final DataFrame Sample:")
        final_df.show(5)
    
    return final_df


if __name__ == "__main__":
    main()