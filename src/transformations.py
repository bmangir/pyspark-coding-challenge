from pyspark.sql import functions as F, Window
import datetime

from src.config import ACTION_CLICK, ACTION_ATC, ACTION_ORD


class Transformer:
    def __init__(self, name: str, n_actions=1000, days=365):
        self.name = name
        self.n_actions = n_actions
        self.days = days
        self.reference_date = None

    def transform(self, data):
        raise NotImplementedError("This method should be overridden by subclasses")

    def __repr__(self):
        return f"Transformer(name={self.name})"

    def __prepare_impressions__(self, df):
        impression_dates = df.select(F.max("dt").alias("max_dt")).collect()[0]["max_dt"]

        self.reference_date = F.date_sub(F.lit(impression_dates), self.days)
        return df

    def __prepare_clicks__(self, df):
        renamed_df = df.withColumn("action_type", F.lit(ACTION_CLICK)) \
            .withColumnRenamed("item_id", "action_item_id") \
            .withColumnRenamed("click_time", "action_time") \
            .select("customer_id", "action_item_id", "action_time", "action_type")

        filtered_df = renamed_df.filter(F.col("action_time") >= self.reference_date)

        return filtered_df

    def __prepare_add_to_carts__(self, df):
        renamed_df = df.withColumn("action_type", F.lit(ACTION_ATC)) \
            .withColumnRenamed("config_id", "action_item_id") \
            .withColumnRenamed("occurred_at", "action_time") \
            .select("customer_id", "action_item_id", "action_time", "action_type")

        filtered_df = renamed_df.filter(F.col("action_time") >= self.reference_date)

        return filtered_df

    def __prepare_previous_orders__(self, df):
        renamed_df = df.withColumn("action_type", F.lit(ACTION_ORD)) \
            .withColumnRenamed("config_id", "action_item_id") \
            .withColumnRenamed("order_date", "action_time") \
            .select("customer_id", "action_item_id", "action_time", "action_type")

        filtered_df = renamed_df.filter(F.col("action_time") >= F.date_sub(F.current_date(), self.days)) \

        return filtered_df

    def prepare_data(self, *args):
        """
        :param args: impressions, clicks, add_to_carts, previous_orders dfs respectively
        :return:
        """
        return (self.__prepare_impressions__(args[0]),
                self.__prepare_clicks__(args[1]),
                self.__prepare_add_to_carts__(args[2]),
                self.__prepare_previous_orders__(args[3]))

    def tag(self):
        pass

    def build_action_history(self, clicks_df, carts_df, orders_df):
        return clicks_df.unionByName(carts_df).unionByName(orders_df)

    def join_impressions_with_actions(self):
        pass
