from pyspark.sql import functions as F, Window

from src.config import ACTION_CLICK, ACTION_ATC, ACTION_ORD


class Transformer:
    def __init__(self, name: str):
        self.name = name

    def transform(self, data):
        raise NotImplementedError("This method should be overridden by subclasses")

    def __repr__(self):
        return f"Transformer(name={self.name})"

    def prepare_impressions(self, df):
        return df

    def prepare_clicks(self, df):
        return df.withColumn("action_type", F.lit(ACTION_CLICK)) \
            .withColumnRenamed("item_id", "action_item_id") \
            .withColumnRenamed("click_time", "action_time")

    def prepare_add_to_carts(self, df):
        return df.withColumn("action_type", F.lit(ACTION_ATC)) \
            .withColumnRenamed("config_id", "action_item_id") \
            .withColumnRenamed("occurred_at", "action_time")

    def prepare_previous_orders(self, df):
        return df.withColumn("action_type", F.lit(ACTION_ORD)) \
            .withColumnRenamed("config_id", "action_item_id") \
            .withColumnRenamed("order_date", "action_time")

    def prepare_data(self, *args):
        """
        :param args: impressions, clicks, add_to_carts, previous_orders dfs respectively
        :return:
        """
        pass

    def tag(self):
        pass

    def build_action_history(self):
        pass

    def join_impressions_with_actions(self):
        pass
