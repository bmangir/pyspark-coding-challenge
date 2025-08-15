from pyspark.sql import functions as F, Window
import datetime

from src.config import ACTION_CLICK, ACTION_ATC, ACTION_ORD, ACTION_NONE


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
    
    def __explode_impressions__(self, df):
        """
        Explode impressions array so each row represents a single impression item
        """
        exploded_df = df.select(
            "dt",
            "customer_id", 
            "ranking_id",
            F.explode("impressions").alias("impression_item")
        ).select(
            "dt",
            "customer_id",
            "ranking_id", 
            F.col("impression_item.item_id").alias("item_id"),
            F.col("impression_item.is_order").alias("is_order")
        )
        
        return exploded_df

    def __prepare_clicks__(self, df):
        renamed_df = df.withColumn("action_type", F.lit(ACTION_CLICK)) \
            .withColumnRenamed("item_id", "action_item_id") \
            .withColumnRenamed("click_time", "action_time") \
            .select("customer_id", "action_item_id", "action_time", "action_type")

        # Filter for the last year from reference date
        filtered_df = renamed_df.filter(F.col("action_time") >= self.reference_date)

        return filtered_df

    def __prepare_add_to_carts__(self, df):
        renamed_df = df.withColumn("action_type", F.lit(ACTION_ATC)) \
            .withColumnRenamed("config_id", "action_item_id") \
            .withColumnRenamed("occurred_at", "action_time") \
            .select("customer_id", "action_item_id", "action_time", "action_type")

        # Filter for the last year from reference date
        filtered_df = renamed_df.filter(F.col("action_time") >= self.reference_date)

        return filtered_df

    def __prepare_previous_orders__(self, df):
        renamed_df = df.withColumn("action_type", F.lit(ACTION_ORD)) \
            .withColumnRenamed("config_id", "action_item_id") \
            .withColumnRenamed("order_date", "action_time") \
            .select("customer_id", "action_item_id", "action_time", "action_type")

        # Filter for the last year from reference date
        filtered_df = renamed_df.filter(F.col("action_time") >= self.reference_date)

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

    def join_impressions_with_actions(self, actions_df, impressions_df, MAX_ACTIONS=1000):
        exploded_impressions = self.__explode_impressions__(impressions_df)
        
        # Convert dates to proper format for comparison
        exploded_impressions = exploded_impressions.withColumn("impression_date", F.to_date("dt"))
        actions_df = actions_df.withColumn("action_date", F.to_date("action_time"))
        
        # For each impression, actions are needed which are occurred before that impression date
        # action_date < impression_date
        joined_df = exploded_impressions.alias("imp").join(
            actions_df.alias("act"),
            (F.col("imp.customer_id") == F.col("act.customer_id")) &
            (F.col("act.action_date") < F.col("imp.impression_date")),
            "left"
        )
        
        # For each impression, rank actions by recency and take top 1000 (MAX_ACTIONS)
        window_spec = Window.partitionBy("imp.customer_id", "imp.dt", "imp.ranking_id", "imp.item_id") \
                           .orderBy(F.col("act.action_time").desc())
        
        ranked_actions = joined_df.withColumn("action_rank", F.row_number().over(window_spec)) \
                                .filter((F.col("action_rank") <= MAX_ACTIONS) | F.col("action_rank").isNull())

        grouped_actions = ranked_actions.groupBy(
            "imp.dt", "imp.customer_id", "imp.ranking_id", "imp.item_id", "imp.is_order"
        ).agg(
            F.collect_list("act.action_item_id").alias("actions"),
            F.collect_list("act.action_type").alias("action_types")
        )

        def pad_array(col, pad_value):
            """ Pad arrays to exactly MAX_ACTIONS length """
            return F.when(
                F.size(col) < MAX_ACTIONS,
                F.concat(col, F.array_repeat(F.lit(pad_value), MAX_ACTIONS - F.size(col)))
            ).otherwise(F.slice(col, 1, MAX_ACTIONS))
        
        # Handle null arrays (customers with no actions)
        final_df = grouped_actions.withColumn(
            "actions", 
            F.when(F.col("actions").isNull(), 
                   F.array_repeat(F.lit(0), MAX_ACTIONS))
             .otherwise(pad_array(F.col("actions"), F.lit(0)))
        ).withColumn(
            "action_types",
            F.when(F.col("action_types").isNull(),
                   F.array_repeat(F.lit(ACTION_NONE), MAX_ACTIONS))
             .otherwise(pad_array(F.col("action_types"), F.lit(ACTION_NONE)))
        )
        
        # Rename columns to match the expected output
        final_df = final_df.select(
            F.col("dt"),
            F.col("customer_id"), 
            F.col("ranking_id"),
            F.col("item_id"),
            F.col("is_order"),
            F.col("actions"),
            F.col("action_types")
        )
        
        return final_df

    def save(self, df, path, format="parquet", mode="overwrite"):
        """
        Save DataFrame to specified path in given format
        :param df: DataFrame to save
        :param path: Path to save the DataFrame
        :param format: Format to save the DataFrame (default is parquet)
        :param mode: Save mode (default is overwrite)
        """
        df.write.mode(mode).format(format).save(path)
