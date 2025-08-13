from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, BooleanType


# Define action types as constants
ACTION_CLICK = 1
ACTION_ATC = 2
ACTION_ORD = 3

# Define the schema for each dataset
click_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("customer_id", IntegerType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("click_time", TimestampType(), True)
])

add_to_cart_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("customer_id", IntegerType(), False),
    StructField("config_id", IntegerType(), False),
    StructField("simple_id", IntegerType(), False),
    StructField("occurred_at", TimestampType(), True)
])

impression_item_schema = StructType([
    StructField("item_id", IntegerType(), False),
    StructField("is_order", BooleanType(), False)
])

impression_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("ranking_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("impressions", ArrayType(impression_item_schema), True)
])

order_schema = StructType([
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), False),
    StructField("config_id", IntegerType(), False),
    StructField("simple_id", IntegerType(), False),
    StructField("occurred_at", TimestampType(), True)
])


