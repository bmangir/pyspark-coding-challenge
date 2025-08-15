from pyspark.sql import SparkSession
from datetime import datetime, date


def create_test_spark_session():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test-spark-job") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


def get_test_impressions_data():
    """Sample impressions data for testing"""
    return [
        {
            "dt": "2024-12-15",
            "ranking_id": "rank_001",
            "customer_id": 1,
            "impressions": [
                {"item_id": 101, "is_order": False},
                {"item_id": 102, "is_order": True}
            ]
        },
        {
            "dt": "2024-12-16",
            "ranking_id": "rank_002", 
            "customer_id": 1,
            "impressions": [
                {"item_id": 103, "is_order": False},
                {"item_id": 104, "is_order": False}
            ]
        },
        {
            "dt": "2024-12-16",
            "ranking_id": "rank_003",
            "customer_id": 2,
            "impressions": [
                {"item_id": 105, "is_order": False},
                {"item_id": 106, "is_order": True}
            ]
        },
        {
            "dt": "2024-12-17",
            "ranking_id": "rank_004",
            "customer_id": 1,
            "impressions": [
                {"item_id": 107, "is_order": False}
            ]
        },
        {
            "dt": "2024-12-17",
            "ranking_id": "rank_005",
            "customer_id": 3,
            "impressions": [
                {"item_id": 108, "is_order": False},
                {"item_id": 109, "is_order": True},
                {"item_id": 110, "is_order": False}
            ]
        },
        {
            "dt": "2024-12-18",
            "ranking_id": "rank_006",
            "customer_id": 2,
            "impressions": [
                {"item_id": 111, "is_order": False},
                {"item_id": 112, "is_order": False}
            ]
        }
    ]


def get_test_clicks_data():
    """Sample clicks data for testing"""
    return [
        # Old data (should be filtered out due to 1 year limit)
        {
            "dt": "2023-12-10",
            "customer_id": 1,
            "item_id": 201,
            "click_time": datetime(2023, 12, 10, 10, 30, 0)
        },
        {
            "dt": "2023-11-15", 
            "customer_id": 2,
            "item_id": 202,
            "click_time": datetime(2023, 11, 15, 11, 15, 0)
        },
        # Recent data (should be included)
        {
            "dt": "2024-12-10",
            "customer_id": 1,
            "item_id": 203,
            "click_time": datetime(2024, 12, 10, 9, 0, 0)
        },
        {
            "dt": "2024-12-11",
            "customer_id": 1,
            "item_id": 204,
            "click_time": datetime(2024, 12, 11, 14, 20, 0)
        },
        {
            "dt": "2024-12-12",
            "customer_id": 1,
            "item_id": 205,
            "click_time": datetime(2024, 12, 12, 16, 45, 0)
        },
        {
            "dt": "2024-12-14",
            "customer_id": 2,
            "item_id": 206,
            "click_time": datetime(2024, 12, 14, 8, 30, 0)
        },
        {
            "dt": "2024-12-15",
            "customer_id": 2,
            "item_id": 207,
            "click_time": datetime(2024, 12, 15, 12, 15, 0)
        },
        {
            "dt": "2024-12-16",
            "customer_id": 3,
            "item_id": 208,
            "click_time": datetime(2024, 12, 16, 15, 0, 0)
        },
        {
            "dt": "2024-12-17",
            "customer_id": 1,
            "item_id": 209,
            "click_time": datetime(2024, 12, 17, 11, 30, 0)
        }
    ]


def get_test_add_to_carts_data():
    """Sample add to carts data for testing"""
    return [
        # Old data (should be filtered out)
        {
            "dt": "2023-10-20",
            "customer_id": 1,
            "config_id": 301,
            "simple_id": 3001,
            "occurred_at": datetime(2023, 10, 20, 12, 30, 0)
        },
        # Recent data (should be included)
        {
            "dt": "2024-12-12",
            "customer_id": 1,
            "config_id": 302,
            "simple_id": 3002,
            "occurred_at": datetime(2024, 12, 12, 10, 15, 0)
        },
        {
            "dt": "2024-12-13",
            "customer_id": 1,
            "config_id": 303,
            "simple_id": 3003,
            "occurred_at": datetime(2024, 12, 13, 16, 45, 0)
        },
        {
            "dt": "2024-12-14",
            "customer_id": 2,
            "config_id": 304,
            "simple_id": 3004,
            "occurred_at": datetime(2024, 12, 14, 14, 20, 0)
        },
        {
            "dt": "2024-12-16",
            "customer_id": 2,
            "config_id": 305,
            "simple_id": 3005,
            "occurred_at": datetime(2024, 12, 16, 9, 30, 0)
        },
        {
            "dt": "2024-12-17",
            "customer_id": 3,
            "config_id": 306,
            "simple_id": 3006,
            "occurred_at": datetime(2024, 12, 17, 13, 0, 0)
        }
    ]


def get_test_orders_data():
    """Sample orders data for testing"""
    return [
        # Old data (should be filtered out)
        {
            "order_date": date(2023, 9, 10),
            "customer_id": 2,
            "config_id": 401,
            "simple_id": 4001,
            "occurred_at": datetime(2023, 9, 10, 18, 0, 0)
        },
        # Recent data (should be included)
        {
            "order_date": date(2024, 12, 10),
            "customer_id": 1,
            "config_id": 402,
            "simple_id": 4002,
            "occurred_at": datetime(2024, 12, 10, 20, 30, 0)
        },
        {
            "order_date": date(2024, 12, 13),
            "customer_id": 1,
            "config_id": 403,
            "simple_id": 4003,
            "occurred_at": datetime(2024, 12, 13, 19, 15, 0)
        },
        {
            "order_date": date(2024, 12, 14),
            "customer_id": 2,
            "config_id": 404,
            "simple_id": 4004,
            "occurred_at": datetime(2024, 12, 14, 17, 45, 0)
        },
        {
            "order_date": date(2024, 12, 16),
            "customer_id": 3,
            "config_id": 405,
            "simple_id": 4005,
            "occurred_at": datetime(2024, 12, 16, 21, 30, 0)
        }
    ]


def create_test_dataframes(spark):
    """Create test DataFrames from sample data"""
    from src.config import impression_schema, click_schema, add_to_cart_schema, order_schema
    
    impressions_df = spark.createDataFrame(get_test_impressions_data(), impression_schema)
    clicks_df = spark.createDataFrame(get_test_clicks_data(), click_schema)
    carts_df = spark.createDataFrame(get_test_add_to_carts_data(), add_to_cart_schema)
    orders_df = spark.createDataFrame(get_test_orders_data(), order_schema)
    
    return impressions_df, clicks_df, carts_df, orders_df
