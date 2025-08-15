import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.pipeline import main
from tests.sample_data import create_test_spark_session, create_test_dataframes


class TestPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = create_test_spark_session()
        
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
        
    def setUp(self):
        """Set up test data for each test"""
        self.impressions_df, self.clicks_df, self.carts_df, self.orders_df = create_test_dataframes(self.spark)


if __name__ == '__main__':
    unittest.main()
