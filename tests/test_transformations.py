import unittest
from src.transformations import Transformer
from src.config import ACTION_CLICK, ACTION_ATC, ACTION_ORD, ACTION_NONE
from tests.sample_data import create_test_spark_session, create_test_dataframes


class TestTransformer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = create_test_spark_session()
        cls.transformer = Transformer("TestTransformer", n_actions=10, days=365)
        
    @classmethod 
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
        
    def setUp(self):
        """Set up test data for each test"""
        self.impressions_df, self.clicks_df, self.carts_df, self.orders_df = create_test_dataframes(self.spark)


if __name__ == '__main__':
    unittest.main()
