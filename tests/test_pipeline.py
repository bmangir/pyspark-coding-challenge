import unittest
from unittest.mock import patch, MagicMock
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

    @patch('src.pipeline.read_json')
    @patch('src.pipeline.SparkSession.builder')
    def test_main_pipeline(self, mock_spark_builder, mock_read_json):
        """Test the main pipeline function"""
        # Mock Spark session
        mock_spark = MagicMock()
        mock_spark_builder.appName.return_value.getOrCreate.return_value = mock_spark

        mock_read_json.side_effect = [
            self.impressions_df,
            self.clicks_df,
            self.carts_df,
            self.orders_df
        ]

        with patch('builtins.print'):  # Suppress print statements in main
            result_df = main()

        # Verify read_json was called 4 times
        self.assertEqual(mock_read_json.call_count, 4)

        # Verify result DataFrame has correct structure
        self.assertIsNotNone(result_df)
        expected_columns = ["dt", "customer_id", "ranking_id", "item_id", "is_order", "actions", "action_types"]
        self.assertEqual(result_df.columns, expected_columns)

        # Should have 12 training samples (exploded impressions)
        self.assertEqual(result_df.count(), 12)

if __name__ == '__main__':
    unittest.main()
