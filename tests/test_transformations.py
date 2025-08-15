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

    def test_explode_impressions(self):
        """Test that impressions are properly exploded into individual rows"""
        print("\n=== Testing Impression Explosion ===")

        original_count = self.impressions_df.count()
        print(f"Original impressions count: {original_count}")

        exploded_df = self.transformer.__explode_impressions__(self.impressions_df)
        exploded_count = exploded_df.count()
        print(f"Exploded impressions count: {exploded_count}")

        # Expected: 6 impression records with total of 12 items
        expected_count = 12
        print(f"Expected exploded count: {expected_count}")
        print(f"Count matches expected" if exploded_count == expected_count else f"Count mismatch!")
        self.assertEqual(exploded_count, expected_count)

        # Check schema
        expected_columns = ["dt", "customer_id", "ranking_id", "item_id", "is_order"]
        actual_columns = exploded_df.columns
        print(f"Expected columns: {expected_columns}")
        print(f"Actual columns: {actual_columns}")
        print(f"Schema matches" if actual_columns == expected_columns else f"Schema mismatch!")
        self.assertEqual(actual_columns, expected_columns)

        # Check specific values
        rows = exploded_df.collect()

        # First impression from customer 1 on 2024-12-15
        first_row = [r for r in rows if r.customer_id == 1 and r.item_id == 101][0]
        print(f"First row - dt: {first_row.dt}, ranking_id: {first_row.ranking_id}, is_order: {first_row.is_order}")

        expected_dt = "2024-12-15"
        expected_ranking = "rank_001"
        expected_is_order = False

        print(f"Date matches" if first_row.dt == expected_dt else f"Date mismatch: expected {expected_dt}, got {first_row.dt}")
        print(f"Ranking matches" if first_row.ranking_id == expected_ranking else f"Ranking mismatch: expected {expected_ranking}, got {first_row.ranking_id}")
        print(f"is_order matches" if first_row.is_order == expected_is_order else f"is_order mismatch: expected {expected_is_order}, got {first_row.is_order}")

        self.assertEqual(first_row.dt, expected_dt)
        self.assertEqual(first_row.ranking_id, expected_ranking)
        self.assertEqual(first_row.is_order, expected_is_order)


if __name__ == '__main__':
    unittest.main()
