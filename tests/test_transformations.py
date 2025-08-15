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

    def test_prepare_clicks(self):
        """Test clicks data preparation"""
        self.transformer.__prepare_impressions__(self.impressions_df)

        prepared_clicks = self.transformer.__prepare_clicks__(self.clicks_df)

        # Check schema
        expected_columns = ["customer_id", "action_item_id", "action_time", "action_type"]
        self.assertEqual(prepared_clicks.columns, expected_columns)

        # Check action type
        rows = prepared_clicks.collect()
        for row in rows:
            self.assertEqual(row.action_type, ACTION_CLICK)

        # Check data transformation
        self.assertIn("customer_id", prepared_clicks.columns)
        self.assertIn("action_item_id", prepared_clicks.columns)

    def test_prepare_add_to_carts(self):
        """Test add to carts data preparation"""
        self.transformer.__prepare_impressions__(self.impressions_df)

        prepared_carts = self.transformer.__prepare_add_to_carts__(self.carts_df)

        # Check schema
        expected_columns = ["customer_id", "action_item_id", "action_time", "action_type"]
        self.assertEqual(prepared_carts.columns, expected_columns)

        # Check action type
        rows = prepared_carts.collect()
        for row in rows:
            self.assertEqual(row.action_type, ACTION_ATC)

    def test_prepare_previous_orders(self):
        """Test previous orders data preparation"""
        self.transformer.__prepare_impressions__(self.impressions_df)

        prepared_orders = self.transformer.__prepare_previous_orders__(self.orders_df)

        # Check schema
        expected_columns = ["customer_id", "action_item_id", "action_time", "action_type"]
        self.assertEqual(prepared_orders.columns, expected_columns)

        # Check action type
        rows = prepared_orders.collect()
        for row in rows:
            self.assertEqual(row.action_type, ACTION_ORD)

    def test_build_action_history(self):
        """Test building unified action history"""
        self.transformer.__prepare_impressions__(self.impressions_df)

        clicks_df = self.transformer.__prepare_clicks__(self.clicks_df)
        carts_df = self.transformer.__prepare_add_to_carts__(self.carts_df)
        orders_df = self.transformer.__prepare_previous_orders__(self.orders_df)

        actions_df = self.transformer.build_action_history(clicks_df, carts_df, orders_df)

        # Must have all actions combined
        total_expected = clicks_df.count() + carts_df.count() + orders_df.count()
        self.assertEqual(actions_df.count(), total_expected)

        # Check that having the all action types
        action_types = [row.action_type for row in actions_df.collect()]
        self.assertIn(ACTION_CLICK, action_types)
        self.assertIn(ACTION_ATC, action_types)
        self.assertIn(ACTION_ORD, action_types)

    def test_join_impressions_with_actions_schema(self):
        """Test final join produces correct schema"""
        (impressions_df, clicks_df, carts_df, orders_df) = self.transformer.prepare_data(
            self.impressions_df, self.clicks_df, self.carts_df, self.orders_df)

        actions_df = self.transformer.build_action_history(clicks_df, carts_df, orders_df)
        final_df = self.transformer.join_impressions_with_actions(actions_df, impressions_df)

        # Check schema
        expected_columns = ["dt", "customer_id", "ranking_id", "item_id", "is_order", "actions", "action_types"]
        self.assertEqual(final_df.columns, expected_columns)

        # Check that we have exploded impressions (12 rows)
        self.assertEqual(final_df.count(), 12)

    def test_join_impressions_with_actions_data_format(self):
        """Test that final data has correct format for PyTorch"""
        (impressions_df, clicks_df, carts_df, orders_df) = self.transformer.prepare_data(
            self.impressions_df, self.clicks_df, self.carts_df, self.orders_df)

        actions_df = self.transformer.build_action_history(clicks_df, carts_df, orders_df)
        final_df = self.transformer.join_impressions_with_actions(actions_df, impressions_df, MAX_ACTIONS=10)

        rows = final_df.collect()

        for row in rows:
            # Check actions array is exactly 10 elements (padded)
            self.assertEqual(len(row.actions), 10)
            self.assertEqual(len(row.action_types), 10)

            # Check padding values
            actions_list = row.actions
            action_types_list = row.action_types

            # Count non-zero actions
            non_zero_actions = [x for x in actions_list if x != 0]
            non_none_types = [x for x in action_types_list if x != ACTION_NONE]

            # Should have same number of non-zero actions and non-none types
            self.assertEqual(len(non_zero_actions), len(non_none_types))

    def test_temporal_filtering(self):
        """Test that actions are filtered correctly by impression date"""
        print("\n=== Testing Temporal Filtering ===")

        original_clicks = self.clicks_df.count()
        original_carts = self.carts_df.count()
        original_orders = self.orders_df.count()
        print(f"Original data: {original_clicks} clicks, {original_carts} carts, {original_orders} orders")

        (impressions_df, clicks_df, carts_df, orders_df) = self.transformer.prepare_data(
            self.impressions_df, self.clicks_df, self.carts_df, self.orders_df)

        # Count filtered actions (should exclude 1+ year old data)
        filtered_clicks = clicks_df.count()
        filtered_carts = carts_df.count()
        filtered_orders = orders_df.count()
        print(f"After 1-year filter: {filtered_clicks} clicks, {filtered_carts} carts, {filtered_orders} orders")

        # Should filter out old data
        print(f"Old clicks filtered" if filtered_clicks < original_clicks else f"No clicks filtered!")
        print(f"Old carts filtered" if filtered_carts < original_carts else f"No carts filtered!")
        print(f"Old orders filtered" if filtered_orders < original_orders else f"No orders filtered!")

        actions_df = self.transformer.build_action_history(clicks_df, carts_df, orders_df)
        final_df = self.transformer.join_impressions_with_actions(actions_df, impressions_df, MAX_ACTIONS=20)

        # Get customer 1's impressions on 2024-12-15
        customer_1_dec_15 = [r for r in final_df.collect()
                           if r.customer_id == 1 and r.dt == "2024-12-15"]

        # Should have 2 impressions for customer 1 on 2024-12-15 (items 101, 102)
        expected_impressions = 2
        actual_impressions = len(customer_1_dec_15)
        print(f"Customer 1 impressions on 2024-12-15: {actual_impressions} (expected: {expected_impressions})")
        print(f"Impression count matches" if actual_impressions == expected_impressions else f"Impression count mismatch!")
        self.assertEqual(actual_impressions, expected_impressions)

        # Both should have the same action history (up to 2024-12-15)
        if len(customer_1_dec_15) >= 2:
            actions_row1 = customer_1_dec_15[0].actions
            actions_row2 = customer_1_dec_15[1].actions

            print(f"First impression actions: {[x for x in actions_row1 if x != 0][:5]}...")
            print(f"Second impression actions: {[x for x in actions_row2 if x != 0][:5]}...")

            actions_match = actions_row1 == actions_row2
            print(f"Action sequences match" if actions_match else f"Action sequences don't match!")
            self.assertEqual(actions_row1, actions_row2)

            non_zero_actions = [x for x in actions_row1 if x != 0]
            print(f"Number of non-zero actions for customer 1 on 2024-12-15: {len(non_zero_actions)}")

            # Customer 1 should have: 1 order (402 from 2024-12-10) up to 2024-12-15
            # clicks: 203, 204, 205 and
            # carts: 302, 303 up to 2024-12-15
            expected_min_actions = 1
            print(f"Expected minimum actions: {expected_min_actions}")
            print(f"Has sufficient actions" if len(non_zero_actions) >= expected_min_actions else f"Insufficient actions!")
            self.assertGreaterEqual(len(non_zero_actions), expected_min_actions)


if __name__ == '__main__':
    unittest.main()
