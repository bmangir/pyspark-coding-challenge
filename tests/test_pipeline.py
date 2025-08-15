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

    @patch('src.pipeline.read_json')
    @patch('src.pipeline.SparkSession.builder')
    def test_process_daily_training_data_with_date_filter(self, mock_spark_builder, mock_read_json):
        """Test daily training data function with date filter"""
        print("\n=== Testing Daily Data Filter ===")

        mock_spark = MagicMock()
        mock_spark_builder.appName.return_value.getOrCreate.return_value = mock_spark

        mock_read_json.side_effect = [
            self.impressions_df,
            self.clicks_df,
            self.carts_df,
            self.orders_df
        ]

        # Test with date filter for 2024-12-15
        target_date = "2024-12-15"
        print(f"Filtering for date: {target_date}")

        with patch('builtins.print'):
            result_df = main(target_date)

        all_rows = result_df.collect()
        filtered_rows = [r for r in all_rows if r.dt == target_date]
        other_date_rows = [r for r in all_rows if r.dt != target_date]

        expected_filtered = 2  # Customer 1 has 2 impressions on 2024-12-15
        actual_filtered = len(filtered_rows)
        actual_other = len(other_date_rows)

        print(f"Expected impressions for {target_date}: {expected_filtered}")
        print(f"Actual impressions for {target_date}: {actual_filtered}")
        print(f"Impressions for other dates: {actual_other}")

        print(f"Correct filter count" if actual_filtered == expected_filtered else f"Filter count mismatch!")
        print(f"No other dates" if actual_other == 0 else f"Found {actual_other} rows from other dates!")

        self.assertEqual(actual_filtered, expected_filtered)
        self.assertEqual(actual_other, 0)


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests that test the actual pipeline logic without mocking"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = create_test_spark_session()

    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()

    def test_end_to_end_training_data_format(self):
        """Test end-to-end pipeline produces correct training data format"""
        print("\n=== Testing End-to-End Training Data Format ===")

        impressions_df, clicks_df, carts_df, orders_df = create_test_dataframes(self.spark)

        original_impressions = impressions_df.count()
        original_clicks = clicks_df.count()
        original_carts = carts_df.count()
        original_orders = orders_df.count()

        print(f"Original data: {original_impressions} impression records, {original_clicks} clicks, {original_carts} carts, {original_orders} orders")

        from src.transformations import Transformer  # Avoid circular import

        transformer = Transformer("TestTransformer", n_actions=10, days=365)
        (impressions_df, clicks_df, carts_df, orders_df) = transformer.prepare_data(
            impressions_df, clicks_df, carts_df, orders_df)

        actions_df = transformer.build_action_history(clicks_df, carts_df, orders_df)
        final_df = transformer.join_impressions_with_actions(actions_df, impressions_df, MAX_ACTIONS=10)

        results = final_df.collect()

        # Should have 12 training samples (exploded impressions)
        expected_samples = 12
        actual_samples = len(results)
        print(f"Expected training samples: {expected_samples}")
        print(f"Actual training samples: {actual_samples}")
        print(f"Sample count matches" if actual_samples == expected_samples else f"Sample count mismatch!")
        self.assertEqual(actual_samples, expected_samples)

        # Test specific training samples
        for i, row in enumerate(results[:3]):
            print(f"\nSample {i+1}: customer {row.customer_id}, item {row.item_id}, date {row.dt}")

            # Each row should have correct format for PyTorch
            actions_len = len(row.actions)
            types_len = len(row.action_types)

            print(f"  Actions length: {actions_len} (expected: 10)")
            print(f"  Action types length: {types_len} (expected: 10)")
            print(f"  Arrays properly padded" if actions_len == 10 and types_len == 10 else f"  Array padding incorrect!")

            self.assertEqual(actions_len, 10)  # Padded to 10
            self.assertEqual(types_len, 10)  # Padded to 10

            # Check data types
            self.assertIsInstance(row.customer_id, int)
            self.assertIsInstance(row.item_id, int)
            self.assertIsInstance(row.dt, str)
            self.assertIsInstance(row.ranking_id, str)
            self.assertIsInstance(row.is_order, bool)
            self.assertIsInstance(row.actions, list)
            self.assertIsInstance(row.action_types, list)

            # Show non-zero actions
            non_zero_actions = [x for x in row.actions if x != 0]
            non_none_types = [x for x in row.action_types if x != 0]
            print(f"  Non-zero actions: {non_zero_actions[:5]}{'...' if len(non_zero_actions) > 5 else ''}")
            print(f"  Corresponding types: {non_none_types[:5]}{'...' if len(non_none_types) > 5 else ''}")

        # Test customer 1's data progression over time
        customer_1_rows = [r for r in results if r.customer_id == 1]
        print(f"\nCustomer 1 has {len(customer_1_rows)} impressions")
        self.assertGreater(len(customer_1_rows), 0)

        # Customer 1 should have more actions on later dates
        cust_1_dec_15_rows = [r for r in customer_1_rows if r.dt == "2024-12-15"]
        cust_1_dec_16_rows = [r for r in customer_1_rows if r.dt == "2024-12-16"]

        if cust_1_dec_15_rows and cust_1_dec_16_rows:
            dec_15_actions = [x for x in cust_1_dec_15_rows[0].actions if x != 0]
            dec_16_actions = [x for x in cust_1_dec_16_rows[0].actions if x != 0]

            print(f"Customer 1 actions on 2024-12-15: {len(dec_15_actions)}")
            print(f"Customer 1 actions on 2024-12-16: {len(dec_16_actions)}")

            print(f"Action progression correct" if len(dec_16_actions) >= len(dec_15_actions) else f"Action progression incorrect!")
            self.assertGreaterEqual(len(dec_16_actions), len(dec_15_actions))

    def test_pytorch_compatibility(self):
        """Test that output format is compatible with PyTorch model signature"""

        impressions_df, clicks_df, carts_df, orders_df = create_test_dataframes(self.spark)

        from src.transformations import Transformer

        transformer = Transformer("TestTransformer", n_actions=1000, days=365)
        (impressions_df, clicks_df, carts_df, orders_df) = transformer.prepare_data(
            impressions_df, clicks_df, carts_df, orders_df)

        actions_df = transformer.build_action_history(clicks_df, carts_df, orders_df)
        final_df = transformer.join_impressions_with_actions(actions_df, impressions_df, MAX_ACTIONS=1000)

        # Simulate creating PyTorch tensors
        results = final_df.collect()

        # Extract data in PyTorch format
        impressions_batch = [row.item_id for row in results]        # [batch_size]
        actions_batch = [row.actions for row in results]            # [batch_size, 1000]
        action_types_batch = [row.action_types for row in results]  # [batch_size, 1000]

        # Validate shapes
        batch_size = len(results)
        self.assertEqual(len(impressions_batch), batch_size)
        self.assertEqual(len(actions_batch), batch_size)
        self.assertEqual(len(action_types_batch), batch_size)

        # Each sequence should be exactly 1000 elements
        for actions_seq, types_seq in zip(actions_batch, action_types_batch):
            self.assertEqual(len(actions_seq), 1000)
            self.assertEqual(len(types_seq), 1000)

        # Validate action types are within expected range
        from src.config import ACTION_NONE, ACTION_CLICK, ACTION_ATC, ACTION_ORD
        valid_action_types = {ACTION_NONE, ACTION_CLICK, ACTION_ATC, ACTION_ORD}

        for types_seq in action_types_batch:
            for action_type in types_seq:
                self.assertIn(action_type, valid_action_types)

    def test_daily_breakdown_functionality(self):
        """Test that daily breakdown works for training iteration"""

        impressions_df, clicks_df, carts_df, orders_df = create_test_dataframes(self.spark)

        from src.transformations import Transformer

        # Test processing different days
        dates_to_test = ["2024-12-15", "2024-12-16"]
        daily_results = {}

        for date in dates_to_test:
            filtered_impressions = impressions_df.filter(impressions_df.dt == date)
            daily_results[date] = []

            if filtered_impressions.count() > 0:
                transformer = Transformer("TestTransformer", n_actions=10, days=365)
                (prep_impressions, prep_clicks, prep_carts, prep_orders) = transformer.prepare_data(
                    filtered_impressions, clicks_df, carts_df, orders_df)

                actions_df = transformer.build_action_history(prep_clicks, prep_carts, prep_orders)
                final_df = transformer.join_impressions_with_actions(actions_df, prep_impressions, MAX_ACTIONS=10)

                daily_results[date] = final_df.collect()

        # Validate daily results
        self.assertIn("2024-12-15", daily_results)
        self.assertIn("2024-12-16", daily_results)

        # 2024-12-15 should have 2 samples (customer 1: items 101, 102)
        self.assertEqual(len(daily_results["2024-12-15"]), 2)

        # 2024-12-16 should have 4 samples (customer 1: items 103, 104 + customer 2: items 105, 106)
        self.assertEqual(len(daily_results["2024-12-16"]), 4)


if __name__ == '__main__':
    unittest.main()
