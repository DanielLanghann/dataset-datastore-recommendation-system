from django.test import TestCase
from django.utils import timezone
from matching_engine.models import MatchingResponse


class MatchingResponseTestCase(TestCase):
    """Unit tests for MatchingResponse model methods"""

    def setUp(self):
        """Set up test data"""
        self.base_data = {
            "model": "llama3.1:8b",
            "description": "Test response",
            "created_at": timezone.now(),
        }

    def test_get_datasets_count_with_datasets_key(self):
        """Test counting datasets with 'datasets' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"datasets": [1, 2, 3, 4]}
        )
        self.assertEqual(response.get_datasets_count(), 4)

    def test_get_datasets_count_with_dataset_ids_key(self):
        """Test counting datasets with 'dataset_ids' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"dataset_ids": [10, 20, 30]}
        )
        self.assertEqual(response.get_datasets_count(), 3)

    def test_get_datasets_count_with_single_dataset_id(self):
        """Test counting datasets with single 'dataset_id' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"dataset_id": 42}
        )
        self.assertEqual(response.get_datasets_count(), 1)

    def test_get_datasets_count_with_list_of_objects(self):
        """Test counting datasets from list of objects with dataset_id"""
        response = MatchingResponse.objects.create(
            **self.base_data,
            result=[
                {"dataset_id": 1, "confidence": 0.9},
                {"dataset_id": 2, "confidence": 0.8},
                {"dataset_id": 3, "confidence": 0.7},
            ]
        )
        self.assertEqual(response.get_datasets_count(), 3)

    def test_get_datasets_count_with_list_of_ids(self):
        """Test counting datasets from list of direct IDs"""
        response = MatchingResponse.objects.create(
            **self.base_data, result=[1, 2, 3, 4, 5]
        )
        self.assertEqual(response.get_datasets_count(), 5)

    def test_get_datasets_count_empty_result(self):
        """Test counting datasets with empty result"""
        response = MatchingResponse.objects.create(**self.base_data, result={})
        self.assertEqual(response.get_datasets_count(), 0)

    def test_get_datasets_count_null_result(self):
        """Test counting datasets with null result"""
        # Since result field has NOT NULL constraint, we'll test the method directly
        response = MatchingResponse(**self.base_data, result={})
        response.result = None  # Set after creation to test the method
        self.assertEqual(response.get_datasets_count(), 0)

    def test_get_datasets_count_empty_list(self):
        """Test counting datasets with empty datasets list"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"datasets": []}
        )
        self.assertEqual(response.get_datasets_count(), 0)

    def test_get_datasets_count_invalid_structure(self):
        """Test counting datasets with invalid JSON structure"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"invalid_key": "invalid_value"}
        )
        self.assertEqual(response.get_datasets_count(), 0)

    # Tests for get_datastores_count()

    def test_get_datastores_count_with_datastores_key(self):
        """Test counting datastores with 'datastores' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"datastores": [101, 102, 103]}
        )
        self.assertEqual(response.get_datastores_count(), 3)

    def test_get_datastores_count_with_datastore_ids_key(self):
        """Test counting datastores with 'datastore_ids' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"datastore_ids": [201, 202]}
        )
        self.assertEqual(response.get_datastores_count(), 2)

    def test_get_datastores_count_with_matched_datastores_key(self):
        """Test counting datastores with 'matched_datastores' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"matched_datastores": [301, 302, 303, 304]}
        )
        self.assertEqual(response.get_datastores_count(), 4)

    def test_get_datastores_count_with_matched_datastore_ids_key(self):
        """Test counting datastores with 'matched_datastore_ids' key"""
        response = MatchingResponse.objects.create(
            **self.base_data,
            result={"matched_datastore_ids": [401, 402, 403, 404, 405]}
        )
        self.assertEqual(response.get_datastores_count(), 5)

    def test_get_datastores_count_with_single_datastore_id(self):
        """Test counting datastores with single 'datastore_id' key"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"datastore_id": 501}
        )
        self.assertEqual(response.get_datastores_count(), 1)

    def test_get_datastores_count_with_list_of_objects(self):
        """Test counting datastores from list of objects with datastore_id"""
        response = MatchingResponse.objects.create(
            **self.base_data,
            result=[
                {"datastore_id": 601, "reason": "High relevance"},
                {"datastore_id": 602, "reason": "Good match"},
                {"matched_datastore_id": 603, "confidence": 0.9},
            ]
        )
        self.assertEqual(response.get_datastores_count(), 3)

    def test_get_datastores_count_empty_result(self):
        """Test counting datastores with empty result"""
        response = MatchingResponse.objects.create(**self.base_data, result={})
        self.assertEqual(response.get_datastores_count(), 0)

    def test_get_datastores_count_null_result(self):
        """Test counting datastores with null result"""
        # Since result field has NOT NULL constraint, we'll test the method directly
        response = MatchingResponse(**self.base_data, result={})
        response.result = None  # Set after creation to test the method
        self.assertEqual(response.get_datastores_count(), 0)

    def test_get_datastores_count_empty_list(self):
        """Test counting datastores with empty datastores list"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"matched_datastores": []}
        )
        self.assertEqual(response.get_datastores_count(), 0)

    def test_get_datastores_count_invalid_structure(self):
        """Test counting datastores with invalid JSON structure"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"random_data": "not_relevant"}
        )
        self.assertEqual(response.get_datastores_count(), 0)

    # Combined tests

    def test_both_counts_with_complex_structure(self):
        """Test both counting methods with complex JSON structure"""
        response = MatchingResponse.objects.create(
            **self.base_data,
            result={
                "datasets": [1, 2, 3],
                "matched_datastore_ids": [101, 102],
                "reasons": ["reason1", "reason2"],
                "confidence_scores": [0.9, 0.8, 0.7],
            }
        )
        self.assertEqual(response.get_datasets_count(), 3)
        self.assertEqual(response.get_datastores_count(), 2)

    def test_both_counts_with_mixed_list_structure(self):
        """Test both counting methods with mixed object structure"""
        response = MatchingResponse.objects.create(
            **self.base_data,
            result=[
                {
                    "dataset_id": 1,
                    "datastore_id": 101,
                    "confidence": 0.9,
                    "reason": "Perfect match",
                },
                {
                    "dataset_id": 2,
                    "matched_datastore_id": 102,
                    "confidence": 0.8,
                    "reason": "Good match",
                },
                {"other_field": "no_ids_here"},
            ]
        )
        self.assertEqual(response.get_datasets_count(), 2)
        self.assertEqual(response.get_datastores_count(), 2)

    def test_malformed_json_handling(self):
        """Test handling of malformed JSON data"""
        response = MatchingResponse.objects.create(
            **self.base_data, result={"datasets": "not_a_list", "datastores": None}
        )
        self.assertEqual(response.get_datasets_count(), 0)
        self.assertEqual(response.get_datastores_count(), 0)

    def test_string_result_handling(self):
        """Test handling when result is a string instead of dict/list"""
        response = MatchingResponse.objects.create(
            **self.base_data, result="invalid json structure"
        )
        self.assertEqual(response.get_datasets_count(), 0)
        self.assertEqual(response.get_datastores_count(), 0)

    def test_nested_structure_ignored(self):
        """Test that nested structures are properly handled"""
        response = MatchingResponse.objects.create(
            **self.base_data,
            result={
                "metadata": {
                    "datasets": [1, 2, 3],  # Nested, should be ignored
                    "datastores": [101, 102],  # Nested, should be ignored
                },
                "datasets": [4, 5],  # Top level, should be counted
                "matched_datastores": [103, 104, 105],  # Top level, should be counted
            }
        )
        self.assertEqual(response.get_datasets_count(), 2)
        self.assertEqual(response.get_datastores_count(), 3)
