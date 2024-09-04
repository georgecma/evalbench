r"""Tests for efv."""

import unittest
import eval_format_verifier as efv
import eval_format_verifier_test_helper as mock_dataset


class EvalFormatVerifierTest(unittest.TestCase):
    """Test the eval format verifier."""

    def test_empty_json_success(self):
        efv.validate_json_format(
            [],
        )

    def test_good_json_success(self):
        json_data = mock_dataset.get_default_single_json()
        efv.validate_json_format(
            json_data,
        )

    def test_poor_format_json_failure(self):
        json_data = mock_dataset.get_default_single_json({"nl_prompt": 1})
        with self.assertRaises(ValueError):
            efv.validate_json_format(
                json_data,
            )
        json_data = mock_dataset.get_default_single_json(
            {"dialects": "not a string"}
        )
        with self.assertRaises(ValueError):
            efv.validate_json_format(
                json_data,
            )

    def test_good_json_list_success(self):
        json_data = mock_dataset.get_default_good_json_list()
        efv.validate_json_format(
            json_data,
        )

    def test_bad_id_json_failure(self):
        json_data = mock_dataset.get_default_single_json({"id": "not_an_int"})
        with self.assertRaises(ValueError):
            efv.validate_json_format(
                json_data,
            )

    def test_duplicate_id_json_failure(self):
        json_data = mock_dataset.get_default_good_json_list()
        json_data[2].update({"id": 1})
        with self.assertRaises(ValueError):
            efv.validate_json_format(
                json_data,
            )

    def test_no_dialects_json_failure(self):
        json_data = mock_dataset.get_default_single_json({"dialects": []})
        with self.assertRaises(ValueError):
            efv.validate_json_format(
                json_data,
            )

    def test_unsupported_dialect_json_failure(self):
        json_data = mock_dataset.get_default_single_json(
            {"dialects": ["unsupported_dialect"]}
        )
        with self.assertRaises(ValueError):
            efv.validate_json_format(
                json_data,
            )

    def test_dialect_not_in_required_sql_json_failure(self):
        for sql_type in [
            "golden_sql",
            "eval_query",
            "setup_sql",
            "cleanup_sql",
        ]:
            json_data = mock_dataset.get_default_single_json(
                {"dialects": ["mysql", "postgres"], sql_type: {"mysql": ["SELECT 1"]}}
            )
            with self.assertRaises(ValueError):
                efv.validate_json_format(
                    json_data,
                )

    def test_required_fields_missing_json_failure(self):
        for required_field in [
            "id",
            "nl_prompt",
            "query_type",
            "database",
            "dialects",
            "golden_sql",
        ]:
            json_data = mock_dataset.get_default_single_json({required_field: None})
            with self.assertRaises(ValueError):
                efv.validate_json_format(
                    json_data,
                )


if __name__ == "__main__":
    unittest.main()
