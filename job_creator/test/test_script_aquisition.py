# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access,
# pylint: disable=too-many-instance-attributes
import unittest
from unittest.mock import patch, Mock

from jobcreator.script_aquisition import acquire_script, apply_json_output


class ScriptAquisitionTest(unittest.TestCase):
    @patch("jobcreator.script_aquisition.requests")
    def test_acquire_script_success(self, mock_requests):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": "some value", "sha": "some sha"}
        mock_requests.get.return_value = mock_response

        out = acquire_script("", 1, "")

        expected_value = (
            "some value\n"
            "import json\n"
            "\n"
            "print(json.dumps({'status': 'Successful', 'status_message': '', 'output_files': output}))\n"
        ), "some sha"

        assert out == expected_value

    @patch("jobcreator.script_aquisition.requests")
    def test_acquire_script_failure(self, mock_requests):
        mock_response = Mock()
        mock_requests.get.return_value = mock_response
        mock_response.status = 500
        with self.assertRaises(Exception):
            acquire_script("", 1, "")

            mock_response.raise_for_status.assert_called_once()

    def test_apply_json_output(self):
        input_script = "hi, I am an input script\n"

        output = apply_json_output(input_script)

        expected_output = (
            input_script + "\nimport json\n\nprint(json.dumps({'status': 'Successful', "
            "'status_message': '', 'output_files': output}))\n"
        )
        self.assertEqual(expected_output, output)
