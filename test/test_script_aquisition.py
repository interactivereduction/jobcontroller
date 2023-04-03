# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock

from main.script_aquisition import acquire_script, apply_json_output


class UtilsTest(unittest.TestCase):
    def test_acquire_script(self):
        ir_api_ip = mock.MagicMock()
        filename = mock.MagicMock()

        output = acquire_script(filename, ir_api_ip)

        expected_output = (
            "output = []\nprint('Performing run for file: " + str(filename) + "...')\nimport json\n\n"
            "print(json.dumps({'status': 'Successful', 'status_message': '', 'output_files': output}))\n"
        )
        self.assertEqual(expected_output, output)

    def test_apply_json_output(self):
        input_script = "hi, I am an input script\n"

        output = apply_json_output(input_script)

        expected_output = (
            input_script + "\nimport json\n\nprint(json.dumps({'status': 'Successful', "
            "'status_message': '', 'output_files': output}))\n"
        )
        self.assertEqual(expected_output, output)
