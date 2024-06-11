from unittest import mock

import pytest

from jobcreator.script_aquisition import acquire_script, apply_json_output


@mock.patch("jobcreator.script_aquisition.requests")
def test_acquire_script_success(mock_requests):
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"value": "some value", "sha": "some sha"}
    mock_requests.get.return_value = mock_response

    out = acquire_script("", 1, "")

    expected_value = (
        (
            "some value\n"
            "import json\n"
            "\n"
            "print(json.dumps({'status': 'Successful', 'status_message': '', 'output_files': output,"
            " 'stacktrace': ''}))\n"
        ),
        "some sha",
    )

    assert out == expected_value


@mock.patch("jobcreator.script_aquisition.requests")
def test_acquire_script_failure(mock_requests):
    mock_response = mock.Mock()
    mock_requests.get.return_value = mock_response
    mock_response.status = 500
    with pytest.raises(RuntimeError):
        acquire_script("", 1, "")

    mock_response.raise_for_status.assert_called_once()


def test_apply_json_output():
    input_script = "hi, I am an input script\n"

    output = apply_json_output(input_script)

    expected_output = (
        input_script + "\nimport json\n\nprint(json.dumps({'status': 'Successful', "
        "'status_message': '', 'output_files': output, 'stacktrace': ''}))\n"
    )
    assert expected_output == output
