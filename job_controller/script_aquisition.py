"""
Contains the functions for acquiring a script for the reduction workflow
"""

from typing import Tuple
import requests


def acquire_script(ir_api_host: str, reduction_id: int, instrument: str) -> Tuple[str, str]:
    """
    Given the IR-API host, reduction_id, and instrument, return the script object required for reduction
    :return: Script, the script for the reduction
    """
    response = requests.get(
        f"http://{ir_api_host}/instrument/{instrument}/script?reduction_id={reduction_id}", timeout=30
    )
    if response.status_code != 200:
        response.raise_for_status()
        # raise_for_status will only raise a httperror for a httperror, this will raise for everything else that isn't
        # a 200 or a script return
        raise RuntimeError(f"Script was never returned, due to unexpected status code {response.status_code}")
    response_object = response.json()
    return apply_json_output(response_object["value"]), response_object.get("sha", None)


def apply_json_output(script: str) -> str:
    """
    The aim is to force whatever the script that is passed to also output to stdinput a json string that consists of
    3 values, status of the run (status), status message, and output files.
    :return: The passed script with 3 lines added to ensure a json dump occurs at the end
    """
    script_addon = (
        "import json\n"
        "\n"
        "print(json.dumps({'status': 'Successful', 'status_message': '', 'output_files': output}))\n"
    )
    return script + "\n" + script_addon
