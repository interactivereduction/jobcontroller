"""
Contains the functions for acquiring a script for the reduction workflow
"""
import requests


def acquire_script(ir_api_host: str, reduction_id: int, instrument: str) -> str:
    """
    The aim of this function is to acquire the script from the IR-API by using the passed filename and ip. The
    responsibility for figuring out what script is what, and doing the substitution is on the API to figure out.
    :return: str, the script for the reduction
    """
    # Currently unused but will be used in the future
    response = requests.get(
        f"http://{ir_api_host}/instrument/{instrument}/script?reduction_id={reduction_id}", timeout=30
    )
    if response.status_code == 200:
        return apply_json_output(response.json()["value"])
    else:
        response.raise_for_status()


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
