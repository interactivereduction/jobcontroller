"""
Contains the functions for acquiring a script for the reduction workflow
"""


def acquire_script(filename: str, ir_api_ip: str) -> str:
    """
    The aim of this function is to acquire the script from the IR-API by using the passed filename and ip. The
    responsibility for figuring out what script is what, and doing the substitution is on the API to figure out.
    :return: str, the script for the reduction
    """
    # Currently unused but will be used in the future
    del ir_api_ip
    return apply_json_output(f"output = []\nprint('Performing run for file: {filename}...')")


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
