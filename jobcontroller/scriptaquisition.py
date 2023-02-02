"""
Contains the functions for aquiring a script for the reduction workflow
"""


def aquire_script(filename: str, ir_api_ip: str) -> str:
    """
    The aim of this function is to aquire the script from the IR-API by using the passed filename and ip. The
    responsibility for figuring out what script is what, and doing the substitution is on the API to figure out.
    :return: str, the script for the reduction
    """
    # Currently unused but will be used in the future
    del filename, ir_api_ip
    return "print('test')"