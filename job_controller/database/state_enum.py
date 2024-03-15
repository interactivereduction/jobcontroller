"""
The SQLAlchemy enum for the State of the reduction status
"""

from enum import Enum


class State(Enum):  # pylint: disable=too-many-ancestors
    """
    The State Enum for reduction status
    """

    SUCCESSFUL = "SUCCESSFUL"
    UNSUCCESSFUL = "UNSUCCESSFUL"
    ERROR = "ERROR"
    NOT_STARTED = "NOT_STARTED"
