"""
The SQLAlchemy enum for the State of the reduction status
"""
from sqlalchemy import Enum


class State(Enum):  # pylint: disable=too-many-ancestors
    """
    The State Enum for reduction status
    """

    SUCCESSFUL = "Successful"
    UNSUCCESSFUL = "Unsuccessful"
    ERROR = "Error"
    NOT_STARTED = "NotStarted"
