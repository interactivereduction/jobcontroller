"""
The SQLAlchemy enum for the State of the reduction status
"""
from sqlalchemy import Enum


class State(Enum):  # pylint: disable=too-many-ancestors
    """
    The State Enum for reduction status
    """

    Successful = "Successful"
    Unsuccessful = "Unsuccessful"
    Error = "Error"
    NotStarted = "NotStarted"
