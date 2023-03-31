from sqlalchemy import Enum


class State(Enum):
    Successful = "Successful"
    Unsuccessful = "Unsuccessful"
    Error = "Error"
    NotStarted = "NotStarted"
