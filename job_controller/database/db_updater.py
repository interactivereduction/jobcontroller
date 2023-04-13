"""
This module is responsible for holding the SQL Classes that SQLAlchemy will use and then formatting the SQL queries
via SQLAlchemy via pre-made functions.
"""

from typing import Any, Dict, List

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    QueuePool,  # type: ignore[attr-defined]
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, sessionmaker, declarative_base  # type: ignore[attr-defined]

from job_controller.database.state_enum import State
from job_controller.utils import logger

Base = declarative_base()


class Run(Base):  # type: ignore[valid-type, misc]
    """
    The Run Table's declarative declaration
    """

    __tablename__ = "runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String)
    title = Column(String)
    users = Column(String)
    experiment_number = Column(Integer)
    run_start = Column(DateTime)
    run_end = Column(DateTime)
    good_frames = Column(Integer)
    raw_frames = Column(Integer)
    reductions = relationship("RunReduction", back_populates="run_relationship")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Run):
            return (
                self.filename == other.filename
                and self.title == other.title
                and self.users == other.users
                and self.experiment_number == other.experiment_number
                and self.run_start == other.run_start
                and self.run_end == other.run_end
                and self.good_frames == other.good_frames
                and self.raw_frames == other.raw_frames
            )
        return False


class Script(Base):  # type: ignore[valid-type, misc]
    """
    The Script Table's declarative declaration
    """

    __tablename__ = "scripts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    script = Column(String)
    reductions = relationship("Reduction", back_populates="script_relationship")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Script):
            return self.script == other.script
        return False


class Reduction(Base):  # type: ignore[valid-type, misc]
    """
    The Reduction Table's declarative declaration
    """

    __tablename__ = "reductions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    reduction_start = Column(DateTime)
    reduction_end = Column(DateTime)
    reduction_state = Column(State)
    reduction_status_message = Column(String)
    reduction_inputs = Column(JSONB)
    script = Column(Integer, ForeignKey("scripts.id"))
    script_relationship = relationship("Script", back_populates="reductions")
    reduction_outputs = Column(String)
    run_reduction_relationship = relationship("RunReduction", back_populates="reduction_relationship")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Reduction):
            return (
                self.reduction_start == other.reduction_start
                and self.reduction_end == other.reduction_end
                and self.reduction_state == other.reduction_state
                and self.reduction_status_message == other.reduction_status_message
                and self.reduction_inputs == other.reduction_inputs
                and self.script == other.script
                and self.reduction_outputs == other.reduction_outputs
            )
        return False


class RunReduction(Base):  # type: ignore[valid-type, misc]
    """
    The RunReduction Table's declarative declaration
    """

    __tablename__ = "runs_reductions"
    run = Column(Integer, ForeignKey("runs.id"), primary_key=True)
    reduction = Column(Integer, ForeignKey("reductions.id"), primary_key=True)
    run_relationship = relationship("Run", back_populates="reductions")
    reduction_relationship = relationship("Reduction", back_populates="run_reduction_relationship")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RunReduction):
            return self.run == other.run and self.reduction == other.reduction
        return False


class DBUpdater:
    """
    The class responsible for handling session state, and sending SQL queries via SQLAlchemy
    """

    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/interactive-reduction"
        engine = create_engine(connection_string, poolclass=QueuePool, pool_size=20)
        self.session_maker_func = sessionmaker(bind=engine)
        self.runs_table = Run()
        self.reductions_table = Reduction()
        self.runs_reductions_table = RunReduction()
        self.script_table = Script()

    # pylint: disable=too-many-arguments
    def add_detected_run(
        self,
        filename: str,
        title: str,
        users: str,
        experiment_number: str,
        run_start: str,
        run_end: str,
        good_frames: str,
        raw_frames: str,
        reduction_inputs: Dict[str, Any],
    ) -> Column[int]:
        """
        This function submits data to the database from what is initially available on detected-runs kafka topic
        :param filename: the filename of the run that needs to be reduced
        :param title: The title of the run file
        :param users: The users entered into the run file
        :param experiment_number: The RB number of the run entered by users
        :param run_start: The time at which the run started, created using the standard python time format.
        :param run_end: The time at which the run ended, created using the standard python time format.
        :param good_frames: The number of frames that are considered "good" in the file
        :param raw_frames: The number of frames that are in the file
        :param reduction_inputs: The inputs to be used by the reduction
        :return: The id of the reduction row entry
        """
        logger.info(
            "Submitting detected-run to the database: {filename: %s, title: %s, users: %s, "
            "experiment_number: %s, run_start: %s, run_end: %s, good_frames: %s, raw_frames: %s, "
            "reduction_inputs: %s}",
            filename,
            title,
            users,
            experiment_number,
            run_start,
            run_end,
            good_frames,
            raw_frames,
            reduction_inputs,
        )
        session = self.session_maker_func()
        run = Run(
            filename=filename,
            title=title,
            users=users,
            experiment_number=experiment_number,
            run_start=run_start,
            run_end=run_end,
            good_frames=good_frames,
            raw_frames=raw_frames,
        )
        reduction = Reduction(
            reduction_start=None,
            reduction_end=None,
            reduction_state=None,
            reduction_inputs=reduction_inputs,
            script=None,
            reduction_outputs=None,
        )
        session.add(run)
        session.add(reduction)
        session.commit()
        # Now create the run_reduction entry and add it
        run_reduction = RunReduction(run=run.id, reduction=reduction.id)
        session.add(run_reduction)
        session.commit()

        logger.info(
            "Submitted detected-run to the database successfully: {filename: %s, title: %s, users: %s, "
            "experiment_number: %s, run_start: %s, run_end: %s, good_frames: %s, raw_frames: %s, "
            "reduction_inputs: %s}",
            filename,
            title,
            users,
            experiment_number,
            run_start,
            run_end,
            good_frames,
            raw_frames,
            reduction_inputs,
        )

        return reduction.id

    def add_completed_run(
        self,
        db_reduction_id: int,
        reduction_inputs: Dict[str, Any],
        state: State,
        status_message: str,
        output_files: List[str],
        reduction_script: str,
    ) -> None:
        """
        This function submits data to the database from what is initially available on completed-runs kafka topic
        :param db_reduction_id: The ID for the reduction row in the reduction table
        :param reduction_inputs: The inputs used in the reduction script by the IR-API
        :param state: The state of how the run ended
        :param status_message: The message that accompanies the state for how the  state ended, if the state for
        example was unsuccessful or an error, it would have the reason/error message.
        :param output_files: The files output from the reduction job
        :param reduction_script: The script used in the reduction
        :return:
        """
        logger.info(
            "Submitting completed-run to the database: {id: %s, reduction_inputs: %s, state: %s, "
            "status_message: %s, output_files: %s, reduction_script: %s}",
            db_reduction_id,
            reduction_inputs,
            str(state),
            status_message,
            output_files,
            reduction_script,
        )
        session = self.session_maker_func()
        script = session.query(Script).filter_by(script=reduction_script).first()
        if script is None:
            script = Script(script=reduction_script)
            session.add(script)
            session.commit()

        reduction = session.query(Reduction).filter_by(id=db_reduction_id).one()
        reduction.reduction_state = str(state)
        reduction.reduction_inputs = reduction_inputs
        reduction.script = script.id
        reduction.reduction_outputs = str(output_files)
        reduction.reduction_status_message = status_message
        session.commit()
        logger.info(
            "Submitted completed-run to the database successfully: {id: %s, reduction_inputs: %s, state: %s, "
            "status_message: %s, output_files: %s, reduction_script: %s}",
            db_reduction_id,
            reduction_inputs,
            str(state),
            status_message,
            output_files,
            reduction_script,
        )


# pylint: enable=too-many-arguments
