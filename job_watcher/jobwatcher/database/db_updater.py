"""
This module is responsible for holding the SQL Classes that SQLAlchemy will use and then formatting the SQL queries
via SQLAlchemy via pre-made functions.
"""
from __future__ import annotations

import textwrap
from typing import Any, List

from sqlalchemy import (  # type: ignore[attr-defined]
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    NullPool,
    Enum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, sessionmaker, declarative_base, Mapped  # type: ignore[attr-defined]

from jobwatcher.database.state_enum import State
from jobwatcher.utils import logger

Base = declarative_base()


class Run(Base):  # type: ignore[valid-type, misc]
    """
    The Run Table's declarative declaration
    """

    __tablename__ = "runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String)
    instrument_id = Column(Integer, ForeignKey("instruments.id"))
    title = Column(String)
    users = Column(String)
    experiment_number = Column(Integer)
    run_start = Column(DateTime)
    run_end = Column(DateTime)
    good_frames = Column(Integer)
    raw_frames = Column(Integer)
    reductions: Mapped[List[Reduction]] = relationship("RunReduction", back_populates="run_relationship")
    instrument: Mapped[Instrument] = relationship("Instrument")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Run):
            return (
                self.filename == other.filename
                and self.title == other.title
                and self.instrument_id == other.instrument_id
                and self.users == other.users
                and self.experiment_number == other.experiment_number
                and self.run_start == other.run_start
                and self.run_end == other.run_end
                and self.good_frames == other.good_frames
                and self.raw_frames == other.raw_frames
            )
        return False

    def __repr__(self) -> str:
        return (
            f"<Run(id={self.id}, filename={self.filename}, instrument_id={self.instrument_id}, title={self.title},"
            f" users={self.users}, experiment_number={self.experiment_number}, run_start={self.run_start},"
            f" run_end={self.run_end}, good_frames={self.good_frames}, raw_frames={self.raw_frames})>"
        )


class Script(Base):  # type: ignore[valid-type, misc]
    """
    The Script Table's declarative declaration
    """

    __tablename__ = "scripts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    script = Column(String, unique=True)
    sha = Column(String, nullable=True)
    reductions: Mapped[Reduction] = relationship("Reduction", back_populates="script")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Script):
            return self.script == other.script
        return False

    def __repr__(self) -> str:
        return f"<Script(id={self.id}, script={self.script})>"


class Reduction(Base):  # type: ignore[valid-type, misc]
    """
    The Reduction Table's declarative declaration
    """

    __tablename__ = "reductions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    reduction_start = Column(DateTime)
    reduction_end = Column(DateTime)
    reduction_state = Column(Enum(State))
    reduction_status_message = Column(String)
    reduction_inputs = Column(JSONB)
    script_id = Column(Integer, ForeignKey("scripts.id"))
    script: Mapped[Script] = relationship("Script", back_populates="reductions")
    reduction_outputs = Column(String)
    stacktrace = Column(String)
    run_reduction_relationship: Mapped[List[Run]] = relationship(
        "RunReduction", back_populates="reduction_relationship"
    )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Reduction):
            return (
                self.reduction_start == other.reduction_start
                and self.reduction_end == other.reduction_end
                and self.reduction_state == other.reduction_state
                and self.reduction_status_message == other.reduction_status_message
                and self.reduction_stack_trace == other.reduction_stack_trace
                and self.reduction_inputs == other.reduction_inputs
                and self.script_id == other.script_id
                and self.reduction_outputs == other.reduction_outputs
            )
        return False

    def __repr__(self) -> str:
        return (
            f"<Reduction(id={self.id}, reduction_start={self.reduction_start}, reduction_end={self.reduction_end},"
            f" reduction_state={self.reduction_state}, reduction_status_message={self.reduction_status_message},"
            f" reduction_inputs={self.reduction_inputs}, script_id={self.script_id},"
            f" reduction_outputs={self.reduction_outputs})>"
        )


class RunReduction(Base):  # type: ignore[valid-type, misc]
    """
    The RunReduction Table's declarative declaration
    """

    __tablename__ = "runs_reductions"
    run_id = Column(Integer, ForeignKey("runs.id"), primary_key=True)
    reduction_id = Column(Integer, ForeignKey("reductions.id"), primary_key=True)
    run_relationship: Mapped[Run] = relationship("Run", back_populates="reductions")
    reduction_relationship: Mapped[Reduction] = relationship("Reduction", back_populates="run_reduction_relationship")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RunReduction):
            return (self.run_id == other.run_id and self.reduction_id == other.reduction_id) or (
                self.run_relationship == other.run_relationship
                and self.reduction_relationship == other.reduction_relationship
            )
        return False

    def __repr__(self) -> str:
        return f"<RunReduction(run={self.run_id}, reduction={self.reduction_id})>"


class Instrument(Base):  # type: ignore[valid-type, misc]
    """
    The Instrument Table's declarative declaration
    """

    __tablename__ = "instruments"
    id = Column(Integer, primary_key=True)
    instrument_name = Column(String, unique=True)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Instrument):
            return self.instrument_name == other.instrument_name
        return False

    def __repr__(self) -> str:
        return f"<Instrument(id={self.id}, instrument_name={self.instrument_name})>"


class DBUpdater:
    """
    The class responsible for handling session state, and sending SQL queries via SQLAlchemy
    """

    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/fia"
        engine = create_engine(connection_string, poolclass=NullPool)
        self.session_maker_func = sessionmaker(bind=engine)

    # pylint: disable=too-many-arguments, too-many-locals
    def update_completed_run(
        self,
        db_reduction_id: int,
        state: State,
        status_message: str,
        output_files: List[str],
        reduction_start: str,
        reduction_end: str,
        stacktrace: str,
    ) -> None:
        """
        This function submits data to the database from what is initially available on completed-runs message broker
        station/topic
        :param db_reduction_id: The ID for the reduction row in the reduction table
        :param state: The state of how the run ended
        :param status_message: The message that accompanies the state for how the  state ended, if the state for
        example was unsuccessful or an error, it would have the reason/error message.
        :param output_files: The files output from the reduction job
        :param reduction_start: The time the pod running the reduction started working
        :param reduction_end: The time the pod running the reduction stopped working
        :return:
        """
        logger.info(
            "Updating completed-run in the database: {id: %s, state: %s, status_message: %s, output_files: %s,"
            " stacktrace: %s}",
            db_reduction_id,
            str(state),
            status_message,
            output_files,
            textwrap.shorten(stacktrace, width=20, placeholder="..."),
        )
        with self.session_maker_func() as session:
            reduction = session.query(Reduction).filter_by(id=db_reduction_id).one()
            reduction.reduction_state = state
            reduction.reduction_outputs = str(output_files)
            reduction.reduction_status_message = status_message
            reduction.stacktrace = stacktrace
            reduction.reduction_start = reduction_start
            reduction.reduction_end = reduction_end
            session.commit()
            logger.info(
                "Submitted completed-run to the database successfully: {id: %s, state: %s, "
                "status_message: %s, output_files: %s, stacktrace: %s}",
                db_reduction_id,
                str(state),
                status_message,
                output_files,
                textwrap.shorten(stacktrace, width=20, placeholder="..."),
            )


# pylint: enable=too-many-arguments, too-many-locals
