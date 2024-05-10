"""
This module is responsible for holding the SQL Classes that SQLAlchemy will use and then formatting the SQL queries
via SQLAlchemy via pre-made functions.
"""

from __future__ import annotations

import hashlib
import textwrap
from typing import Any, Dict, List

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

from jobcreator.database.state_enum import State
from jobcreator.utils import logger

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
    script = Column(String)
    sha = Column(String, nullable=True)
    script_hash = Column(String, nullable=True)
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


def create_hash_of_script(script: str) -> str:
    """
    Create a hash of the script that in theory is unique
    :param script: str, The script to be hashed
    :return: str, a sha512 of the passed in script
    """
    return hashlib.sha512(script.encode()).hexdigest()


class DBUpdater:
    """
    The class responsible for handling session state, and sending SQL queries via SQLAlchemy
    """

    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/fia"
        engine = create_engine(connection_string, poolclass=NullPool)
        self.session_maker_func = sessionmaker(bind=engine)

    # pylint: disable=too-many-arguments, too-many-locals
    def add_detected_run(
        self,
        filename: str,
        title: str,
        instrument_name: str,
        users: str,
        experiment_number: str,
        run_start: str,
        run_end: str,
        good_frames: str,
        raw_frames: str,
        reduction_inputs: Dict[str, Any],
    ) -> int:
        """
        This function submits data to the database from what is initially available on detected-runs message broker
        station/topic
        :param filename: the filename of the run that needs to be reduced
        :param title: The title of the run file
        :param instrument_name: The name of the instrument for the run
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
            "Submitting detected-run to the database: {filename: %s, title: %s, instrument_name: %s, users: %s, "
            "experiment_number: %s, run_start: %s, run_end: %s, good_frames: %s, raw_frames: %s, "
            "reduction_inputs: %s}",
            filename,
            title,
            instrument_name,
            users,
            experiment_number,
            run_start,
            run_end,
            good_frames,
            raw_frames,
            reduction_inputs,
        )
        with self.session_maker_func() as session:
            instrument = session.query(Instrument).filter_by(instrument_name=instrument_name).first()
            if instrument is None:
                instrument = Instrument(instrument_name=instrument_name)

            run = session.query(Run).filter_by(filename=filename).first()
            if run is None:
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
                run.instrument = instrument

            reduction = Reduction(
                reduction_start=None,
                reduction_end=None,
                reduction_state=State.NOT_STARTED,
                reduction_inputs=reduction_inputs,
                script_id=None,
                reduction_outputs=None,
            )
            # Now create the run_reduction entry and add it
            run_reduction = RunReduction(run_relationship=run, reduction_relationship=reduction)
            session.add(run_reduction)
            session.commit()

            logger.info(
                "Submitted detected-run to the database successfully: {filename: %s, title: %s, instrument_name: %s, "
                "users: %s, experiment_number: %s, run_start: %s, run_end: %s, good_frames: %s, raw_frames: %s, "
                "reduction_inputs: %s}",
                filename,
                title,
                instrument_name,
                users,
                experiment_number,
                run_start,
                run_end,
                good_frames,
                raw_frames,
                reduction_inputs,
            )

            return int(reduction.id)

    def update_script(self, db_reduction_id: int, reduction_script: str, script_sha: str) -> None:
        """
        Updates the script tied to a reduction in the DB
        :param db_reduction_id: The ID for the reduction to be updated
        :param reduction_script: The contents of the script to be added
        :param script_sha: The sha of that script
        :return:
        """
        logger.info(
            "Submitting script to the database: {db_reduction_id: %s, reduction_script: %s, script_sha: %s}",
            db_reduction_id,
            textwrap.shorten(reduction_script, width=10, placeholder="..."),
            script_sha,
        )
        with self.session_maker_func() as session:
            script_hash = create_hash_of_script(reduction_script)
            script = session.query(Script).filter_by(script_hash=script_hash).first()
            if script is None:
                script = Script(script=reduction_script, sha=script_sha, script_hash=script_hash)
            reduction = session.query(Reduction).filter_by(id=db_reduction_id).one()
            reduction.script = script
            session.commit()
            logger.info(
                "Submitted script to the database: {db_reduction_id: %s, reduction_script: %s, script_sha: %s}",
                db_reduction_id,
                textwrap.shorten(reduction_script, width=10, placeholder="..."),
                script_sha,
            )


# pylint: enable=too-many-arguments, too-many-locals
