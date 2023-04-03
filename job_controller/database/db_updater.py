from typing import Any, Dict, List

from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, QueuePool
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from database.state_enum import State

Base = declarative_base()


class Run(Base):
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


class Script(Base):
    __tablename__ = "scripts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    script = Column(String)
    reductions = relationship("Reduction", back_populates="script_relationship")


class Reduction(Base):
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


class RunReduction(Base):
    __tablename__ = "runs_reductions"
    run = Column(Integer, ForeignKey("runs.id"), primary_key=True)
    reduction = Column(Integer, ForeignKey("reductions.id"), primary_key=True)
    run_relationship = relationship("Run", back_populates="reductions")
    reduction_relationship = relationship("Reduction", back_populates="run_reduction_relationship")


class DBUpdater:
    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/interactive-reduction"
        engine = create_engine(connection_string, poolclass=QueuePool, pool_size=20)
        self.session_maker_func = sessionmaker(bind=engine)
        self.runs_table = Run()
        self.reductions_table = Reduction()
        self.runs_reductions_table = RunReduction()
        self.script_table = Script()

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
    ) -> int:
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

        return reduction.id

    def add_completed_run(
        self,
        db_reduction_id: int,
        reduction_inputs: Dict[str, Any],
        state: State,
        status_message: str,
        output_files: List[str],
        reduction_script,
    ):
        session = self.session_maker_func()
        script = Script(script=reduction_script)
        session.add(script)
        session.commit()

        reduction = session.query(Reduction).filter_by(id=db_reduction_id).first()
        reduction.reduction_state = str(state)
        reduction.reduction_inputs = reduction_inputs
        reduction.script = script.id
        reduction.reduction_outputs = str(output_files)
        reduction.reduction_status_message = status_message
        session.commit()
