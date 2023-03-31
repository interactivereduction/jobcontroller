from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, QueuePool
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from database.state_enum import State

Base = declarative_base()


class Run(Base):
    __tablename__ = 'runs'
    id = Column(Integer, primary_key=True)
    filename = Column(String)
    title = Column(String)
    users = Column(String)
    experiment_number = Column(Integer)
    run_start = Column(DateTime)
    run_end = Column(DateTime)
    good_frames = Column(Integer)
    raw_frames = Column(Integer)
    reductions = relationship('RunReduction', back_populates='run')


class Script(Base):
    __tablename__ = 'scripts'
    id = Column(Integer, primary_key=True)
    script = Column(String)
    reductions = relationship('Reduction', back_populates='script')


class Reduction(Base):
    __tablename__ = 'reductions'
    id = Column(Integer, primary_key=True)
    reduction_start = Column(DateTime)
    reduction_end = Column(DateTime)
    reduction_state = Column(State)
    reduction_inputs = Column(JSONB)
    script_id = Column(Integer, ForeignKey('scripts.id'))
    script = relationship('Script', back_populates='reductions')
    reduction_outputs = Column(String)


class RunReduction(Base):
    __tablename__ = 'runs_reductions'
    run_id = Column(Integer, ForeignKey('runs.id'), primary_key=True)
    reduction_id = Column(Integer, ForeignKey('reductions.id'), primary_key=True)
    run = relationship('Run', back_populates='reductions')
    reduction = relationship('Reduction')


class DBUpdater:
    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/interactive-reduction"
        engine = create_engine(connection_string, poolclass=QueuePool, pool_size=20)
        self.session_maker_func = sessionmaker(bind=engine)
        self.runs_table = Run()
        self.reductions_table = Reduction()
        self.runs_reductions_table = RunReduction()
        self.script_table = Script()

    class SessionContext:
        def __init__(self, db_updater):
            self.db_updater = db_updater
            self.session = None
            self.items_to_add = []

        def __enter__(self):
            self.session = self.db_updater.session_maker_func()

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.items_to_add is not []:
                for item in self.items_to_add:
                    self.session.add(item)
                    self.session.commit()

    def add_detected_run(self, filename: str, title: str, users: str, experiment_number: int, run_start: str,
                         run_end: str, good_frames: str, raw_frames: str):
        with self.SessionContext(self) as session:
            session.items_to_add.append(Run(filename=filename, title=title, users=users,
                                            experiment_number=experiment_number, run_start=run_start, run_end=run_end,
                                            good_frames=good_frames, raw_frames=raw_frames))

    def add_completed_run(self):
        pass
