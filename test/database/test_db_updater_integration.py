"""
DB updater integration tests. Requires a postgres database at localhost:5432 with password: password
"""
# pylint: disable=redefined-outer-name, too-many-arguments, redefined-argument-from-local,
from datetime import datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload

from job_controller.database.db_updater import DBUpdater, Base, Instrument, Reduction, Run, RunReduction
from job_controller.database.state_enum import State

ENGINE = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/interactive-reduction")


@pytest.fixture(autouse=True)
def setup():
    """Fixture to drop and recreate all tables in the database before each test."""
    Base.metadata.drop_all(ENGINE)
    Base.metadata.create_all(ENGINE)


@pytest.fixture
def db_updater():
    """Fixture to create a new DBUpdater instance."""
    return DBUpdater("localhost", "postgres", "password")


@pytest.fixture()
def session():
    """Fixture to create a new session factory for the database."""
    return sessionmaker(ENGINE)


START = datetime.now()
END = datetime.now()


@pytest.fixture
def instrument_fix():
    """Instrument fixture"""
    return Instrument(instrument_name="mari", id=1)


@pytest.fixture
def run_fix():
    """
    Run fixture
    """
    return Run(
        filename="foo.nxs",
        experiment_number=123,
        title="title",
        users="user 1, user 2",
        run_start=START,
        run_end=END,
        good_frames=1,
        raw_frames=10,
        id=1,
        instrument_id=1,
    )


@pytest.fixture
def run_reduction_fix():
    """RunReduction fixture"""
    return RunReduction(run_id=1, reduction_id=1)


@pytest.fixture
def reduction_fix():
    """Reduction fixture"""
    return Reduction(
        reduction_start=None,
        reduction_end=None,
        reduction_state=None,
        reduction_status_message=None,
        reduction_inputs={"ei": "auto"},
        id=1,
    )


def test_add_detected_run_when_instrument_not_found(
    db_updater, session, instrument_fix, run_fix, reduction_fix, run_reduction_fix
):
    """
    Test the add_detected_run method when the instrument is not found in the database.
    It should add the instrument, reduction, run, and run_reduction to the database.
    """
    db_updater.add_detected_run(
        "foo.nxs", "title", "mari", "user 1, user 2", "123", str(START), str(END), "1", "10", {"ei": "auto"}
    )

    with session() as session:
        instrument = session.execute(select(Instrument).filter(Instrument.id == 1)).scalars().one()
        reduction = session.execute(select(Reduction).filter(Reduction.id == 1)).scalars().one()
        run = session.execute(select(Run).filter(Run.id == 1)).scalars().one()
        run_reduction = session.execute(select(RunReduction).filter(RunReduction.run_id == 1)).scalars().one()

    assert instrument_fix == instrument
    assert reduction_fix == reduction
    assert run_fix == run
    assert run_reduction_fix == run_reduction


def test_add_detected_run_when_instrument_exists(
    db_updater, session, instrument_fix, run_fix, reduction_fix, run_reduction_fix
):
    """
    Test the add_detected_run method when the instrument already exists in the database.
    It should add the reduction, run, and run_reduction to the database without adding a new instrument.
    """
    with session() as session_:
        session_.add(instrument_fix)
        session_.commit()
        session_.refresh(instrument_fix)

    db_updater.add_detected_run(
        "foo.nxs", "title", "mari", "user 1, user 2", "123", str(START), str(END), "1", "10", {"ei": "auto"}
    )

    with session() as session_:
        instrument = session_.execute(select(Instrument).filter(Instrument.id == 1)).scalars().one()
        reduction = session_.execute(select(Reduction).filter(Reduction.id == 1)).scalars().one()
        run = session_.execute(select(Run).filter(Run.id == 1)).scalars().one()
        run_reduction = (
            session_.execute(select(RunReduction).filter((RunReduction.run_id == 1) & (RunReduction.reduction_id == 1)))
            .scalars()
            .one()
        )

    assert instrument_fix == instrument
    assert reduction_fix == reduction
    assert run_fix == run
    assert run_reduction_fix == run_reduction


def test_add_completed_run(db_updater, session, run_fix, reduction_fix):
    """
    Test the add_completed_run method. It should update the reduction's inputs, state, status message,
    outputs, and associated script in the database.
    """
    with session() as session_:
        run_fix.instrument = Instrument(instrument_name="foo")
        session_.add_all([run_fix, reduction_fix, reduction_fix])
        session_.commit()

    db_updater.add_completed_run(
        1,
        {"ei": "auto"},
        State.SUCCESSFUL,
        "status message",
        ["file 1", "file 2"],
        "print()",
        "2023-04-24 14:50:11.000000",
        "2023-04-24 14:50:12.000000",
    )

    with session() as session_:
        reduction = (
            session_.execute(select(Reduction).options(joinedload(Reduction.script)).where(Reduction.id == 1))
            .scalars()
            .one()
        )

    assert reduction.reduction_inputs == {"ei": "auto"}
    assert reduction.reduction_state == State.SUCCESSFUL
    assert reduction.reduction_status_message == "status message"
    assert reduction.reduction_outputs == "['file 1', 'file 2']"
    assert reduction.script.script == "print()"
    assert reduction.reduction_start == "2023-04-24 14:50:11.000000"
    assert reduction.reduction_end == "2023-04-24 14:50:12.000000"
