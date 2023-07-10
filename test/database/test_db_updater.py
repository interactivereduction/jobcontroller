# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
# pylint: disable=too-many-instance-attributes

import unittest
from unittest import mock

from job_controller.database.db_updater import DBUpdater, RunReduction, Reduction, Run, Script, Instrument


class DBUpdaterTests(unittest.TestCase):
    @mock.patch("job_controller.database.db_updater.sessionmaker")
    def setUp(self, _) -> None:
        self.ip = mock.MagicMock()
        self.username = mock.MagicMock()
        self.password = mock.MagicMock()
        self.mock_session = mock.MagicMock()
        self.session_maker_func = mock.MagicMock()
        self.session_maker_func.return_value.__enter__.return_value = self.mock_session
        self.db_updater = DBUpdater(self.ip, self.username, self.password)
        self.db_updater.session_maker_func = self.session_maker_func

    @mock.patch("job_controller.database.db_updater.Reduction")
    def test_add_detected_run_when_instrument_not_found(self, reduction_mock):
        filename = mock.MagicMock()
        title = mock.MagicMock()
        instrument_name = mock.MagicMock()
        experiment_number = mock.MagicMock()
        users = mock.MagicMock()
        run_start = mock.MagicMock()
        run_end = mock.MagicMock()
        good_frames = mock.MagicMock()
        raw_frames = mock.MagicMock()
        reduction_inputs = mock.MagicMock()
        self.mock_session.query(Instrument).filter_by(instrument_name=instrument_name).first = mock.MagicMock(
            return_value=None
        )

        self.db_updater.add_detected_run(
            filename=filename,
            title=title,
            instrument_name=instrument_name,
            users=users,
            experiment_number=experiment_number,
            run_start=run_start,
            run_end=run_end,
            good_frames=good_frames,
            raw_frames=raw_frames,
            reduction_inputs=reduction_inputs,
        )

        instrument = Instrument(instrument_name=instrument_name)

        run = Run(
            filename=filename,
            title=title,
            instrument=instrument,
            users=users,
            experiment_number=experiment_number,
            run_start=run_start,
            run_end=run_end,
            good_frames=good_frames,
            raw_frames=raw_frames,
        )
        self.assertEqual(
            self.mock_session.add.call_args_list[0][0][0],
            RunReduction(run_relationship=run, reduction_relationship=reduction_mock.return_value),
        )
        self.assertEqual(self.mock_session.add.call_count, 1)
        self.mock_session.commit.assert_has_calls([mock.call()])
        self.assertEqual(self.mock_session.commit.call_count, 1)

    @mock.patch("job_controller.database.db_updater.Reduction")
    def test_add_detected_run_when_instrument_found(self, reduction_mock):
        filename = mock.MagicMock()
        title = mock.MagicMock()
        instrument_name = mock.MagicMock()
        experiment_number = mock.MagicMock()
        users = mock.MagicMock()
        run_start = mock.MagicMock()
        run_end = mock.MagicMock()
        good_frames = mock.MagicMock()
        raw_frames = mock.MagicMock()
        reduction_inputs = mock.MagicMock()

        self.db_updater.add_detected_run(
            filename=filename,
            title=title,
            instrument_name=instrument_name,
            users=users,
            experiment_number=experiment_number,
            run_start=run_start,
            run_end=run_end,
            good_frames=good_frames,
            raw_frames=raw_frames,
            reduction_inputs=reduction_inputs,
        )

        run = Run(
            filename=filename,
            title=title,
            instrument=self.mock_session.query(Instrument).filter_by(instrument_name=instrument_name).first().id,
            users=users,
            experiment_number=experiment_number,
            run_start=run_start,
            run_end=run_end,
            good_frames=good_frames,
            raw_frames=raw_frames,
        )
        self.assertEqual(
            self.mock_session.add.call_args_list[0][0][0],
            RunReduction(run_relationship=run, reduction_relationship=reduction_mock.return_value),
        )
        self.assertEqual(self.mock_session.add.call_count, 1)
        self.mock_session.commit.assert_has_calls([mock.call()])
        self.assertEqual(self.mock_session.commit.call_count, 1)

    def test_add_completed_run_when_script_found(self):
        db_reduction_id = mock.MagicMock()
        reduction_inputs = mock.MagicMock()
        state = mock.MagicMock()
        status_message = mock.MagicMock()
        output_files = mock.MagicMock()
        reduction_script = mock.MagicMock()
        reduction_start = mock.MagicMock()
        reduction_end = mock.MagicMock()
        reduction_logs = mock.MagicMock()

        self.db_updater.add_completed_run(
            db_reduction_id=db_reduction_id,
            reduction_inputs=reduction_inputs,
            state=state,
            status_message=status_message,
            output_files=output_files,
            reduction_script=reduction_script,
            reduction_start=reduction_start,
            reduction_end=reduction_end,
            reduction_logs=reduction_logs
        )

        reduction_mock = self.mock_session.query(Reduction).filter_by(id=db_reduction_id).one()
        self.assertEqual(reduction_mock.reduction_state, state)
        self.assertEqual(reduction_mock.reduction_inputs, reduction_inputs)
        self.assertEqual(
            reduction_mock.script,
            self.mock_session.query(Script).filter_by(script=reduction_script).first(),
        )
        self.assertEqual(reduction_mock.reduction_outputs, str(output_files))
        self.assertEqual(reduction_mock.reduction_status_message, status_message)
        self.assertEqual(reduction_mock.reduction_start, reduction_start)
        self.assertEqual(reduction_mock.reduction_end, reduction_end)
        self.assertEqual(reduction_mock.logs, reduction_logs)

        self.mock_session.commit.assert_has_calls([mock.call()])
        self.assertEqual(self.mock_session.commit.call_count, 1)

    def test_add_completed_run_when_script_not_found(self):
        db_reduction_id = mock.MagicMock()
        reduction_inputs = mock.MagicMock()
        state = mock.MagicMock()
        status_message = mock.MagicMock()
        output_files = mock.MagicMock()
        reduction_script = mock.MagicMock()
        reduction_start = mock.MagicMock()
        reduction_end = mock.MagicMock()
        reduction_logs = mock.MagicMock()
        self.mock_session.query(Script).filter_by(script=reduction_script).first.return_value = None

        self.db_updater.add_completed_run(
            db_reduction_id=db_reduction_id,
            reduction_inputs=reduction_inputs,
            state=state,
            status_message=status_message,
            output_files=output_files,
            reduction_script=reduction_script,
            reduction_start=reduction_start,
            reduction_end=reduction_end,
            reduction_logs=reduction_logs
        )

        script = Script(script=reduction_script)

        reduction_mock = self.mock_session.query(Reduction).filter_by(id=db_reduction_id).one()
        self.assertEqual(reduction_mock.reduction_state, state)
        self.assertEqual(reduction_mock.reduction_inputs, reduction_inputs)
        self.assertEqual(reduction_mock.script, script)
        self.assertEqual(reduction_mock.reduction_outputs, str(output_files))
        self.assertEqual(reduction_mock.reduction_status_message, status_message)
        self.assertEqual(reduction_mock.reduction_start, reduction_start)
        self.assertEqual(reduction_mock.reduction_end, reduction_end)
        self.assertEqual(reduction_mock.logs, reduction_logs)

        self.mock_session.commit.assert_has_calls([mock.call()])
        self.assertEqual(self.mock_session.commit.call_count, 1)
