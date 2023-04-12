# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock
from mock_alchemy.mocking import UnifiedAlchemyMagicMock

from job_controller.database.db_updater import DBUpdater, RunReduction, Reduction, Run, Script


class DBUpdaterTests(unittest.TestCase):
    @mock.patch("job_controller.database.db_updater.sessionmaker")
    def setUp(self, session_maker) -> None:
        self.ip = mock.MagicMock()
        self.username = mock.MagicMock()
        self.password = mock.MagicMock()
        self.mock_session = mock.MagicMock()
        self.session_maker_func = mock.MagicMock(return_value=self.mock_session)
        self.db_updater = DBUpdater(self.ip, self.username, self.password)
        self.db_updater.session_maker_func = self.session_maker_func

    def test_add_detected_run(self):
        filename = mock.MagicMock()
        title = mock.MagicMock()
        experiment_number = mock.MagicMock()
        users = mock.MagicMock()
        run_start = mock.MagicMock()
        run_end = mock.MagicMock()
        good_frames = mock.MagicMock()
        raw_frames = mock.MagicMock()
        reduction_inputs = mock.MagicMock()

        self.db_updater.add_detected_run(filename=filename, title=title, users=users,
                                         experiment_number=experiment_number, run_start=run_start,
                                         run_end=run_end, good_frames=good_frames, raw_frames=raw_frames,
                                         reduction_inputs=reduction_inputs)

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
        self.assertEqual(self.mock_session.add.call_args_list[0][0][0], run)
        self.assertEqual(self.mock_session.add.call_args_list[1][0][0], reduction)
        self.assertEqual(self.mock_session.add.call_args_list[2][0][0],
                         RunReduction(run=run.id, reduction=reduction.id))
        self.assertEqual(self.mock_session.add.call_count, 3)
        self.mock_session.commit.assert_has_calls([
            mock.call(),
            mock.call()
        ])
        self.assertEqual(self.mock_session.commit.call_count, 2)

    def test_add_completed_run_when_script_found(self):
        db_reduction_id = mock.MagicMock()
        reduction_inputs = mock.MagicMock()
        state = mock.MagicMock()
        status_message = mock.MagicMock()
        output_files = mock.MagicMock()
        reduction_script = mock.MagicMock()

        self.db_updater.add_completed_run(db_reduction_id=db_reduction_id, reduction_inputs=reduction_inputs,
                                          state=state, status_message=status_message,
                                          output_files=output_files, reduction_script=reduction_script)

        reduction_mock = self.mock_session.query(Reduction).filter_by(id=db_reduction_id).one()
        self.assertEqual(reduction_mock.reduction_state, str(state))
        self.assertEqual(reduction_mock.reduction_inputs, reduction_inputs)
        self.assertEqual(reduction_mock.script, self.mock_session.query(Script)
                         .filter_by(script=reduction_script).first().id)
        self.assertEqual(reduction_mock.reduction_outputs, str(output_files))
        self.assertEqual(reduction_mock.reduction_status_message, status_message)

        self.mock_session.commit.assert_has_calls([
            mock.call()
        ])
        self.assertEqual(self.mock_session.commit.call_count, 1)


    def test_add_completed_run_when_script_not_found(self):
        db_reduction_id = mock.MagicMock()
        reduction_inputs = mock.MagicMock()
        state = mock.MagicMock()
        status_message = mock.MagicMock()
        output_files = mock.MagicMock()
        reduction_script = mock.MagicMock()
        self.mock_session.query(Script).filter_by(script=reduction_script).first.return_value = None

        self.db_updater.add_completed_run(db_reduction_id=db_reduction_id, reduction_inputs=reduction_inputs,
                                          state=state, status_message=status_message,
                                          output_files=output_files, reduction_script=reduction_script)

        script = Script(id=0, script=reduction_script)

        self.assertEqual(self.mock_session.add.call_args_list[0][0][0], script)
        self.assertEqual(self.mock_session.add.call_count, 1)

        reduction_mock = self.mock_session.query(Reduction).filter_by(id=db_reduction_id).one()
        self.assertEqual(reduction_mock.reduction_state, str(state))
        self.assertEqual(reduction_mock.reduction_inputs, reduction_inputs)
        self.assertEqual(reduction_mock.script, None)
        self.assertEqual(reduction_mock.reduction_outputs, str(output_files))
        self.assertEqual(reduction_mock.reduction_status_message, status_message)

        self.mock_session.commit.assert_has_calls([
            mock.call(),
            mock.call()
        ])
        self.assertEqual(self.mock_session.commit.call_count, 2)
