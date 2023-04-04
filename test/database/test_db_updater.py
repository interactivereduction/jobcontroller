# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock
from mock_alchemy.mocking import UnifiedAlchemyMagicMock

from job_controller.database.db_updater import DBUpdater, RunReduction, Reduction, Run


class DBUpdaterTests(unittest.TestCase):
    @mock.patch("job_controller.database.db_updater.sessionmaker")
    def setUp(self, session_maker) -> None:
        self.ip = mock.MagicMock()
        self.username = mock.MagicMock()
        self.password = mock.MagicMock()
        self.mock_session = UnifiedAlchemyMagicMock()
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

        self.db_updater.add_detected_run(
            filename=filename,
            title=title,
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
        self.assertEqual(
            self.mock_session.add.call_args_list[2][0][0], RunReduction(run=run.id, reduction=reduction.id)
        )
        self.assertEqual(self.mock_session.add.call_count, 3)
        self.mock_session.commit.assert_has_calls([mock.call(), mock.call()])
        self.assertEqual(self.mock_session.commit.call_count, 2)

    def test_add_completed_run_when_script_found(self):
        pass

    def test_add_completed_run_when_script_not_found(self):
        pass
