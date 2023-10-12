# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
import unittest
from pathlib import Path
from unittest import mock

import pytest

from test.utils import AwaitableNonAsyncMagicMock  # pylint: disable=wrong-import-order
from job_controller.main import JobController


class JobControllerTest(unittest.IsolatedAsyncioTestCase):
    @mock.patch("job_controller.main.DBUpdater")
    @mock.patch("job_controller.main.JobCreator")
    @mock.patch("job_controller.main.QueueConsumer")
    def setUp(self, _, __, ___):
        os.environ["RUNNER_SHA"] = "literally_anything"
        self.joc = JobController()

    def _test_queue_consumer_passed_vars(
        self, queue_consumer, queue_host, consumer_username, consumer_password, queue_name
    ):
        queue_consumer.assert_called_with(
            self.joc.on_message,
            queue_host=queue_host,
            username=consumer_username,
            password=consumer_password,
            queue_name=queue_name,
        )

    @mock.patch("job_controller.main.JobCreator")
    @mock.patch("job_controller.main.QueueConsumer")
    def test_job_controller_gets_various_vars_from_env(self, queue_consumer, __):
        self.joc = JobController()
        self.assertEqual(self.joc.ir_api_host, "ir-api-service.ir.svc.cluster.local:80")
        self.assertEqual(self.joc.reduce_user_id, "")

        self._test_queue_consumer_passed_vars(
            queue_consumer, queue_host="", consumer_username="", consumer_password="", queue_name=""
        )

        os.environ["IR_API"] = "fancy_ir_api_ip"
        os.environ["QUEUE_HOST"] = "random_ip_address_from_broke"
        os.environ["REDUCE_USER_ID"] = "reduceuser"
        os.environ["QUEUE_USER"] = "usernameforconsuming"
        os.environ["QUEUE_PASSWORD"] = "passwordforconsuming"
        os.environ["INGRESS_QUEUE_NAME"] = "queue_name"

        self.joc = JobController()
        self.assertEqual(self.joc.ir_api_host, "fancy_ir_api_ip")
        self.assertEqual(self.joc.reduce_user_id, "reduceuser")

        self._test_queue_consumer_passed_vars(
            queue_consumer,
            queue_host="random_ip_address_from_broke",
            consumer_username="usernameforconsuming",
            consumer_password="passwordforconsuming",
            queue_name="queue_name",
        )

        os.environ.pop("IR_API")
        os.environ.pop("QUEUE_HOST")
        os.environ.pop("REDUCE_USER_ID")
        os.environ.pop("QUEUE_USER")
        os.environ.pop("QUEUE_PASSWORD")
        os.environ.pop("INGRESS_QUEUE_NAME")

    @mock.patch("job_controller.main.JobCreator")
    @mock.patch("job_controller.main.QueueConsumer")
    def test_job_controller_gets_runner_sha_from_env(self, _, job_creator):
        runner_sha = mock.MagicMock()
        os.environ["RUNNER_SHA"] = str(runner_sha)

        JobController()

        job_creator.assert_called_once_with(runner_sha=str(runner_sha))

    @mock.patch("job_controller.main.JobCreator")
    @mock.patch("job_controller.main.QueueConsumer")
    def test_job_controller_raises_if_runner_sha_not_set(self, _, __):
        os.environ.pop("RUNNER_SHA")

        self.assertRaises(OSError, JobController)

    @mock.patch("job_controller.main.JobWatcher")
    @mock.patch("job_controller.main.acquire_script")
    @mock.patch("job_controller.main.create_ceph_path")
    def test_on_message_calls_spawn_pod_with_message(self, create_ceph_path, acquire_script, _):
        message = mock.MagicMock()
        path = "/tmp/ceph/mari/RBNumber/RB000001/autoreduced"
        acquire_script.return_value = ("script", "hash")
        create_ceph_path.return_value = path
        if not os.path.exists(path):
            os.makedirs(path)

        self.joc.on_message(message)

        self.joc.job_creator.spawn_job.assert_called_once()
        self.assertIn(
            f"run-{Path(message['filepath']).stem}", self.joc.job_creator.spawn_job.call_args.kwargs["job_name"]
        )
        self.assertEqual(self.joc.job_creator.spawn_job.call_args.kwargs["script"], acquire_script.return_value[0])
        self.assertEqual(self.joc.job_creator.spawn_job.call_args.kwargs["ceph_path"], create_ceph_path.return_value)
        os.removedirs(path)

    @mock.patch("job_controller.main.acquire_script")
    @mock.patch("job_controller.main.create_ceph_path")
    @mock.patch("job_controller.job_watcher.load_kubernetes_config")
    def test_on_message_aquires_script_using_filename(self, _, __, acquire_script):
        message = mock.MagicMock()
        self.joc.ir_api_host = mock.MagicMock()

        self.joc.on_message(message)

        acquire_script.assert_called_once_with(
            ir_api_host=self.joc.ir_api_host,
            reduction_id=self.joc.db_updater.add_detected_run.return_value,
            instrument=message["instrument"],
        )

    @mock.patch("job_controller.main.acquire_script")
    @mock.patch("job_controller.main.create_ceph_path")
    def test_on_message_calls_create_ceph_path(self, create_ceph_path, _):
        message = mock.MagicMock()

        self.joc.on_message(message)

        create_ceph_path.assert_called_once_with(
            instrument_name=message["instrument"], rb_number=message["experiment_number"]
        )

    @mock.patch("job_controller.main.acquire_script")
    @mock.patch("job_controller.main.create_ceph_path")
    @mock.patch("job_controller.main.ensure_ceph_path_exists")
    def test_on_message_sends_the_job_to_the_job_watch(self, ensure_ceph_path_exists, _, acquire_script):
        message = mock.MagicMock()
        job_name = mock.MagicMock()
        pv_name = mock.MagicMock()
        pvc_name = mock.MagicMock()
        self.joc.job_creator.spawn_job = mock.MagicMock(return_value=(job_name, pv_name, pvc_name))
        self.joc.create_job_watcher = mock.MagicMock()
        acquire_script.return_value = ("script", "hash")

        self.joc.on_message(message)

        self.joc.create_job_watcher.assert_called_once_with(
            job_name,
            pv_name,
            pvc_name,
            ensure_ceph_path_exists.return_value,
            self.joc.db_updater.add_detected_run.return_value,
            acquire_script.return_value[0],
            acquire_script.return_value[1],
            message["additional_values"],
        )

    @mock.patch("job_controller.main.logger")
    def test_ensure_exceptions_are_caught_and_logged_using_the_logger_in_on_message(self, logger):
        exception = Exception("Super cool exception message")
        message = mock.MagicMock()

        def exception_side_effect(*_, **__):
            raise exception

        with mock.patch("job_controller.main.create_ceph_path", side_effect=exception_side_effect):
            self.joc.on_message(message)

        logger.exception.assert_called_once_with(exception)

    @mock.patch("job_controller.main.JobWatcher")
    @mock.patch("job_controller.main.threading")
    def test_create_job_watchers_spins_off_new_thread_with_jobwatcher(self, threading, job_watcher):
        self.joc.create_job_watcher(
            job_name="job",
            pv="pv",
            pvc="pvc",
            ceph_path="/path/to/ceph/folder/for/output",
            db_reduction_id=1,
            job_script="print('script')",
            script_sha="some sha",
            reduction_inputs={},
        )

        threading.Thread.assert_called_once_with(target=job_watcher.return_value.watch)
        threading.Thread.return_value.start.assert_called_once_with()

    def test_run_class_starts_consuming(self):
        self.joc.consumer = mock.MagicMock()

        self.joc.run()

        self.joc.consumer.start_consuming.assert_called_once_with()
