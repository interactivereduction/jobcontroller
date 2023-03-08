# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
import unittest
from unittest import mock

from jobcontroller.jobcontroller import JobController


class JobControllerTest(unittest.TestCase):
    @mock.patch("jobcontroller.jobcontroller.K8sAPI")
    @mock.patch("jobcontroller.jobcontroller.TopicConsumer")
    def setUp(self, _, __):
        self.joc = JobController()

    @mock.patch("jobcontroller.jobcontroller.K8sAPI")
    @mock.patch("jobcontroller.jobcontroller.TopicConsumer")
    def test_job_controller_gets_kafka_ip_from_env(self, _, __):
        self.assertEqual(self.joc.kafka_ip, "broker")

        os.environ["KAFKA_IP"] = "random_ip_address_from_kafka"

        self.assertEqual(JobController().kafka_ip, "random_ip_address_from_kafka")

        os.environ.pop("KAFKA_IP")

    @mock.patch("jobcontroller.jobcontroller.aquire_script")
    @mock.patch("jobcontroller.jobcontroller.create_ceph_path")
    def test_on_message_calls_spawn_pod_with_message(self, create_ceph_path, aquire_script):
        message = mock.MagicMock()

        self.joc.on_message(message)

        self.joc.job_creator.spawn_job.assert_called_once_with(
            job_name=f"run-{os.path.basename(message['filepath'])}",
            script=aquire_script.return_value,
            ceph_path=create_ceph_path.return_value,
        )

    @mock.patch("jobcontroller.jobcontroller.aquire_script")
    @mock.patch("jobcontroller.jobcontroller.create_ceph_path")
    def test_on_message_aquires_script_using_filename(self, _, aquire_script):
        message = mock.MagicMock()
        self.joc.ir_api_ip = mock.MagicMock()

        self.joc.on_message(message)

        aquire_script.assert_called_once_with(
            filename=os.path.basename(message["filepath"]), ir_api_ip=self.joc.ir_api_ip
        )

    @mock.patch("jobcontroller.jobcontroller.aquire_script")
    @mock.patch("jobcontroller.jobcontroller.create_ceph_path")
    def test_on_message_calls_create_ceph_path(self, create_ceph_path, _):
        message = mock.MagicMock()

        self.joc.on_message(message)

        create_ceph_path.assert_called_once_with(
            instrument_name=message["instrument"], rb_number=message["experiment_number"]
        )

    @mock.patch("jobcontroller.jobcontroller.aquire_script")
    @mock.patch("jobcontroller.jobcontroller.create_ceph_path")
    def test_on_message_sends_the_job_to_the_job_watch(self, create_ceph_path, __):
        message = mock.MagicMock()
        self.joc.create_job_watcher = mock.MagicMock()

        self.joc.on_message(message)

        self.joc.create_job_watcher.assert_called_once_with(
            self.joc.job_creator.spawn_job.return_value, create_ceph_path.return_value
        )

    @mock.patch("jobcontroller.jobcontroller.logger")
    def test_ensure_exceptions_are_caught_and_logged_using_the_logger_in_on_message(self, logger):
        exception = Exception("Super cool exception message")
        message = mock.MagicMock()

        def exception_side_effect(*_, **__):
            raise exception

        self.joc.job_creator.spawn_job = mock.MagicMock(side_effect=exception_side_effect)

        self.joc.on_message(message)

        logger.exception.assert_called_once_with(exception)

    @mock.patch("jobcontroller.jobcontroller.JobWatcher")
    @mock.patch("jobcontroller.jobcontroller.threading")
    def test_create_job_watchers_spins_off_new_thread_with_jobwatcher(self, threading, job_watcher):
        self.joc.create_job_watcher(job_name="job", ceph_path="/path/to/ceph/folder/for/output")

        threading.Thread.assert_called_once_with(target=job_watcher.return_value.watch)
        threading.Thread.return_value.start.assert_called_once_with()

    def test_run_class_starts_consuming(self):
        self.joc.run()

        self.joc.consumer.start_consuming.assert_called_once_with()
