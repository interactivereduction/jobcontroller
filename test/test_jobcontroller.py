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

    def test_on_message_calls_spawn_pod_with_message(self):
        message = mock.MagicMock()
        kafka_ip = mock.MagicMock()
        self.joc.kafka_ip = kafka_ip

        self.joc.on_message(message)

        self.joc.k8s.spawn_pod.assert_called_once_with(
            filename=os.path.basename(message["filename"]), kafka_ip=kafka_ip,
            rb_number=message["rb_number"], instrument_name=message["instrument_name"])

    def test_run_class_start_consuming(self):
        self.joc.run()

        self.joc.consumer.start_consuming.assert_called_once_with()
