import unittest
from unittest import mock

from jobcontroller.jobcontroller import JobController


class JobControllerTest(unittest.TestCase):
    @mock.patch("jobcontroller.jobcontroller.K8sAPI")
    @mock.patch("jobcontroller.jobcontroller.TopicConsumer")
    def setUp(self, _, __):
        self.jc = JobController()

    def test_on_message_calls_spawn_pod_with_message(self):
        message = mock.MagicMock()

        self.jc.on_message(message)

        self.jc.k8s.spawn_pod.assert_called_once_with(message)

    def test_run_class_start_consuming(self):
        self.jc.run()

        self.jc.consumer.start_consuming.assert_called_once_with()
