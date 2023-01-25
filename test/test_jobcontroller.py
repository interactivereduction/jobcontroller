"""
The tests for the jobcontroller class
"""
import unittest
from unittest import mock

from jobcontroller.jobcontroller import JobController


class JobControllerTest(unittest.TestCase):
    """
    The unittest test class
    """

    @mock.patch("jobcontroller.jobcontroller.K8sAPI")
    @mock.patch("jobcontroller.jobcontroller.TopicConsumer")
    def setUp(self, _, __):
        self.joc = JobController()

    def test_on_message_calls_spawn_pod_with_message(self):
        message = mock.MagicMock()

        self.joc.on_message(message)

        self.joc.k8s.spawn_pod.assert_called_once_with(message)

    def test_run_class_start_consuming(self):
        self.joc.run()

        self.joc.consumer.start_consuming.assert_called_once_with()
