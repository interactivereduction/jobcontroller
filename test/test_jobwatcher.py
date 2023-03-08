# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import unittest
from unittest import mock

from jobcontroller.jobwatcher import JobWatcher


class JobWatcherTest(unittest.TestCase):
    @mock.patch("jobcontroller.jobwatcher.load_kubernetes_config")
    def setUp(self, _):
        self.job_name = mock.MagicMock()
        self.namespace = mock.MagicMock()
        self.kafka_ip = mock.MagicMock()
        self.ceph_path = mock.MagicMock()
        self.jobw = JobWatcher(
            job_name=self.job_name, namespace=self.namespace, kafka_ip=self.kafka_ip, ceph_path=self.ceph_path
        )

    @mock.patch("jobcontroller.jobwatcher.load_kubernetes_config")
    def test_ensure_init_load_kube_config(self, load_kube_config):
        JobWatcher("", "", "", "")

        load_kube_config.assert_called_once_with()

    @mock.patch("jobcontroller.jobwatcher.client")
    def test_grab_pod_name_filters_all_pods_in_namespace_against_passed_job_name(self, k8s_client):
        output = mock.MagicMock()
        owner = mock.MagicMock()
        owner.name = self.job_name
        pod = mock.MagicMock()
        pod.metadata.owner_references = [owner]
        pod.metadata.name = output
        k8s_client.CoreV1Api.return_value.list_namespaced_pod.return_value.items = [pod]

        return_value = self.jobw.grab_pod_name_from_job_name_in_namespace(self.job_name, self.namespace)

        self.assertEqual(return_value, output)

    @mock.patch("jobcontroller.jobwatcher.logger")
    @mock.patch("jobcontroller.jobwatcher.watch")
    @mock.patch("jobcontroller.jobwatcher.client")
    def test_watch_handles_exceptions_from_code_handling_events(self, k8s_client, k8s_watch, logger):
        v1 = k8s_client.BatchV1Api.return_value
        watch_ = k8s_watch.Watch.return_value

        def raise_exception(_):
            raise Exception("EVERYTHING IS ON FIRE")

        self.jobw.process_event = mock.MagicMock(side_effect=raise_exception)
        event = mock.MagicMock()
        watch_.stream.return_value = [event]

        self.jobw.watch()

        watch_.stream.assert_called_once_with(v1.list_job_for_all_namespaces)
        logger.error.assert_called_once_with(
            "Job watching failed due to an exception: %s", str(Exception("EVERYTHING IS ON FIRE"))
        )

    @mock.patch("jobcontroller.jobwatcher.watch")
    @mock.patch("jobcontroller.jobwatcher.client")
    def test_watch_analyzes_events_from_watch_stream(self, k8s_client, k8s_watch):
        v1 = k8s_client.BatchV1Api.return_value
        watch_ = k8s_watch.Watch.return_value
        self.jobw.process_event = mock.MagicMock()
        event = mock.MagicMock()
        watch_.stream.return_value = [event]

        self.jobw.watch()

        watch_.stream.assert_called_once_with(v1.list_job_for_all_namespaces)
        self.jobw.process_event.assert_called_once_with(event)

    def test_process_event_on_success_calls_success(self):
        event = mock.MagicMock()
        self.jobw.job_name = "mari0-asfn"
        event.__getitem__.return_value.metadata.name = "mari0-asfn-132"
        event.__getitem__.return_value.status.succeeded = 1
        self.jobw.process_event_success = mock.MagicMock()

        self.jobw.process_event(event)

        self.jobw.process_event_success.assert_called_once_with()

    def test_process_event_on_failures_calls_failure(self):
        event = mock.MagicMock()
        self.jobw.job_name = "mari0-asfn"
        event.__getitem__.return_value.metadata.name = "mari0-asfn-132"
        event.__getitem__.return_value.status.failed = 1
        self.jobw.process_event_failed = mock.MagicMock()

        self.jobw.process_event(event)

        self.jobw.process_event_failed.assert_called_once_with(event.__getitem__.return_value)

    @mock.patch("jobcontroller.jobwatcher.client")
    def test_process_event_success_grabs_pod_name_using_grab_pod_name_from_job_name_in_namespace(self, _):
        self.jobw.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")
        self.jobw.notify_kafka = mock.MagicMock()

        self.jobw.process_event_success()

        self.jobw.grab_pod_name_from_job_name_in_namespace.assert_called_once_with(
            job_name=self.job_name, job_namespace=self.namespace
        )

    @mock.patch("jobcontroller.jobwatcher.client")
    def test_process_event_success_grabs_pod_name_using_grab_pod_name_from_job_name_in_namespace_raises_when_none(
        self, _
    ):
        self.jobw.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value=None)
        self.jobw.notify_kafka = mock.MagicMock()

        self.assertRaises(TypeError, self.jobw.process_event_success)

        self.jobw.grab_pod_name_from_job_name_in_namespace.assert_called_once_with(
            job_name=self.job_name, job_namespace=self.namespace
        )

    @mock.patch("jobcontroller.jobwatcher.client")
    def test_process_event_success_passed_penultimate_log_line_to_notify_kafka_as_data(self, k8s_client):
        self.jobw.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")
        self.jobw.notify_kafka = mock.MagicMock()
        k8s_client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = (
            '4th to last\n3rd to last\n{"status": "Success", "output_files": [], "status_message": ""}\n'
        )

        self.jobw.process_event_success()

        self.jobw.grab_pod_name_from_job_name_in_namespace.assert_called_once_with(
            job_name=self.job_name, job_namespace=self.namespace
        )
        self.jobw.notify_kafka.assert_called_once_with(status="Success", status_message="", output_files=[])

    @mock.patch("jobcontroller.jobwatcher.client")
    def test_process_event_success_handles_errors_where_penultimate_line_of_logs_is_not_valid_json(self, k8s_client):
        self.jobw.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")
        self.jobw.notify_kafka = mock.MagicMock()
        k8s_client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = (
            '4th to last\n3rd to last\n{"status": Not valid json, "output_files": [], ' '"status_message": ""}\n'
        )

        self.jobw.process_event_success()

        self.jobw.grab_pod_name_from_job_name_in_namespace.assert_called_once_with(
            job_name=self.job_name, job_namespace=self.namespace
        )
        self.jobw.notify_kafka.assert_called_once_with(
            status="Unsuccessful", status_message="Expecting value: line 1 column 12 (char 11)", output_files=[]
        )

    def test_process_event_failed_notifies_kafka(self):
        self.jobw.notify_kafka = mock.MagicMock()
        job = mock.MagicMock()
        job.status.message = "Status message"

        self.jobw.process_event_failed(job)

        self.jobw.notify_kafka.assert_called_once_with(status="Error", status_message="Status message")

    @mock.patch("jobcontroller.jobwatcher.add_ceph_path_to_output_files")
    @mock.patch("jobcontroller.jobwatcher.Producer")
    def test_notify_kafka_converts_output_files_to_ceph_paths(self, _, add_ceph_path_to_output_files):
        self.jobw.notify_kafka("", "", ["/path"])

        add_ceph_path_to_output_files.assert_called_once_with(ceph_path=self.ceph_path, output_files=["/path"])

    @mock.patch("jobcontroller.jobwatcher.Producer")
    def test_notify_kafka_produces_a_message_using_passed_data_for_success(self, producer):
        self.jobw.ceph_path = "/ceph/path/here/"
        value = "{\"status\": \"Successful\", \"run output\": [\"/ceph/path/here/path\"]}"

        self.jobw.notify_kafka("Successful", "", ["path"])

        producer.return_value.produce.assert_called_once_with("completed-runs", value=value,
                                                              callback=self.jobw._delivery_callback)

    @mock.patch("jobcontroller.jobwatcher.Producer")
    def test_notify_kafka_produces_a_message_using_passed_data_for_error(self, producer):
        self.jobw.ceph_path = "/ceph/path/here/"
        value = "{\"status\": \"Error\", \"status message\": \"Status message\"}"

        self.jobw.notify_kafka("Error", "Status message", [])

        producer.return_value.produce.assert_called_once_with("completed-runs", value=value,
                                                              callback=self.jobw._delivery_callback)

    @mock.patch("jobcontroller.jobwatcher.Producer")
    def test_notify_kafka_produces_a_message_using_passed_data_for_other(self, producer):
        self.jobw.ceph_path = "/ceph/path/here/"
        value = "{\"status\": \"ANYTHING ELSE\", \"status message\": \"Status message\"}"

        self.jobw.notify_kafka("ANYTHING ELSE", "Status message", [])

        producer.return_value.produce.assert_called_once_with("completed-runs", value=value,
                                                              callback=self.jobw._delivery_callback)
