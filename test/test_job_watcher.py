# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
# pylint: disable=too-many-instance-attributes
import unittest
from unittest import mock

from job_controller.database.state_enum import State
from job_controller.job_watcher import JobWatcher


class JobWatcherTest(unittest.TestCase):
    @mock.patch("job_controller.job_watcher.load_kubernetes_config")
    def setUp(self, _):
        self.job_name = mock.MagicMock()
        self.namespace = mock.MagicMock()
        self.kafka_ip = mock.MagicMock()
        self.ceph_path = mock.MagicMock()
        self.db_updater = mock.MagicMock()
        self.db_reduction_id = mock.MagicMock()
        self.job_script = mock.MagicMock()
        self.reduction_inputs = mock.MagicMock()
        self.job_watcher = JobWatcher(
            job_name=self.job_name,
            namespace=self.namespace,
            kafka_ip=self.kafka_ip,
            ceph_path=self.ceph_path,
            db_updater=self.db_updater,
            db_reduction_id=self.db_reduction_id,
            job_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
        )

    @mock.patch("job_controller.job_watcher.load_kubernetes_config")
    def test_ensure_init_load_kube_config(self, load_kube_config):
        JobWatcher("", "", "", "", mock.MagicMock(), 1, "", {})

        load_kube_config.assert_called_once_with()

    @mock.patch("job_controller.job_watcher.client")
    def test_grab_pod_name_filters_all_pods_in_namespace_against_passed_job_name(self, k8s_client):
        output = mock.MagicMock()
        owner = mock.MagicMock()
        owner.name = self.job_name
        pod = mock.MagicMock()
        pod.metadata.owner_references = [owner]
        pod.metadata.name = output
        k8s_client.CoreV1Api.return_value.list_namespaced_pod.return_value.items = [pod]

        return_value = self.job_watcher.grab_pod_name_from_job_name_in_namespace(self.job_name, self.namespace)

        self.assertEqual(str(return_value), str(output))

    @mock.patch("job_controller.job_watcher.logger")
    @mock.patch("job_controller.job_watcher.watch")
    @mock.patch("job_controller.job_watcher.client")
    def test_watch_handles_exceptions_from_code_handling_events(self, k8s_client, k8s_watch, logger):
        v1 = k8s_client.BatchV1Api.return_value
        watch_ = k8s_watch.Watch.return_value

        def raise_exception(_):
            raise Exception("EVERYTHING IS ON FIRE")  # pylint: disable=broad-exception-raised

        self.job_watcher.process_event = mock.MagicMock(side_effect=raise_exception)
        event = mock.MagicMock()
        watch_.stream.return_value = [event]

        self.job_watcher.watch()

        watch_.stream.assert_called_once_with(v1.list_job_for_all_namespaces)
        logger.error.assert_called_once_with(
            "Job watching failed due to an exception: %s", str(Exception("EVERYTHING IS ON FIRE"))
        )

    @mock.patch("job_controller.job_watcher.watch")
    @mock.patch("job_controller.job_watcher.client")
    def test_watch_analyzes_events_from_watch_stream(self, k8s_client, k8s_watch):
        v1 = k8s_client.BatchV1Api.return_value
        watch_ = k8s_watch.Watch.return_value
        self.job_watcher.process_event = mock.MagicMock()
        event = mock.MagicMock()
        watch_.stream.return_value = [event]

        self.job_watcher.watch()

        watch_.stream.assert_called_once_with(v1.list_job_for_all_namespaces)
        self.job_watcher.process_event.assert_called_once_with(event)

    def test_process_event_on_success_calls_success(self):
        event = mock.MagicMock()
        self.job_watcher.job_name = "mari0-asfn"
        event.__getitem__.return_value.metadata.name = "mari0-asfn-132"
        event.__getitem__.return_value.status.succeeded = 1
        self.job_watcher.process_event_success = mock.MagicMock()

        self.job_watcher.process_event(event)

        self.job_watcher.process_event_success.assert_called_once_with()

    def test_process_event_on_failures_calls_failure(self):
        event = mock.MagicMock()
        self.job_watcher.job_name = "mari0-asfn"
        event.__getitem__.return_value.metadata.name = "mari0-asfn-132"
        event.__getitem__.return_value.status.failed = 1
        self.job_watcher.process_event_failed = mock.MagicMock()

        self.job_watcher.process_event(event)

        self.job_watcher.process_event_failed.assert_called_once_with(event.__getitem__.return_value)

    @mock.patch("job_controller.job_watcher.client")
    def test_process_event_success_grabs_pod_name_using_grab_pod_name_from_job_name_in_namespace(self, _):
        self.job_watcher.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")
        self.job_watcher.notify_kafka = mock.MagicMock()
        self.job_watcher.get_logs = mock.MagicMock()

        self.job_watcher.process_event_success()

        self.job_watcher.grab_pod_name_from_job_name_in_namespace.assert_called_with(
            job_name=self.job_name, job_namespace=self.namespace
        )
        self.assertEqual(self.job_watcher.grab_pod_name_from_job_name_in_namespace.call_count, 1)

    @mock.patch("job_controller.job_watcher.client")
    def test_process_event_success_grabs_pod_name_using_grab_pod_name_from_job_name_in_namespace_raises_when_none(
        self, _
    ):
        self.job_watcher.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value=None)
        self.job_watcher.notify_kafka = mock.MagicMock()

        self.assertRaises(TypeError, self.job_watcher.process_event_success)

        self.job_watcher.grab_pod_name_from_job_name_in_namespace.assert_called_once_with(
            job_name=self.job_name, job_namespace=self.namespace
        )

    @mock.patch("job_controller.job_watcher.client")
    def test_process_event_success_passed_penultimate_log_line_to_notify_kafka_as_data(self, k8s_client):
        self.job_watcher.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")
        self.job_watcher.notify_kafka = mock.MagicMock()
        k8s_client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = (
            '4th to last\n3rd to last\n{"status": "SUCCESSFUL", "output_files": [], "status_message": ""}\n'
        )

        self.job_watcher.process_event_success()

        self.job_watcher.grab_pod_name_from_job_name_in_namespace.assert_called_with(
            job_name=self.job_name, job_namespace=self.namespace
        )
        self.assertEqual(self.job_watcher.grab_pod_name_from_job_name_in_namespace.call_count, 2)
        self.db_updater.add_completed_run.assert_called_once_with(
            db_reduction_id=self.db_reduction_id,
            state=State.SUCCESSFUL,
            status_message="",
            output_files=[],
            reduction_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
            reduction_start=k8s_client.CoreV1Api.return_value.read_namespaced_pod.return_value.status.start_time,
            reduction_end=str(None),
            reduction_logs='4th to last\n3rd to last\n{"status": "SUCCESSFUL", "output_files": [], '
            '"status_message": ""',
        )

    @mock.patch("job_controller.job_watcher.client")
    def test_process_event_success_handles_errors_where_penultimate_line_of_logs_is_not_valid_json(self, k8s_client):
        self.job_watcher.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")
        self.job_watcher.notify_kafka = mock.MagicMock()
        k8s_client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = (
            '4th to last\n3rd to last\n{"status": Not valid json, "output_files": [], "status_message": ""}\n'
        )

        self.job_watcher.process_event_success()

        self.job_watcher.grab_pod_name_from_job_name_in_namespace.assert_called_with(
            job_name=self.job_name, job_namespace=self.namespace
        )
        self.assertEqual(self.job_watcher.grab_pod_name_from_job_name_in_namespace.call_count, 2)
        self.db_updater.add_completed_run.assert_called_once_with(
            db_reduction_id=self.db_reduction_id,
            state=State.UNSUCCESSFUL,
            status_message="Expecting value: line 1 column 12 (char 11)",
            output_files=[],
            reduction_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
            reduction_start=k8s_client.CoreV1Api.return_value.read_namespaced_pod.return_value.status.start_time,
            reduction_end=str(None),
            reduction_logs='4th to last\n3rd to last\n{"status": Not valid json, "output_files": [], '
            '"status_message": ""',
        )

    @mock.patch("job_controller.job_watcher.client")
    def test_get_logs(self, client):
        namespace = mock.MagicMock()
        self.job_watcher.namespace = namespace
        self.job_watcher.grab_pod_name_from_job_name_in_namespace = mock.MagicMock(return_value="pod_name")

        return_value = self.job_watcher.get_logs()

        self.assertEqual(return_value, str(client.CoreV1Api.return_value.read_namespaced_pod_log.return_value))
        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name="pod_name", namespace=namespace
        )
