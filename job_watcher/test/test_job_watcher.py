# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access,
# pylint: disable=too-many-instance-attributes, invalid-name, too-many-public-methods, line-too-long,
# pylint: disable=redefined-outer-name, broad-exception-raised

import os
import random
import unittest
from datetime import datetime, timezone, timedelta
from json import JSONDecodeError
from unittest import mock
from unittest.mock import call

from jobwatcher.job_watcher import (JobWatcher, clean_up_pvcs_for_job, clean_up_pvs_for_job,
                                    _find_pod_from_partial_name, _find_latest_raised_error_and_stacktrace_from_reversed_logs)
from jobwatcher.database.state_enum import State


class JobWatcherTest(unittest.TestCase):
    @mock.patch("jobwatcher.job_watcher.client")
    def test_clean_up_pvcs_for_job(self, client):
        namespace = str(mock.MagicMock())
        job = mock.MagicMock()
        pvcs = [str(mock.MagicMock()), str(mock.MagicMock()), str(mock.MagicMock())]
        job.metadata.annotations = {"pvcs": str(pvcs)}

        clean_up_pvcs_for_job(job, namespace)

        for pvc in pvcs:
            self.assertIn(
                call(pvc, namespace=namespace),
                client.CoreV1Api.return_value.delete_namespaced_persistent_volume_claim.call_args_list,
            )

    @mock.patch("jobwatcher.job_watcher.client")
    def test_clean_up_pvs_for_job(self, client):
        job = mock.MagicMock()
        pvs = [str(mock.MagicMock()), str(mock.MagicMock()), str(mock.MagicMock())]
        job.metadata.annotations = {"pvs": str(pvs)}

        clean_up_pvs_for_job(job)

        for pv in pvs:
            self.assertIn(call(pv), client.CoreV1Api.return_value.delete_persistent_volume.call_args_list)

    @mock.patch("jobwatcher.job_watcher.client")
    def test_find_pod_from_partial_name(self, client):
        partial_name = str(mock.MagicMock())
        namespace = str(mock.MagicMock())
        pod = mock.MagicMock()
        pod.metadata.name = partial_name + "123123123"
        client.CoreV1Api.return_value.list_namespaced_pod.return_value.items = [pod]

        self.assertEqual(pod, _find_pod_from_partial_name(partial_name, namespace))

        client.CoreV1Api.return_value.list_namespaced_pod.assert_called_once_with(namespace=namespace)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_jobwatcher_init(self, client, _find_pod_from_partial_name):
        job_name = str(mock.MagicMock())
        partial_pod_name = str(mock.MagicMock())
        container_name = str(mock.MagicMock())
        db_updater = mock.MagicMock()
        max_time_to_complete = random.randint(1, 21000)
        namespace = str(mock.MagicMock())
        os.environ["JOB_NAMESPACE"] = namespace

        jw = JobWatcher(job_name, partial_pod_name, container_name, db_updater, max_time_to_complete)

        self.assertEqual(jw.job_name, job_name)
        self.assertEqual(jw.pod_name, _find_pod_from_partial_name.return_value.metadata.name)
        self.assertEqual(jw.pod, _find_pod_from_partial_name.return_value)
        self.assertEqual(jw.job, client.BatchV1Api.return_value.read_namespaced_job())
        self.assertEqual(jw.done_watching, False)
        self.assertEqual(jw.max_time_to_complete, max_time_to_complete)
        self.assertEqual(jw.db_updater, db_updater)
        self.assertEqual(jw.namespace, namespace)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_jobwatcher_watch_done_watching(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.check_for_changes = mock.MagicMock()
        jw.done_watching = True

        jw.watch()

        jw.check_for_changes.assert_not_called()

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_jobwatcher_watch_not_done_watching_but_done_via_changes(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())

        def done_watching():
            jw.done_watching = True

        jw.check_for_changes = mock.MagicMock(side_effect=done_watching)
        jw.done_watching = False

        jw.watch()

        jw.check_for_changes.assert_called_once_with()

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_update_current_container_info_no_name_provided(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod_name = None

        self.assertRaises(ValueError, jw.update_current_container_info)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_update_current_container_info_full_name(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        pod_name = str(mock.MagicMock())
        jw.pod_name = pod_name

        jw.update_current_container_info()

        client.CoreV1Api.return_value.read_namespaced_pod.assert_called_once_with(name=pod_name, namespace=jw.namespace)
        self.assertEqual(jw.pod, client.CoreV1Api.return_value.read_namespaced_pod.return_value)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_update_current_container_info_partial_name(self, client, _find_pod_from_partial_name):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        partial_pod_name = str(mock.MagicMock())
        jw.pod_name = None
        client.CoreV1Api.return_value.read_namespaced_pod = mock.MagicMock()

        jw.update_current_container_info(partial_pod_name)

        self.assertEqual(jw.pod, _find_pod_from_partial_name.return_value)
        _find_pod_from_partial_name.assert_called_with(partial_pod_name, namespace=jw.namespace)
        self.assertEqual(_find_pod_from_partial_name.call_count, 2)  # Called twice because called during jw init

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_update_current_container_info_partial_name_no_pod(self, _, _find_pod_from_partial_name):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        partial_name = str(mock.MagicMock())
        _find_pod_from_partial_name.return_value = None

        self.assertRaises(ValueError, jw.update_current_container_info, partial_name)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_changes_job_incomplete_and_not_stalled(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.update_current_container_info = mock.MagicMock()
        jw.cleanup_job = mock.MagicMock()
        jw.check_for_job_complete = mock.MagicMock(return_value=False)
        jw.check_for_pod_stalled = mock.MagicMock(return_value=False)
        jw.done_watching = False

        jw.check_for_changes()

        jw.update_current_container_info.assert_called_once_with()
        jw.cleanup_job.assert_not_called()
        jw.check_for_job_complete.assert_called_once_with()
        jw.check_for_pod_stalled.assert_called_once_with()
        self.assertEqual(jw.done_watching, False)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_changes_job_complete_and_not_stalled(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.update_current_container_info = mock.MagicMock()
        jw.cleanup_job = mock.MagicMock()
        jw.check_for_job_complete = mock.MagicMock(return_value=True)
        jw.check_for_pod_stalled = mock.MagicMock(return_value=False)
        jw.done_watching = False

        jw.check_for_changes()

        jw.update_current_container_info.assert_called_once_with()
        jw.cleanup_job.assert_called_once_with()
        jw.check_for_job_complete.assert_called_once_with()
        jw.check_for_pod_stalled.assert_not_called()
        self.assertEqual(jw.done_watching, True)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_changes_job_complete_and_stalled(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.update_current_container_info = mock.MagicMock()
        jw.cleanup_job = mock.MagicMock()
        jw.check_for_job_complete = mock.MagicMock(return_value=True)
        jw.check_for_pod_stalled = mock.MagicMock(return_value=True)
        jw.done_watching = False

        jw.check_for_changes()

        jw.update_current_container_info.assert_called_once_with()
        jw.cleanup_job.assert_called_once_with()
        jw.check_for_job_complete.assert_called_once_with()
        jw.check_for_pod_stalled.assert_not_called()
        self.assertEqual(jw.done_watching, True)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_changes_job_incomplete_and_stalled(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.update_current_container_info = mock.MagicMock()
        jw.cleanup_job = mock.MagicMock()
        jw.check_for_job_complete = mock.MagicMock(return_value=False)
        jw.check_for_pod_stalled = mock.MagicMock(return_value=True)
        jw.done_watching = False

        jw.check_for_changes()

        jw.update_current_container_info.assert_called_once_with()
        jw.cleanup_job.assert_called_once_with()
        jw.check_for_job_complete.assert_called_once_with()
        jw.check_for_pod_stalled.assert_called_once_with()
        self.assertEqual(jw.done_watching, True)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_get_container_status(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        container_status = mock.MagicMock()
        jw.pod.status.container_statuses = [container_status]
        container_name = str(mock.MagicMock())
        jw.container_name = container_name
        container_status.name = container_name

        result = jw.get_container_status()

        self.assertEqual(result, container_status)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_get_container_status_pod_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod = mock.MagicMock()

        result = jw.get_container_status()

        self.assertEqual(result, None)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_get_container_status_not_present(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        container_status = mock.MagicMock()
        jw.pod.status.container_statuses = [container_status]
        container_name = str(mock.MagicMock())
        jw.container_name = mock.MagicMock()
        container_status.name = container_name

        result = jw.get_container_status()

        self.assertEqual(result, None)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_job_complete_container_status_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.get_container_status = mock.MagicMock(return_value=None)

        self.assertRaises(ValueError, jw.check_for_job_complete)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_job_complete_container_status_is_terminated_exit_code_0(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        container_status = mock.MagicMock()
        container_status.state.terminated.exit_code = 0
        jw.get_container_status = mock.MagicMock(return_value=container_status)
        jw.process_job_success = mock.MagicMock()
        jw.process_job_failed = mock.MagicMock()

        self.assertEqual(jw.check_for_job_complete(), True)

        jw.process_job_success.assert_called_once_with()
        jw.process_job_failed.assert_not_called()

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_job_complete_container_status_is_terminated_exit_code_not_0(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        container_status = mock.MagicMock()
        container_status.state.terminated.exit_code = 100
        jw.get_container_status = mock.MagicMock(return_value=container_status)
        jw.process_job_success = mock.MagicMock()
        jw.process_job_failed = mock.MagicMock()

        self.assertEqual(jw.check_for_job_complete(), True)

        jw.process_job_success.assert_not_called()
        jw.process_job_failed.assert_called_once_with()

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_job_complete_container_status_running(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        container_status = mock.MagicMock()
        container_status.state.terminated = None
        jw.get_container_status = mock.MagicMock(return_value=container_status)
        jw.process_job_success = mock.MagicMock()
        jw.process_job_failed = mock.MagicMock()

        self.assertEqual(jw.check_for_job_complete(), False)

        jw.process_job_success.assert_not_called()
        jw.process_job_failed.assert_not_called()

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_pod_stalled_pod_is_younger_than_30_minutes(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod.metadata.creation_timestamp = datetime.now(timezone.utc)
        jw.max_time_to_complete = 99999999

        self.assertEqual(jw.check_for_pod_stalled(), False)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_pod_stalled_pod_where_pod_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod = None

        self.assertRaises(AttributeError, jw.check_for_pod_stalled)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_pod_stalled_pod_is_stalled_for_30_minutes(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod.metadata.creation_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60 * 35)
        jw.max_time_to_complete = 99999999
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = ""

        self.assertEqual(jw.check_for_pod_stalled(), True)

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name=jw.pod.metadata.name,
            namespace=jw.pod.metadata.namespace,
            timestamps=True,
            tail_lines=1,
            since_seconds=60 * 30,
            container=jw.container_name,
        )

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_check_for_pod_stalled_pod_is_running_fine(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod.metadata.creation_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60 * 10)
        jw.max_time_to_complete = 1

        self.assertEqual(jw.check_for_pod_stalled(), True)

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_not_called()

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_find_start_and_end_of_pod(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        pod = mock.MagicMock()
        jw.get_container_status = mock.MagicMock()
        jw.get_container_status.return_value.state.terminated = None

        self.assertEqual(
            jw._find_start_and_end_of_pod(pod),
            (client.CoreV1Api.return_value.read_namespaced_pod.return_value.status.start_time, None),
        )

        client.CoreV1Api.return_value.read_namespaced_pod.assert_called_once_with(pod.metadata.name, jw.namespace)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_find_start_and_end_of_pod_terminated(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        pod = mock.MagicMock()
        jw.get_container_status = mock.MagicMock()

        self.assertEqual(
            jw._find_start_and_end_of_pod(pod),
            (
                client.CoreV1Api.return_value.read_namespaced_pod.return_value.status.start_time,
                jw.get_container_status.return_value.state.terminated.finished_at,
            ),
        )

        client.CoreV1Api.return_value.read_namespaced_pod.assert_called_once_with(pod.metadata.name, jw.namespace)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_failed(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        status_message = mock.MagicMock()
        stacktrace = mock.MagicMock()
        jw._find_latest_raised_error_and_stacktrace = mock.MagicMock(return_value=[status_message, stacktrace])
        start = mock.MagicMock()
        end = mock.MagicMock()
        jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
        jw.db_updater = mock.MagicMock()

        jw.process_job_failed()

        jw.db_updater.update_completed_run.assert_called_once_with(
            db_reduction_id=jw.job.metadata.annotations["reduction-id"],
            state=State(State.ERROR),
            status_message=status_message,
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
            stacktrace=stacktrace,
        )

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_failed_pod_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod = None
        jw.job = mock.MagicMock()

        self.assertRaises(AttributeError, jw.process_job_failed)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_failed_job_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod = mock.MagicMock()
        jw.job = None

        self.assertRaises(AttributeError, jw.process_job_failed)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_success(self, client, _):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        start = mock.MagicMock()
        end = mock.MagicMock()
        jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
        client.BatchV1Api.return_value.read_namespaced_job.return_value.metadata.annotations.get.return_value = "id"
        logs = """
        line 1
        line 2
        line 3
        
        {"status": "Successful", "status_message": "status_message", "output_files": "output_file.nxs"}
        """
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = logs

        jw.process_job_success()

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
        )
        jw.db_updater.update_completed_run.assert_called_once_with(
            db_reduction_id=client.BatchV1Api.return_value.read_namespaced_job.return_value.metadata.annotations.get.return_value,
            state=State.SUCCESSFUL,
            status_message="status_message",
            output_files="output_file.nxs",
            reduction_end=str(end),
            reduction_start=start,
            stacktrace="",
        )

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_success_pod_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod = None
        jw.job = mock.MagicMock()

        self.assertRaises(AttributeError, jw.process_job_success)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_success_job_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.job = None
        jw.pod = mock.MagicMock()

        self.assertRaises(AttributeError, jw.process_job_success)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_success_raise_json_decode_error(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        start = mock.MagicMock()
        end = mock.MagicMock()
        jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))

        def raise_error(name, namespace, container):
            raise JSONDecodeError("", "", 1)

        client.CoreV1Api.return_value.read_namespaced_pod_log = mock.MagicMock(side_effect=raise_error)

        jw.process_job_success()

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
        )
        jw.db_updater.update_completed_run.assert_called_once_with(
            db_reduction_id=client.BatchV1Api.return_value.read_namespaced_job.return_value.metadata.annotations.get.return_value,
            state=State.UNSUCCESSFUL,
            status_message=": line 1 column 2 (char 1)",
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
            stacktrace="",
        )

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_success_raise_type_error(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        start = mock.MagicMock()
        end = mock.MagicMock()
        jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))

        def raise_error(name, namespace, container):
            raise TypeError("TypeError!")

        client.CoreV1Api.return_value.read_namespaced_pod_log = mock.MagicMock(side_effect=raise_error)

        jw.process_job_success()

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
        )
        jw.db_updater.update_completed_run.assert_called_once_with(
            db_reduction_id=client.BatchV1Api.return_value.read_namespaced_job.return_value.metadata.annotations.get.return_value,
            state=State.UNSUCCESSFUL,
            status_message="TypeError!",
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
            stacktrace="",
        )

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_process_job_success_raise_exception(self, client, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        start = mock.MagicMock()
        end = mock.MagicMock()
        jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))

        def raise_error(name, namespace, container):
            raise Exception("Exception raised!")

        client.CoreV1Api.return_value.read_namespaced_pod_log = mock.MagicMock(side_effect=raise_error)

        jw.process_job_success()

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
        )
        jw.db_updater.update_completed_run.assert_called_once_with(
            db_reduction_id=client.BatchV1Api.return_value.read_namespaced_job.return_value.metadata.annotations.get.return_value,
            state=State.UNSUCCESSFUL,
            status_message="Exception raised!",
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
            stacktrace="",
        )

    @mock.patch("jobwatcher.job_watcher.clean_up_pvcs_for_job")
    @mock.patch("jobwatcher.job_watcher.clean_up_pvs_for_job")
    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_cleanup_job(self, _, __, clean_up_pvs_for_job, clean_up_pvcs_for_job):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())

        jw.cleanup_job()

        clean_up_pvs_for_job.assert_called_once_with(jw.job)
        clean_up_pvcs_for_job.assert_called_once_with(jw.job, jw.namespace)

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_cleanup_job_where_job_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.job = None

        self.assertRaises(AttributeError, jw.cleanup_job)

    @mock.patch("jobwatcher.job_watcher.client")
    def test_find_latest_raised_error_and_stacktrace_from_reversed_logs(self, _):
        logs = [
            "Random error ahead! Just doing some data processing watch out!",
            "Some data processing output",
            "Traceback (most recent call last):",
            '  File "/path/to/example.py", line 4, in <module>',
            "    processData('I'm processing data over here!')",
            '  File "/path/to/example.py", line 2, in greet',
            "    dataCreate('Give me the gabagool')",
            "AttributeError: I don't know where the gabagool is Tony",
            "{{output the failure to get gabagool using json}}",
        ]
        logs.reverse()
        expected_stacktrace = (
            "Traceback (most recent call last):\n"
            '  File "/path/to/example.py", line 4, in <module>\n'
            "    processData('I'm processing data over here!')\n"
            '  File "/path/to/example.py", line 2, in greet\n'
            "    dataCreate('Give me the gabagool')\n"
            "AttributeError: I don't know where the gabagool is Tony\n"
        )
        expected_recorded_line = "AttributeError: I don't know where the gabagool is Tony"

        recorded_line, stacktrace = _find_latest_raised_error_and_stacktrace_from_reversed_logs(logs)

        self.assertEqual(recorded_line, expected_recorded_line)
        self.assertEqual(stacktrace, expected_stacktrace)

    @mock.patch("jobwatcher.job_watcher._find_latest_raised_error_and_stacktrace_from_reversed_logs")
    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_find_latest_raised_error_and_stacktrace(self, client, _, return_raised_error_call):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())

        jw._find_latest_raised_error_and_stacktrace()

        client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
            name=jw.pod.metadata.name,
            namespace=jw.pod.metadata.namespace,
            tail_lines=50,
            container=jw.container_name,
        )
        (
            client.CoreV1Api.return_value.read_namespaced_pod_log.return_value.split.return_value.reverse.assert_called_once_with()
        )
        return_raised_error_call.assert_called_once_with(
            client.CoreV1Api.return_value.read_namespaced_pod_log.return_value.split.return_value
        )

    @mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
    @mock.patch("jobwatcher.job_watcher.client")
    def test_find_latest_raised_error_and_stacktrace_raises_error_on_pod_is_none(self, _, __):
        jw = JobWatcher(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        jw.pod = None

        self.assertRaises(AttributeError, jw._find_latest_raised_error_and_stacktrace)
