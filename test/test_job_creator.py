# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock

from job_controller.main import JobCreator


class JobCreatorTest(unittest.TestCase):
    @mock.patch("job_controller.job_creator.load_kubernetes_config")
    def test_init_calls_load_cluster_config(self, load_kubernetes_config):
        JobCreator()

        load_kubernetes_config.assert_called_once_with()

    @mock.patch("job_controller.job_creator.client")
    @mock.patch("job_controller.job_creator.load_kubernetes_config")
    def test_spawn_pod_creates_pod_with_passed_values(self, _, kubernetes_client):
        script = mock.MagicMock()
        ceph_path = mock.MagicMock()
        job_name = mock.MagicMock()
        job_namespace = mock.MagicMock()
        user_id = mock.MagicMock()
        k8s = JobCreator()

        k8s.spawn_job(
            job_name=job_name, job_namespace=job_namespace, script=script, ceph_path=ceph_path, user_id=user_id
        )

        k8s_pod_call_kwargs = kubernetes_client.V1Job.call_args_list[0].kwargs
        pod_metadata = k8s_pod_call_kwargs["metadata"]
        self.assertEqual(pod_metadata["name"], job_name)

        security_context = k8s_pod_call_kwargs["spec"]["template"]["spec"]["security_context"]
        self.assertEqual({"runAsUser": user_id}, security_context)

        pod_container = k8s_pod_call_kwargs["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(pod_container["name"], job_name)
        self.assertEqual(["-c", script], pod_container["args"])
        self.assertEqual(len(pod_container["volumeMounts"]), 2)
        self.assertIn({"name": "archive-mount", "mountPath": "/archive"}, pod_container["volumeMounts"])
        self.assertIn({"name": "ceph-mount", "mountPath": "/output"}, pod_container["volumeMounts"])

        volumes = k8s_pod_call_kwargs["spec"]["template"]["spec"]["volumes"]
        self.assertIn({"name": "archive-mount", "hostPath": {"type": "Directory", "path": "/archive"}}, volumes)
        self.assertIn({"name": "ceph-mount", "hostPath": {"type": "Directory", "path": ceph_path}}, volumes)
        self.assertEqual(len(volumes), 2)

        kubernetes_client.BatchV1Api.return_value.create_namespaced_job.assert_called_once()
        self.assertEqual(
            kubernetes_client.BatchV1Api.return_value.create_namespaced_job.call_args.kwargs["namespace"], job_namespace
        )
