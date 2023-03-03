# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock

from jobcontroller.k8sapi import K8sAPI


class K8sAPITest(unittest.TestCase):
    @mock.patch("jobcontroller.k8sapi.load_kubernetes_config")
    def test_init_calls_load_cluster_config(self, load_kubernetes_config):
        K8sAPI()

        load_kubernetes_config.assert_called_once_with()

    @mock.patch("jobcontroller.k8sapi.client")
    @mock.patch("jobcontroller.k8sapi.load_kubernetes_config")
    def test_spawn_pod_creates_pod_with_passed_values(self, _, kubernetes_client):
        script = mock.MagicMock()
        ceph_path = mock.MagicMock()
        job_name = mock.MagicMock()
        k8s = K8sAPI()

        k8s.spawn_job(job_name=job_name, script=script, ceph_path=ceph_path)

        k8s_pod_call_kwargs = kubernetes_client.V1Job.call_args_list[0].kwargs
        pod_metadata = k8s_pod_call_kwargs["metadata"]
        self.assertEqual(pod_metadata["name"], job_name)

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
