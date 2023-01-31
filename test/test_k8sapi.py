# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock

from jobcontroller.k8sapi import K8sAPI


class K8sAPITest(unittest.TestCase):
    @mock.patch("jobcontroller.k8sapi.config")
    def test_config_grabbed_from_incluster(self, kubernetes_config):
        K8sAPI()

        kubernetes_config.load_incluster_config.assert_called_once_with()

    @mock.patch("jobcontroller.k8sapi.client")
    @mock.patch("jobcontroller.k8sapi.config")
    def test_spawn_pod_creates_pod_with_passed_values(self, _, kubernetes_client):
        filename = mock.MagicMock()
        kafka_ip = mock.MagicMock()
        rb_number = mock.MagicMock()
        instrument_name = mock.MagicMock()
        job_name = f"run-{filename}"
        k8s = K8sAPI()

        k8s.spawn_pod(filename=filename, kafka_ip=kafka_ip, rb_number=rb_number, instrument_name=instrument_name)

        k8s_pod_call_kwargs = kubernetes_client.V1Pod.call_args_list[0].kwargs
        pod_metadata = k8s_pod_call_kwargs["metadata"]
        self.assertEqual(pod_metadata["name"], job_name)

        pod_container = k8s_pod_call_kwargs["spec"]["containers"][0]
        self.assertEqual(pod_container["name"], job_name)
        self.assertIn("ir-mantid-runner@sha256", pod_container["image"])
        self.assertEqual(len(pod_container["env"]), 5)
        self.assertIn({"name": "KAFKA_IP", "value": kafka_ip}, pod_container["env"])
        self.assertIn({"name": "RUN_FILENAME", "value": filename}, pod_container["env"])
        self.assertIn({"name": "IR_API_IP", "value": "irapi.ir.svc.cluster.local"}, pod_container["env"])
        self.assertIn({"name": "RB_NUMBER", "value": rb_number}, pod_container["env"])
        self.assertIn({"name": "INSTRUMENT_NAME", "value": instrument_name}, pod_container["env"])
        self.assertEqual(len(pod_container["volumeMounts"]), 2)
        self.assertIn({"name": "archive-mount", "mountPath": "/archive"}, pod_container["volumeMounts"])
        self.assertIn({"name": "ceph-mount", "mountPath": "/ceph"}, pod_container["volumeMounts"])
