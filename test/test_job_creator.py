# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock

from job_controller.main import JobCreator


class JobCreatorTest(unittest.TestCase):
    @mock.patch("job_controller.job_creator.load_kubernetes_config")
    def test_init_calls_load_cluster_config(self, load_kubernetes_config):
        JobCreator("")

        load_kubernetes_config.assert_called_once_with()

    @mock.patch("job_controller.job_creator.client")
    @mock.patch("job_controller.job_creator.load_kubernetes_config")
    def test_spawn_pod_creates_pod_with_passed_values(self, _, kubernetes_client):
        script = mock.MagicMock()
        ceph_path = mock.MagicMock()
        job_name = mock.MagicMock()
        job_namespace = mock.MagicMock()
        user_id = mock.MagicMock()
        runner_sha = mock.MagicMock()
        k8s = JobCreator(runner_sha)

        k8s.spawn_job(
            job_name=job_name, job_namespace=job_namespace, script=script, ceph_path=ceph_path, user_id=user_id
        )

        k8s_pod_call_kwargs = kubernetes_client.V1Job.call_args_list[0].kwargs
        pod_metadata = k8s_pod_call_kwargs["metadata"]
        self.assertEqual(pod_metadata["name"], job_name)

        pod_spec = k8s_pod_call_kwargs["spec"]
        self.assertEqual(pod_spec["backoffLimit"], 0)
        self.assertEqual(pod_spec["ttlSecondsAfterFinished"], 21600)

        security_context = k8s_pod_call_kwargs["spec"]["template"]["spec"]["security_context"]
        self.assertEqual({"runAsUser": user_id}, security_context)

        pod_container = k8s_pod_call_kwargs["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(pod_container["name"], job_name)
        self.assertEqual(f"ghcr.io/interactivereduction/runner@sha256:{runner_sha}", pod_container["image"])
        self.assertEqual([script], pod_container["args"])
        self.assertEqual(len(pod_container["volumeMounts"]), 2)
        self.assertIn({"name": "archive-mount", "mountPath": "/archive"}, pod_container["volumeMounts"])
        self.assertIn({"name": "ceph-mount", "mountPath": "/output"}, pod_container["volumeMounts"])

        volumes = k8s_pod_call_kwargs["spec"]["template"]["spec"]["volumes"]
        self.assertIn({"name": "archive-mount", "persistentVolumeClaim": {"claimName": f"{job_name}-archive-pvc",
                                                                          "readOnly": True}}, volumes)
        self.assertIn({"name": "ceph-mount", "hostPath": {"type": "Directory", "path": ceph_path}}, volumes)
        self.assertEqual(len(volumes), 2)

        kubernetes_client.BatchV1Api.return_value.create_namespaced_job.assert_called_once()
        self.assertEqual(
            kubernetes_client.BatchV1Api.return_value.create_namespaced_job.call_args.kwargs["namespace"], job_namespace
        )

    @mock.patch("job_controller.job_creator.client")
    @mock.patch("job_controller.job_creator.load_kubernetes_config")
    def test_spawn_pod_creates_pvc_with_passed_values(self, _, kubernetes_client):
        script = mock.MagicMock()
        ceph_path = mock.MagicMock()
        job_name = mock.MagicMock()
        job_namespace = mock.MagicMock()
        user_id = mock.MagicMock()
        runner_sha = mock.MagicMock()
        k8s = JobCreator(runner_sha)

        k8s.spawn_job(
            job_name=job_name, job_namespace=job_namespace, script=script, ceph_path=ceph_path, user_id=user_id
        )

        k8s_pod_call_kwargs = kubernetes_client.V1PersistentVolumeClaim.call_args_list[0].kwargs
        self.assertEqual(k8s_pod_call_kwargs["kind"], "PersistentVolumeClaim")

        metadata = k8s_pod_call_kwargs["metadata"]
        self.assertEqual(metadata["name"], f"{job_name}-archive-pvc")

        spec = k8s_pod_call_kwargs["spec"]
        self.assertEqual(spec["accessModes"], ["ReadOnlyMany"])
        self.assertEqual(spec["resources"]["requests"]["storage"], "1000Gi")
        self.assertEqual(spec["volumeName"], f"{job_name}-archive-pv-smb")
        self.assertEqual(spec["storageClassName"], "smb")

    @mock.patch("job_controller.job_creator.client")
    @mock.patch("job_controller.job_creator.load_kubernetes_config")
    def test_spawn_pod_creates_pv_with_passed_values(self, _, kubernetes_client):
        script = mock.MagicMock()
        ceph_path = mock.MagicMock()
        job_name = mock.MagicMock()
        job_namespace = mock.MagicMock()
        user_id = mock.MagicMock()
        runner_sha = mock.MagicMock()
        k8s = JobCreator(runner_sha)

        k8s.spawn_job(
            job_name=job_name, job_namespace=job_namespace, script=script, ceph_path=ceph_path, user_id=user_id
        )

        k8s_pod_call_kwargs = kubernetes_client.V1PersistentVolume.call_args_list[0].kwargs
        self.assertEqual(k8s_pod_call_kwargs["kind"], "PersistentVolume")

        metadata = k8s_pod_call_kwargs["metadata"]
        self.assertEqual(metadata["annotations"]["pv.kubernetes.io/provisioned-by"], "smb.csi.k8s.io")
        self.assertEqual(metadata["name"], f"{job_name}-archive-pv-smb")

        spec = k8s_pod_call_kwargs["spec"]
        self.assertEqual(spec["capacity"]["storage"], "1000Gi")
        self.assertEqual(spec["accessModes"], ["ReadOnlyMany"])
        self.assertEqual(spec["persistentVolumeReclaimPolicy"], "Retain")
        self.assertEqual(spec["storageClassName"], "smb")
        self.assertListEqual(spec["mountOptions"], ["noserverino", "_netdev", "vers=2.1", "uid=1001", "gid=1001",
                                                    "dir_mode=0555", "file_mode=0444"])

        csi = spec["csi"]
        self.assertEqual(csi["driver"], "smb.csi.k8s.io")
        self.assertEqual(csi["readOnly"], True)
        self.assertEqual(csi["volumeHandle"], "archive.ir.svc.cluster.local/share##archive")
        self.assertEqual(csi["volumeAttributes"]["source"], "//isisdatar55.isis.cclrc.ac.uk/inst$/")
        self.assertEqual(csi["nodeStageSecretRef"]["name"], "archive-creds")
        self.assertEqual(csi["nodeStageSecretRef"]["namespace"], "ir")
