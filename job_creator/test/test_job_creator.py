import random
import unittest
from unittest import mock
from unittest.mock import call

from jobcreator.job_creator import JobCreator, _setup_archive_pv, _setup_archive_pvc, _setup_ceph_pv, _setup_ceph_pvc


class JobCreatorTest(unittest.TestCase):
    @mock.patch("jobcreator.job_creator.client")
    def test_setup_archive_pv(self, client):
        job_name = str(mock.MagicMock())
        pv_name = f"{job_name}-archive-pv-smb"

        self.assertEqual(_setup_archive_pv(job_name), pv_name)

        client.CoreV1Api.return_value.create_persistent_volume.assert_called_once_with(
            client.V1PersistentVolume.return_value
        )
        client.V1PersistentVolume.assert_called_once_with(
            api_version="v1",
            kind="PersistentVolume",
            metadata=client.V1ObjectMeta.return_value,
            spec=client.V1PersistentVolumeSpec.return_value,
        )
        client.V1ObjectMeta.assert_called_once_with(
            name=pv_name, annotations={"pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"}
        )
        client.V1PersistentVolumeSpec.assert_called_once_with(
            capacity={"storage": "1000Gi"},
            access_modes=["ReadOnlyMany"],
            persistent_volume_reclaim_policy="Retain",
            mount_options=["noserverino", "_netdev", "vers=2.1"],
            csi=client.V1CSIPersistentVolumeSource.return_value,
        )
        client.V1CSIPersistentVolumeSource.assert_called_once_with(
            driver="smb.csi.k8s.io",
            read_only=True,
            volume_handle=pv_name,
            volume_attributes={"source": "//isisdatar55.isis.cclrc.ac.uk/inst$/"},
            node_stage_secret_ref=client.V1SecretReference.return_value,
        )
        client.V1SecretReference.assert_called_once_with(name="archive-creds", namespace="ir")

    @mock.patch("jobcreator.job_creator.client")
    def test_setup_archive_pvc(self, client):
        job_name = str(mock.MagicMock())
        job_namespace = str(mock.MagicMock())
        pvc_name = f"{job_name}-archive-pvc"

        self.assertEqual(_setup_archive_pvc(job_name, job_namespace), pvc_name)

        client.V1ObjectMeta.assert_called_once_with(name=pvc_name)
        client.V1ResourceRequirements(requests={"storage": "1000Gi"})
        client.V1PersistentVolumeClaimSpec.assert_called_once_with(
            access_modes=["ReadOnlyMany"],
            resources=client.V1ResourceRequirements.return_value,
            volume_name=f"{job_name}-archive-pv-smb",
            storage_class_name="",
        )
        client.V1PersistentVolumeClaim.assert_called_once_with(
            api_version="v1",
            kind="PersistentVolumeClaim",
            metadata=client.V1ObjectMeta.return_value,
            spec=client.V1PersistentVolumeClaimSpec.return_value,
        )
        client.CoreV1Api.return_value.create_namespaced_persistent_volume_claim.assert_called_once_with(
            namespace=job_namespace, body=client.V1PersistentVolumeClaim.return_value
        )

    @mock.patch("jobcreator.job_creator.client")
    def test_setup_ceph_pv(self, client):
        job_name = str(mock.MagicMock())
        ceph_creds_k8s_secret_name = str(mock.MagicMock())
        ceph_creds_k8s_namespace = str(mock.MagicMock())
        cluster_id = str(mock.MagicMock())
        fs_name = str(mock.MagicMock())
        ceph_mount_path = str(mock.MagicMock())
        pv_name = f"{job_name}-ceph-pv"

        self.assertEqual(
            _setup_ceph_pv(
                job_name, ceph_creds_k8s_secret_name, ceph_creds_k8s_namespace, cluster_id, fs_name, ceph_mount_path
            ),
            pv_name,
        )

        client.CoreV1Api.return_value.create_persistent_volume.assert_called_once_with(
            client.V1PersistentVolume.return_value
        )
        client.V1PersistentVolume.assert_called_once_with(
            api_version="v1",
            kind="PersistentVolume",
            metadata=client.V1ObjectMeta.return_value,
            spec=client.V1PersistentVolumeSpec.return_value,
        )
        client.V1ObjectMeta.assert_called_once_with(name=pv_name)
        client.V1PersistentVolumeSpec.assert_called_once_with(
            capacity={"storage": "1000Gi"},
            storage_class_name="",
            access_modes=["ReadWriteMany"],
            persistent_volume_reclaim_policy="Retain",
            volume_mode="Filesystem",
            csi=client.V1CSIPersistentVolumeSource.return_value,
        )
        client.V1CSIPersistentVolumeSource.assert_called_once_with(
            driver="cephfs.csi.ceph.com",
            node_stage_secret_ref=client.V1SecretReference.return_value,
            volume_handle=pv_name,
            volume_attributes={
                "clusterID": cluster_id,
                "mounter": "fuse",
                "fsName": fs_name,
                "staticVolume": "true",
                "rootPath": "/isis/instrument" + ceph_mount_path,
            },
        )
        client.V1SecretReference.assert_called_once_with(
            name=ceph_creds_k8s_secret_name, namespace=ceph_creds_k8s_namespace
        )

    @mock.patch("jobcreator.job_creator.client")
    def test_setup_ceph_pvc(self, client):
        job_name = str(mock.MagicMock())
        job_namespace = str(mock.MagicMock())
        pvc_name = f"{job_name}-ceph-pvc"

        self.assertEqual(_setup_ceph_pvc(job_name, job_namespace), pvc_name)

        client.V1ObjectMeta.assert_called_once_with(name=pvc_name)
        client.V1ResourceRequirements(requests={"storage": "1000Gi"})
        client.V1PersistentVolumeClaimSpec.assert_called_once_with(
            access_modes=["ReadWriteMany"],
            resources=client.V1ResourceRequirements.return_value,
            volume_name=f"{job_name}-ceph-pv",
            storage_class_name="",
        )
        client.V1PersistentVolumeClaim.assert_called_once_with(
            api_version="v1",
            kind="PersistentVolumeClaim",
            metadata=client.V1ObjectMeta.return_value,
            spec=client.V1PersistentVolumeClaimSpec.return_value,
        )
        client.CoreV1Api.return_value.create_namespaced_persistent_volume_claim.assert_called_once_with(
            namespace=job_namespace, body=client.V1PersistentVolumeClaim.return_value
        )

    @mock.patch("jobcreator.job_creator.load_kubernetes_config")
    def test_jobcreator_init(self, mock_load_kubernetes_config):
        JobCreator("", False)

        mock_load_kubernetes_config.assert_called_once()

    @mock.patch("jobcreator.job_creator._setup_archive_pv")
    @mock.patch("jobcreator.job_creator._setup_archive_pvc")
    @mock.patch("jobcreator.job_creator._setup_ceph_pv")
    @mock.patch("jobcreator.job_creator._setup_ceph_pvc")
    @mock.patch("jobcreator.job_creator.load_kubernetes_config")
    @mock.patch("jobcreator.job_creator.client")
    def test_jobcreator_spawn_job_dev_mode_false(
        self, client, _, setup_ceph_pvc, setup_ceph_pv, setup_archive_pvc, setup_archive_pv
    ):
        job_name = mock.MagicMock()
        script = mock.MagicMock()
        job_namespace = mock.MagicMock()
        ceph_creds_k8s_secret_name = mock.MagicMock()
        ceph_creds_k8s_namespace = mock.MagicMock()
        cluster_id = mock.MagicMock()
        fs_name = mock.MagicMock()
        ceph_mount_path = mock.MagicMock()
        reduction_id = random.randint(1, 100)
        max_time_to_complete_job = random.randint(1, 20000)
        db_ip = mock.MagicMock()
        db_username = mock.MagicMock()
        db_password = mock.MagicMock()
        runner_sha = mock.MagicMock()
        watcher_sha = mock.MagicMock()
        job_creator = JobCreator(watcher_sha, False)

        job_creator.spawn_job(
            job_name,
            script,
            job_namespace,
            ceph_creds_k8s_secret_name,
            ceph_creds_k8s_namespace,
            cluster_id,
            fs_name,
            ceph_mount_path,
            reduction_id,
            max_time_to_complete_job,
            db_ip,
            db_username,
            db_password,
            runner_sha,
        )

        self.assertEqual(
            client.BatchV1Api.return_value.create_namespaced_job.call_args.kwargs["namespace"], job_namespace
        )
        self.assertEqual(
            client.BatchV1Api.return_value.create_namespaced_job.call_args.kwargs["body"], client.V1Job.return_value
        )
        client.V1Job.assert_called_once_with(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta.return_value,
            spec=client.V1JobSpec.return_value,
        )
        client.V1ObjectMeta.assert_called_once_with(
            name=job_name,
            annotations={
                "reduction-id": str(reduction_id),
                "pvs": str([setup_archive_pv.return_value, setup_ceph_pv.return_value]),
                "pvcs": str([setup_archive_pvc.return_value, setup_ceph_pvc.return_value]),
                "kubectl.kubernetes.io/default-container": client.V1Container.return_value.name,
            },
        )
        client.V1JobSpec.assert_called_once_with(
            template=client.V1PodTemplateSpec.return_value, backoff_limit=0, ttl_seconds_after_finished=21600
        )
        client.V1PodTemplateSpec.assert_called_once_with(spec=client.V1PodSpec.return_value)
        client.V1PodSpec.assert_called_once_with(
            service_account_name="jobwatcher",
            containers=[client.V1Container.return_value, client.V1Container.return_value],
            restart_policy="Never",
            tolerations=[client.V1Toleration.return_value],
            volumes=[client.V1Volume.return_value, client.V1Volume.return_value],
        )
        self.assertIn(
            call(name="ceph-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value),
            client.V1Volume.call_args_list,
        )
        self.assertIn(
            call(claim_name=f"{job_name}-ceph-pvc", read_only=False),
            client.V1PersistentVolumeClaimVolumeSource.call_args_list,
        )
        client.V1Volume.assert_called_with(
            name="archive-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value
        )
        client.V1PersistentVolumeClaimVolumeSource.assert_called_with(
            claim_name=f"{job_name}-archive-pvc", read_only=True
        )
        self.assertEqual(client.V1Volume.call_count, 2)
        self.assertEqual(client.V1PersistentVolumeClaimVolumeSource.call_count, 2)
        self.assertIn(
            call(
                name="job-watcher",
                image=f"ghcr.io/interactivereduction/jobwatcher@sha256:{watcher_sha}",
                env=[
                    client.V1EnvVar(name="DB_IP", value=db_ip),
                    client.V1EnvVar(name="DB_USERNAME", value=db_username),
                    client.V1EnvVar(name="DB_PASSWORD", value=db_password),
                    client.V1EnvVar(name="MAX_TIME_TO_COMPLETE_JOB", value=str(max_time_to_complete_job)),
                    client.V1EnvVar(name="CONTAINER_NAME", value=job_name),
                    client.V1EnvVar(name="JOB_NAME", value=job_name),
                    client.V1EnvVar(name="POD_NAME", value=job_name),
                ],
            ),
            client.V1Container.call_args_list,
        )
        self.assertIn(
            call(
                name=job_name,
                image=f"ghcr.io/interactivereduction/runner@sha256:{runner_sha}",
                args=[script],
                volume_mounts=[
                    client.V1VolumeMount(name="archive-mount", mount_path="/archive"),
                    client.V1VolumeMount(name="ceph-mount", mount_path="/output"),
                ],
            ),
            client.V1Container.call_args_list,
        )
        self.assertEqual(client.V1Container.call_count, 2)

        setup_ceph_pv.assert_called_once_with(
            job_name, ceph_creds_k8s_secret_name, ceph_creds_k8s_namespace, cluster_id, fs_name, ceph_mount_path
        )
        setup_ceph_pvc.assert_called_once_with(job_name=job_name, job_namespace=job_namespace)

    @mock.patch("jobcreator.job_creator._setup_archive_pv")
    @mock.patch("jobcreator.job_creator._setup_archive_pvc")
    @mock.patch("jobcreator.job_creator._setup_ceph_pv")
    @mock.patch("jobcreator.job_creator._setup_ceph_pvc")
    @mock.patch("jobcreator.job_creator.load_kubernetes_config")
    @mock.patch("jobcreator.job_creator.client")
    def test_jobcreator_spawn_job_dev_mode_true(
        self, client, _, setup_ceph_pvc, setup_ceph_pv, setup_archive_pvc, setup_archive_pv
    ):
        job_name = mock.MagicMock()
        script = mock.MagicMock()
        job_namespace = mock.MagicMock()
        ceph_creds_k8s_secret_name = mock.MagicMock()
        ceph_creds_k8s_namespace = mock.MagicMock()
        cluster_id = mock.MagicMock()
        fs_name = mock.MagicMock()
        ceph_mount_path = mock.MagicMock()
        reduction_id = random.randint(1, 100)
        max_time_to_complete_job = random.randint(1, 20000)
        db_ip = mock.MagicMock()
        db_username = mock.MagicMock()
        db_password = mock.MagicMock()
        runner_sha = mock.MagicMock()
        job_creator = JobCreator(mock.MagicMock(), True)

        job_creator.spawn_job(
            job_name,
            script,
            job_namespace,
            ceph_creds_k8s_secret_name,
            ceph_creds_k8s_namespace,
            cluster_id,
            fs_name,
            ceph_mount_path,
            reduction_id,
            max_time_to_complete_job,
            db_ip,
            db_username,
            db_password,
            runner_sha,
        )

        client.V1ObjectMeta.assert_called_once_with(
            name=job_name,
            annotations={
                "reduction-id": str(reduction_id),
                "pvs": str([setup_archive_pv.return_value]),
                "pvcs": str([setup_archive_pvc.return_value]),
                "kubectl.kubernetes.io/default-container": client.V1Container.return_value.name,
            },
        )
        self.assertIn(
            call(name="ceph-mount", empty_dir=client.V1EmptyDirVolumeSource.return_value),
            client.V1Volume.call_args_list,
        )
        client.V1EmptyDirVolumeSource.assert_called_once_with(size_limit="10000Mi")

        setup_ceph_pv.assert_not_called()
        setup_ceph_pvc.assert_not_called()
