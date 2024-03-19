"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by message to the RunMaker
"""

from kubernetes import client  # type: ignore[import]

from jobcreator.utils import logger, load_kubernetes_config


def _setup_archive_pv(job_name: str) -> str:
    """
    Sets up the archive PV using the loaded kubeconfig as a destination
    :param job_name: str, the name of the job needing an archive
    :return: str, the name of the archive PV
    """
    pv_name = f"{job_name}-archive-pv-smb"
    metadata = client.V1ObjectMeta(name=pv_name, annotations={"pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"})
    secret_ref = client.V1SecretReference(name="archive-creds", namespace="ir")
    csi = client.V1CSIPersistentVolumeSource(
        driver="smb.csi.k8s.io",
        read_only=True,
        volume_handle=pv_name,
        volume_attributes={"source": "//isisdatar55.isis.cclrc.ac.uk/inst$/"},
        node_stage_secret_ref=secret_ref,
    )
    spec = client.V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadOnlyMany"],
        persistent_volume_reclaim_policy="Retain",
        mount_options=["noserverino", "_netdev", "vers=2.1"],
        csi=csi,
    )
    archive_pv = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    client.CoreV1Api().create_persistent_volume(archive_pv)
    return pv_name


def _setup_archive_pvc(job_name: str, job_namespace: str) -> str:
    """
    Sets up the archive PVC using the loaded kubeconfig as a destination
    :param job_name: str, the name of the job that the PVC is made for
    :param job_namespace: str, the namespace that the job is in
    :return: str, the name of the PVC
    """
    pvc_name = f"{job_name}-archive-pvc"
    metadata = client.V1ObjectMeta(name=pvc_name)
    resources = client.V1ResourceRequirements(requests={"storage": "1000Gi"})
    spec = client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadOnlyMany"],
        resources=resources,
        volume_name=f"{job_name}-archive-pv-smb",
        storage_class_name="",
    )
    archive_pvc = client.V1PersistentVolumeClaim(
        api_version="v1", kind="PersistentVolumeClaim", metadata=metadata, spec=spec
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(namespace=job_namespace, body=archive_pvc)
    return pvc_name


def _setup_ceph_pv(
    job_name: str,
    ceph_creds_k8s_secret_name: str,
    ceph_creds_k8s_namespace: str,
    cluster_id: str,
    fs_name: str,
    ceph_mount_path: str,
) -> str:
    """
    Sets up the ceph deneb PV using the loaded kubeconfig as a destination
    :param job_name: str, the name of the job needing an ceph deneb PV mount
    :return: str, the name of the ceph deneb PV
    """
    pv_name = f"{job_name}-ceph-pv"
    metadata = client.V1ObjectMeta(name=pv_name)
    secret_ref = client.V1SecretReference(name=ceph_creds_k8s_secret_name, namespace=ceph_creds_k8s_namespace)
    csi = client.V1CSIPersistentVolumeSource(
        driver="cephfs.csi.ceph.com",
        node_stage_secret_ref=secret_ref,
        volume_handle=pv_name,
        volume_attributes={
            "clusterID": cluster_id,
            "mounter": "fuse",
            "fsName": fs_name,
            "staticVolume": "true",
            "rootPath": "/isis/instrument" + ceph_mount_path,
        },
    )
    spec = client.V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        storage_class_name="",
        access_modes=["ReadWriteMany"],
        persistent_volume_reclaim_policy="Retain",
        volume_mode="Filesystem",
        csi=csi,
    )
    ceph_pv = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    client.CoreV1Api().create_persistent_volume(ceph_pv)
    return pv_name


def _setup_ceph_pvc(job_name, job_namespace):
    """
    Sets up the ceph PVC using the loaded kubeconfig as a destination
    :param job_name: str, the name of the job that the PVC is made for
    :param job_namespace: str, the namespace that the job is in
    :return: str, the name of the PVC
    """
    pvc_name = f"{job_name}-ceph-pvc"
    metadata = client.V1ObjectMeta(name=pvc_name)
    resources = client.V1ResourceRequirements(requests={"storage": "1000Gi"})
    spec = client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadWriteMany"], resources=resources, volume_name=f"{job_name}-ceph-pv", storage_class_name=""
    )
    ceph_pvc = client.V1PersistentVolumeClaim(
        api_version="v1", kind="PersistentVolumeClaim", metadata=metadata, spec=spec
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(namespace=job_namespace, body=ceph_pvc)
    return pvc_name


class JobCreator:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self, watcher_sha: str, dev_mode: bool) -> None:
        """
        Takes the runner_sha and ensures that the kubernetes config is loaded before continuing.
        :param watcher_sha: str, The sha256 used for the watcher, often made by the watcher.D file in this repo's container folder
        :param dev_mode: bool, Whether the jobwatcher is launched in development mode
        :return: None
        """
        load_kubernetes_config()
        self.watcher_sha = watcher_sha
        self.dev_mode = dev_mode

    # pylint: disable=too-many-arguments
    def spawn_job(
        self,
        job_name: str,
        script: str,
        job_namespace: str,
        ceph_creds_k8s_secret_name: str,
        ceph_creds_k8s_namespace: str,
        cluster_id: str,
        fs_name: str,
        ceph_mount_path: str,
        reduction_id: int,
        max_time_to_complete_job: int,
        db_ip: str,
        db_username: str,
        db_password: str,
        runner_sha: str,
    ):
        """
        Takes the meta_data from the message and uses that dictionary for generating the deployment of the pod.
        :param job_name: The name that the job should be created as
        :param script: The script that should be executed
        :param job_namespace: The namespace that the job should be created in
        :param ceph_creds_k8s_secret_name: The secret name of the ceph credentials
        :param ceph_creds_k8s_namespace: The secret namespace of the ceph credentials
        :param cluster_id: The cluster id for the ceph cluster to connect to
        :param fs_name: The file system name for the ceph cluster
        :param ceph_mount_path: the path on the ceph cluster to mount
        :param reduction_id: The id used in the DB for the reduction
        :param max_time_to_complete_job: The maximum time to allow for completion of a job in seconds
        :param db_ip: The database ip to connect to
        :param db_username: the database username to use to connect
        :param db_password: the database password to use to connect
        :param runner_sha: The sha used for defining what version the runner is
        the containers have permission to use the directories required for outputting data.
        :return: A tuple containing the (job's name, PV name, and PVCs name)
        """
        logger.info("Creating PV and PVC for: %s", job_name)

        pv_names = []
        pvc_names = []
        # Setup PVs
        pv_names.append(_setup_archive_pv(job_name=job_name))
        if not self.dev_mode:
            pv_names.append(
                _setup_ceph_pv(
                    job_name, ceph_creds_k8s_secret_name, ceph_creds_k8s_namespace, cluster_id, fs_name, ceph_mount_path
                )
            )

        # Setup PVCs
        pvc_names.append(_setup_archive_pvc(job_name=job_name, job_namespace=job_namespace))
        if not self.dev_mode:
            pvc_names.append(_setup_ceph_pvc(job_name=job_name, job_namespace=job_namespace))

        # Create the Job
        logger.info("Spawning job: %s", job_name)
        main_container = client.V1Container(
            name=job_name,
            image=f"ghcr.io/interactivereduction/runner@sha256:{runner_sha}",
            args=[script],
            volume_mounts=[
                client.V1VolumeMount(name="archive-mount", mount_path="/archive"),
                client.V1VolumeMount(name="ceph-mount", mount_path="/output"),
            ],
        )

        watcher_container = client.V1Container(
            name="job-watcher",
            image=f"ghcr.io/interactivereduction/jobwatcher@sha256:{self.watcher_sha}",
            env=[
                client.V1EnvVar(name="DB_IP", value=db_ip),
                client.V1EnvVar(name="DB_USERNAME", value=db_username),
                client.V1EnvVar(name="DB_PASSWORD", value=db_password),
                client.V1EnvVar(name="MAX_TIME_TO_COMPLETE_JOB", value=str(max_time_to_complete_job)),
                client.V1EnvVar(name="CONTAINER_NAME", value=job_name),
                client.V1EnvVar(name="JOB_NAME", value=job_name),
                client.V1EnvVar(name="POD_NAME", value=job_name),
            ],
        )

        if not self.dev_mode:
            ceph_volume = client.V1Volume(
                name="ceph-mount",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"{job_name}-ceph-pvc", read_only=False
                ),
            )
        else:
            ceph_volume = client.V1Volume(
                name="ceph-mount", empty_dir=client.V1EmptyDirVolumeSource(size_limit="10000Mi")
            )

        pod_spec = client.V1PodSpec(
            service_account_name="jobwatcher",
            containers=[main_container, watcher_container],
            restart_policy="Never",
            tolerations=[client.V1Toleration(key="queue-worker", effect="NoSchedule", operator="Exists")],
            volumes=[
                client.V1Volume(
                    name="archive-mount",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=f"{job_name}-archive-pvc", read_only=True
                    ),
                ),
                ceph_volume,
            ],
        )

        template = client.V1PodTemplateSpec(spec=pod_spec)

        spec = client.V1JobSpec(
            template=template,
            backoff_limit=0,
            ttl_seconds_after_finished=21600,  # 6 hours
        )

        metadata = client.V1ObjectMeta(
            name=job_name,
            annotations={
                "reduction-id": str(reduction_id),
                "pvs": str(pv_names),
                "pvcs": str(pvc_names),
                "kubectl.kubernetes.io/default-container": main_container.name,
            },
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=metadata,
            spec=spec,
        )
        client.BatchV1Api().create_namespaced_job(namespace=job_namespace, body=job)
