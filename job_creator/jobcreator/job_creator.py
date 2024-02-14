"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by message to the RunMaker
"""

from kubernetes import client  # type: ignore[import]

from jobcreator.utils import logger, load_kubernetes_config
from kubernetes.client import V1Container, V1PodSpec, V1PodTemplateSpec, V1JobSpec, V1ObjectMeta, V1SecurityContext, \
    V1Toleration, V1Volume, V1PersistentVolumeClaimVolumeSource, V1VolumeMount, V1EnvVar, V1EmptyDirVolumeSource, \
    V1PersistentVolumeSpec, V1CSIPersistentVolumeSource, V1SecretReference, V1PersistentVolumeClaimSpec, \
    V1ResourceRequirements, V1PersistentVolumeClaim, V1PersistentVolume


def _setup_archive_pv(job_name) -> str:
    pv_name = f"{job_name}-archive-pv-smb"
    metadata = V1ObjectMeta(name=pv_name,
                            annotations={
                                "pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"
                            })
    secret_ref = V1SecretReference(name="archive-creds",
                                   namespace="ir")
    csi = V1CSIPersistentVolumeSource(driver="smb.csi.k8s.io", read_only=True, volume_handle=pv_name, volume_attributes={"source": "//isisdatar55.isis.cclrc.ac.uk/inst$/"}, node_stage_secret_ref=secret_ref)
    spec = V1PersistentVolumeSpec(capacity={"storage": "1000Gi"},
                                  access_modes=["ReadOnlyMany"],
                                  persistent_volume_reclaim_policy="Retain",
                                  mount_options=["noserverino", "_netdev", "vers=2.1"],
                                  csi=csi)
    archive_pv = V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata=metadata,
        spec=spec
    )
    client.CoreV1Api().create_persistent_volume(archive_pv)
    return pv_name


def _setup_archive_pvc(job_name, job_namespace) -> str:
    pvc_name = f"{job_name}-archive-pvc"
    metadata = V1ObjectMeta(name=pvc_name)
    resources = V1ResourceRequirements(requests={"storage": "1000Gi"})
    spec = V1PersistentVolumeClaimSpec(access_modes=["ReadOnlyMany"],
                                       resources=resources,
                                       volume_name=f"{job_name}-archive-pv-smb",
                                       storage_class_name="")
    archive_pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=metadata,
        spec=spec
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(
        namespace=job_namespace, body=archive_pvc
    )
    return pvc_name


def _setup_ceph_pv(job_name: str, ceph_creds_k8s_secret_name: str,
                   ceph_creds_k8s_namespace: str, cluster_id: str, fs_name: str,
                   ceph_mount_path: str) -> str:
    pv_name = f"{job_name}-ceph-pv"
    metadata = V1ObjectMeta(name=pv_name)
    secret_ref = V1SecretReference(name=ceph_creds_k8s_secret_name,
                                   namespace=ceph_creds_k8s_namespace)
    csi = V1CSIPersistentVolumeSource(driver="cephfs.csi.ceph.com",
                                      node_stage_secret_ref=secret_ref,
                                      volume_handle=pv_name,
                                      volume_attributes={
                                        "clusterID": cluster_id,
                                        "mounter": "fuse",
                                        "fsName": fs_name,
                                        "staticVolume": "true",
                                        "rootPath": "/isis/instrument" + ceph_mount_path
                                      })
    spec = V1PersistentVolumeSpec(capacity={"storage": "1000Gi"}, storage_class_name="",
                                  access_modes=["ReadWriteMany"],
                                  persistent_volume_reclaim_policy="Retain",
                                  volume_mode="Filesystem", csi=csi)
    ceph_pv = V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata=metadata,
        spec=spec
    )
    client.CoreV1Api().create_persistent_volume(ceph_pv)
    return pv_name


def _setup_ceph_pvc(job_name, job_namespace):
    pvc_name = f"{job_name}-ceph-pvc"
    metadata = V1ObjectMeta(name=pvc_name)
    resources = V1ResourceRequirements(requests={"storage": "1000Gi"})
    spec = V1PersistentVolumeClaimSpec(access_modes=["ReadWriteMany"],
                                       resources=resources,
                                       volume_name=f"{job_name}-ceph-pv",
                                       storage_class_name="")
    ceph_pvc = V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=metadata,
        spec=spec
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(
        namespace=job_namespace, body=ceph_pvc
    )
    return pvc_name


class JobCreator:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self, watcher_sha: str, dev_mode: bool) -> None:
        """
        Takes the runner_sha and ensures that the kubernetes config is loaded before continuing.
        :param runner_sha: The sha256 used for the runner, often made by the runner.D file in this repo's container
        folder
        """
        load_kubernetes_config()
        self.watcher_sha = watcher_sha
        self.dev_mode = dev_mode

    def _setup_runner_files_pv(self, job_name: str, ceph_creds_k8s_secret_name: str,
                               ceph_creds_k8s_namespace: str, cluster_id: str,
                               fs_name: str, ceph_mount_path: str) -> str:
        pass

    def _setup_runner_files_pvc(self, job_name, job_namespace) -> str:
        pass

    # pylint: disable=too-many-arguments
    def spawn_job(self, job_name: str, script: str, job_namespace: str, user_id: str,
                  ceph_creds_k8s_secret_name: str, ceph_creds_k8s_namespace: str,
                  cluster_id: str, fs_name: str, ceph_mount_path: str,
                  reduction_id: int, max_time_to_complete_job: int, db_ip: str,
                  db_username: str, db_password: str, runner_sha: str):
        """
        Takes the meta_data from the message and uses that dictionary for generating the deployment of the pod.
        :param job_name: The name that the job should be created as
        :param script: The script that should be executed
        :param job_namespace: The namespace that the job should be created in
        :param user_id: The autoreduce user's user id, this is used primarily for mounting CEPH and will ensure that ceph can be used to output data to. Will be cast to an int.
        :param ceph_creds_k8s_secret_name:
        :param ceph_creds_k8s_namespace:
        :param cluster_id:
        :param fs_name:
        :param ceph_mount_path:
        :param reduction_id:
        :param max_time_to_complete_job:
        :param db_ip:
        :param db_username:
        :param db_password:
        :param runner_sha:
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
                _setup_ceph_pv(job_name, ceph_creds_k8s_secret_name,
                               ceph_creds_k8s_namespace, cluster_id, fs_name,
                               ceph_mount_path))
        pv_names.append(
            self._setup_runner_files_pv(job_name, ceph_creds_k8s_secret_name,
                                        ceph_creds_k8s_namespace, cluster_id, fs_name,
                                        ceph_mount_path))

        # Setup PVCs
        pvc_names.append(
            _setup_archive_pvc(job_name=job_name, job_namespace=job_namespace))
        if not self.dev_mode:
            pvc_names.append(
                _setup_ceph_pvc(job_name=job_name, job_namespace=job_namespace))
        pvc_names.append(
            self._setup_runner_files_pvc(job_name=job_name,
                                         job_namespace=job_namespace))

        # Create the Job
        logger.info("Spawning job: %s", job_name)
        main_container = (
            V1Container(name=job_name,
                        image=f"ghcr.io/interactivereduction/runner@sha256:{runner_sha}",
                        args=[script],
                        volume_mounts=[
                            V1VolumeMount(name="archive-mount", mount_path="/archive"),
                            V1VolumeMount(name="ceph-mount", mount_path="/output")
                        ])
        )

        watcher_container = (
            V1Container(name="job-watcher",
                        image=f"ghcr.io/interactivereduction/jobwatcher@sha256:{self.watcher_sha}",
                        env=[
                            V1EnvVar(name="DB_IP", value=db_ip),
                            V1EnvVar(name="DB_USERNAME", value=db_username),
                            V1EnvVar(name="DB_PASSWORD", value=db_password),
                            V1EnvVar(name="MAX_TIME_TO_COMPLETE_JOB",
                                     value=str(max_time_to_complete_job)),
                            V1EnvVar(name="CONTAINER_NAME", value=job_name),
                            V1EnvVar(name="JOB_NAME", value=job_name),
                            V1EnvVar(name="POD_NAME", value=job_name),
                        ])
        )

        if not self.dev_mode:
            ceph_volume = V1Volume(name="ceph-mount", persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=f"{job_name}-ceph-pvc", read_only=False))
        else:
            ceph_volume = V1Volume(name="ceph-mount", empty_dir=V1EmptyDirVolumeSource(size_limit="10000Mi"))

        pod_spec = V1PodSpec(
            service_account_name="jobwatcher",
            containers=[main_container, watcher_container],
            restart_policy="Never",
            security_context=V1SecurityContext(run_as_user=int(user_id)),
            tolerations=[V1Toleration(key="queue-worker", effect="NoSchedule", operator="Exists")],
            volumes=[
                V1Volume(name="archive-mount",
                         persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                             claim_name=f"{job_name}-archive-pvc", read_only=True)),
                ceph_volume
            ]
        )

        template = V1PodTemplateSpec(
            spec=pod_spec
        )

        spec = V1JobSpec(
            template=template,
            backoff_limit=0,
            ttl_seconds_after_finished=21600,  # 6 hours
        )

        metadata = V1ObjectMeta(
            name=job_name,
            annotations={
                "reduction-id": str(reduction_id),
                "pvs": str(pv_names),
                "pvcs": str(pvc_names),
            }
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=metadata,
            spec=spec,
        )

        # job = client.V1Job(
        #     api_version="batch/v1",
        #     kind="Job",
        #     metadata={
        #         "name": job_name,
        #         "annotations": {
        #             "reduction-id": str(reduction_id),
        #             "pvs": pv_names,
        #             "pvcs": pvc_names,
        #             }
        #         },
        #     spec={
        #         "backoffLimit": 0,
        #         "ttlSecondsAfterFinished": 21600,  # 6 hours
        #         "template": {
        #             "spec": {
        #                 "security_context": {
        #                     "runAsUser": user_id,
        #                 },
        #                 "containers": [
        #                     {
        #                         "name": job_name,
        #                         "image": f"ghcr.io/interactivereduction/runner@sha256:{runner_sha}",
        #                         "args": [script],
        #                         "volumeMounts": [
        #                             {"name": "archive-mount", "mountPath": "/archive"},
        #                             {"name": "ceph-mount", "mountPath": "/output"},
        #                         ],
        #                     },
        #                     {
        #                         "name": "job-watcher",
        #                         "image": f"ghcr.io/interactivereduction/jobwatcher@sha256:{self.watcher_sha}",
        #                         "env": [
        #                             {"name": "DB_IP", "value": db_ip},
        #                             {"name": "DB_USERNAME", "value": db_username},
        #                             {"name": "DB_PASSWORD", "value": db_password},
        #                             {"name": "MAX_TIME_TO_COMPLETE_JOB", "value": str(max_time_to_complete_job)},
        #                             {"name": "CONTAINER_NAME", "value": job_name},
        #                             {"name": "JOB_NAME", "value": job_name},
        #                             {"name": "POD_NAME", "value": job_name},
        #                         ]
        #                     }
        #                 ],
        #                 "restartPolicy": "Never",
        #                 "tolerations": [{"key": "queue-worker", "effect": "NoSchedule", "operator": "Exists"}],
        #                 "volumes": [
        #                     {
        #                         "name": "archive-mount",
        #                         "persistentVolumeClaim": {"claimName": f"{job_name}-archive-pvc", "readOnly": True},
        #                     },
        #                     {
        #                         "name": "ceph-mount",
        #                         "persistentVolumeClaim": {"claimName": f"{job_name}-ceph-pvc", "readOnly": False}
        #                     },
        #                 ],
        #             },
        #         },
        #     },
        # )
        # if self.dev_mode:
        #     # Use an emptyDir instead of a ceph mount for the jobs, this will de deleted when the pod dies.
        #     job.spec["template"]["spec"]["volumes"][1] = {
        #         "name": "ceph-mount",
        #         "emptyDir": {
        #             "sizeLimit": "10000Mi"
        #         }
        #     }

        client.BatchV1Api().create_namespaced_job(namespace=job_namespace, body=job)
