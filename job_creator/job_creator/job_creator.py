"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by message to the RunMaker
"""

from kubernetes import client  # type: ignore[import]

from utils import logger, load_kubernetes_config


def _setup_archive_pv(job_name) -> str:
    pv_name = f"{job_name}-archive-pv-smb"
    archive_pv = client.V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata={
            "annotations": {
                "pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io",
            },
            "name": pv_name,
        },
        spec={
            "capacity": {"storage": "1000Gi"},
            "accessModes": ["ReadOnlyMany"],
            "persistentVolumeReclaimPolicy": "Retain",
            "mountOptions": [
                "noserverino",
                "_netdev",
                "vers=2.1"
            ],
            "csi": {
                "driver": "smb.csi.k8s.io",
                "readOnly": True,
                "volumeHandle": pv_name,
                "volumeAttributes": {"source": "//isisdatar55.isis.cclrc.ac.uk/inst$/"},
                "nodeStageSecretRef": {
                    "name": "archive-creds",
                    "namespace": "ir",
                },
            },
        },
    )
    client.CoreV1Api().create_persistent_volume(archive_pv)
    return pv_name


def _setup_archive_pvc(job_name, job_namespace) -> str:
    pvc_name = f"{job_name}-archive-pvc"
    archive_pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata={"name": pvc_name},
        spec={
            "accessModes": ["ReadOnlyMany"],
            "resources": {"requests": {"storage": "1000Gi"}},
            "volumeName": f"{pvc_name}-smb",
            "storageClassName": "",
        },
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(
        namespace=job_namespace, body=archive_pvc
    )
    return pvc_name


def _setup_ceph_pv(job_name: str, ceph_creds_k8s_secret_name: str,
                   ceph_creds_k8s_namespace: str, cluster_id: str, fs_name: str,
                   ceph_mount_path: str) -> str:
    pv_name = f"{job_name}-ceph-pv"
    ceph_pv = client.V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata={
            "name": pv_name,
        },
        spec={
            "capacity": {"storage": "1000Gi"},
            "storageClassName": "",
            "accessModes": ["ReadWriteMany"],
            "persistentVolumeReclaimPolicy": "Retain",
            "volumeMode": "Filesystem",
            "csi": {
                "driver": "cephfs.csi.ceph.com",
                "nodeStageSecretRef": {"name": ceph_creds_k8s_secret_name,
                                       "namespace": ceph_creds_k8s_namespace},
                "volumeHandle": pv_name,
                "volumeAttributes": {
                    "clusterID": cluster_id,
                    "mounter": "fuse",
                    "fsName": fs_name,
                    "staticVolume": "true",
                    "rootPath": "/isis/instrument" + ceph_mount_path
                },
            },
        },
    )
    client.CoreV1Api().create_persistent_volume(ceph_pv)
    return pv_name


def _setup_ceph_pvc(job_name, job_namespace):
    pvc_name = f"{job_name}-ceph-pvc"
    ceph_pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata={"name": pvc_name},
        spec={
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1000Gi"}},
            "volumeName": f"{job_name}-ceph-pv",
            "storageClassName": "",
        },
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(
        namespace=job_namespace, body=ceph_pvc
    )
    return pvc_name


class JobCreator:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self, runner_sha: str, dev_mode: bool) -> None:
        """
        Takes the runner_sha and ensures that the kubernetes config is loaded before continuing.
        :param runner_sha: The sha256 used for the runner, often made by the runner.D file in this repo's container
        folder
        """
        load_kubernetes_config()
        self.runner_sha = runner_sha
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
                  reduction_id: int):
        """
        Takes the meta_data from the message and uses that dictionary for generating the deployment of the pod.
        :param job_name: The name that the job should be created as
        :param script: The script that should be executed
        :param job_namespace: The namespace that the job should be created in
        :param user_id: The autoreduce user's user id, this is used primarily for mounting CEPH and will ensure that
        :param ceph_creds_k8s_secret_name:
        :param ceph_creds_k8s_namespace:
        :param cluster_id:
        :param fs_name:
        :param ceph_mount_path:
        :param reduction_id:
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
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata={"name": job_name,
                      "annotations": {
                          "reduction-id": str(reduction_id),
                          "pvs": pv_names,
                          "pvcs": pvc_names,
                      }
                      },
            spec={
                "backoffLimit": 0,
                "ttlSecondsAfterFinished": 21600,  # 6 hours
                "template": {
                    "spec": {
                        "security_context": {
                            "runAsUser": user_id,
                        },
                        "containers": [
                            {
                                "name": job_name,
                                "image": f"ghcr.io/interactivereduction/runner@sha256:{self.runner_sha}",
                                "args": [script],
                                "volumeMounts": [
                                    {"name": "archive-mount", "mountPath": "/archive"},
                                    {"name": "ceph-mount", "mountPath": "/output"},
                                ],
                            }
                        ],
                        "restartPolicy": "Never",
                        "tolerations": [{"key": "queue-worker", "effect": "NoSchedule", "operator": "Exists"}],
                        "volumes": [
                            {
                                "name": "archive-mount",
                                "persistentVolumeClaim": {"claimName": f"{job_name}-archive-pvc", "readOnly": True},
                            },
                            {
                                "name": "ceph-mount",
                                "persistentVolumeClaim": {"claimName": f"{job_name}-ceph-pvc", "readOnly": False}
                            },
                        ],
                    },
                },
            },
        )
        if self.dev_mode:
            # Use an emptyDir instead of a ceph mount for the jobs, this will de deleted when the pod dies.
            job.spec["template"]["spec"]["volumes"][1] = {
                "name": "ceph-mount",
                "emptyDir": {
                    "sizeLimit": "10000Mi"
                }
            }

        client.BatchV1Api().create_namespaced_job(namespace=job_namespace, body=job)
