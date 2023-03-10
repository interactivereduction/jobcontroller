"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by kafka message to the RunMaker
"""
from kubernetes import client  # type: ignore

from jobcontroller.utils import logger, load_kubernetes_config


class JobCreator:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self) -> None:
        load_kubernetes_config()

    @staticmethod
    def spawn_job(job_name: str, script: str, ceph_path: str, job_namespace: str) -> str:
        """
        Takes the meta_data from the kafka message and uses that dictionary for generating the deployment of the pod.
        :param job_name: The name that the job should be created as
        :param script: The script that should be executed
        :param ceph_path: The path to which that should be mounted at /output in the job, this is expected to be the
        RBNumber folder that data will be dumped into.
        :param job_namespace: The namespace that the job should be created inside of
        :return: The response for the actual job's name
        """
        logger.info("Spawning job: %s", job_name)
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata={"name": job_name},
            spec={
                "ttlSecondsAfterFinished": 21600,  # 6 hours
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": job_name,
                                "image": "ghcr.io/interactivereduction/runner@sha256:"
                                "231d92480ee9338104836a83d89de0bd6c09cf3f7c1154cb99eeb643e1b58e94",
                                "command": ["python"],
                                "args": ["-c", script],
                                "volumeMounts": [
                                    {"name": "archive-mount", "mountPath": "/archive"},
                                    {"name": "ceph-mount", "mountPath": "/output"},
                                ],
                            }
                        ],
                        "restartPolicy": "Never",
                        "tolerations": [{"key": "queue-worker", "effect": "NoSchedule", "operator": "Exists"}],
                        "volumes": [
                            {"name": "archive-mount", "hostPath": {"type": "Directory", "path": "/archive"}},
                            {"name": "ceph-mount", "hostPath": {"type": "Directory", "path": ceph_path}},
                        ],
                    },
                },
            },
        )
        response = client.BatchV1Api().create_namespaced_job(namespace=job_namespace, body=job)
        return response.metadata.name
