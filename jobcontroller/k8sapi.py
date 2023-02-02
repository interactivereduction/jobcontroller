"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by kafka message to the RunMaker
"""
from kubernetes import client, config  # type: ignore


class K8sAPI:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self) -> None:
        config.load_incluster_config()

    def spawn_job(self, job_name: str, script: str, ceph_path: str) -> str:
        """
        Takes the meta_data from the kafka message and uses that dictionary for generating the deployment of the pod.
        """
        from jobcontroller.jobcontroller import logger

        logger.info("Spawning job: %s", job_name)
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata={"name": job_name},
            spec={
                "containers": [
                    {
                        "name": job_name,
                        "image": "python:3.10",
                        "command": ["python"],
                        "args": ["-c", script],
                        "volumeMounts": [
                            {"name": "archive-mount", "mountPath": "/archive"},
                            {"name": "ceph-mount", "mountPath": "/output"},
                        ],
                    }
                ],
                "volumes": [
                    {"name": "archive-mount", "hostPath": {"type": "Directory", "path": "/archive"}},
                    {"name": "ceph-mount", "hostPath": {"type": "Directory", "path": ceph_path}},
                ],
            },
        )
        response = client.BatchV1Api().create_namespaced_job(namespace="ir-jobs", body=job)
        return response.metadata.name
