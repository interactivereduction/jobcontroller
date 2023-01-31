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

    def spawn_pod(self, filename: str, kafka_ip: str, rb_number: str, instrument_name: str) -> None:
        """
        Takes the meta_data from the kafka message and uses that dictionary for generating the deployment of the pod.
        """
        from jobcontroller.jobcontroller import logger

        logger.info("Spawning pod with metadata: %s", filename)
        job_name = f"run-{filename}"
        pod = client.V1Pod(
            metadata={"name": job_name},
            spec={
                "containers": [
                    {
                        "name": job_name,
                        # TODO  update this to include a sha256
                        "image": "ir-mantid-runner@sha256:e82d7bafaa82cd58b248086e4c161117c5aacef393058d2e03d20d85d3c74b01",
                        "env": [{"name": "KAFKA_IP", "value": kafka_ip},
                                {"name": "RUN_FILENAME", "value": filename},
                                {"name": "IR_API_IP", "value": "irapi.ir.svc.cluster.local"},
                                {"name": "RB_NUMBER", "value": rb_number},
                                {"name": "INSTRUMENT_NAME", "value": instrument_name}
                                ],
                        "volumeMounts": [
                            {"name": "archive-mount", "mountPath": "/archive"},
                            {"name": "ceph-mount", "mountPath": "/ceph"},
                        ],
                    }
                ],
                "volumes": [
                    {"name": "archive-mount", "hostPath": {"type": "Directory", "path": "/archive"}},
                    {"name": "ceph-mount", "hostPath": {"type": "Directory", "path": "/ceph"}},
                ],
            }
        )
        client.CoreV1Api().create_namespaced_pod(namespace="ir-runs", body=pod)
