import json
from json import JSONDecodeError
from typing import Any, List

from confluent_kafka import Producer
from kubernetes import client, watch

from jobcontroller.utils import logger, add_ceph_path_to_output_files, load_kubernetes_config


class JobWatcher:
    def __init__(self, job_name: str, namespace: str, kafka_ip: str, ceph_path: str):
        self.job_name = job_name
        self.namespace = namespace
        self.kafka_ip = kafka_ip
        self.ceph_path = ceph_path
        load_kubernetes_config()  # Should already be called in K8sAPI, this is a defensive call.

    @staticmethod
    def grab_pod_name_from_job_name_in_namespace(job_name: str, job_namespace: str) -> str:
        v1 = client.CoreV1Api()
        pods = v1.list_namespaced_pod(job_namespace)
        for pod in pods.items:
            for owner in pod.metadata.owner_references:
                if owner.name == job_name:
                    return pod.metadata.name

    def watch(self) -> None:
        """
        This is the main function responsible for watching a job, and it's responsible for calling the function that
        will notify kafka.
        :return:
        """
        logger.info("Starting JobWatcher for job %s, and in namespace: %s", self.job_name, self.namespace)
        v1 = client.BatchV1Api()
        watch_ = watch.Watch()
        try:
            for event in watch_.stream(v1.list_job_for_all_namespaces):
                job = event['object']
                if self.job_name in job.metadata.name:
                    if job.status.succeeded == 1:
                        # Job passed
                        pod_name = self.grab_pod_name_from_job_name_in_namespace(job_name=self.job_name,
                                                                                 job_namespace=self.namespace)
                        if pod_name is None:
                            raise TypeError(f"Pod name can't be None, {self.job_name} name and {self.namespace} "
                                            f"namespace returned None when looking for a pod.")
                        v1Core = client.CoreV1Api()
                        logs = v1Core.read_namespaced_pod_log(
                            name=pod_name,
                            namespace=self.namespace)
                        output = logs.split('\n')[-2]  # Get second last line (last line is empty)
                        logger.info("Job %s has been completed with output: %s", self.job_name, output)
                        # Convert message from JSON string to python dict
                        try:
                            job_output = json.loads(output)
                        except JSONDecodeError as exception:
                            logger.error("Last message from job is not a JSON string: %s", str(exception))
                            job_output = {"status": "Unsuccessful", "output_files": [], "status_message":
                                          f"{str(exception)}"}

                        # Grab status from output
                        status = job_output.get("status", "Unsuccessful")
                        status_message = job_output.get("status_message", "")
                        output_files = job_output.get("output_files", [])
                        self.notify_kafka(status=status, status_message=status_message, output_files=output_files)
                    elif job.status.failed == 1:
                        # Job failed
                        logger.info("Job %s has %s, with message: %s", self.job_name, job.status.phase,
                                    job.status.message)
                        status = "Error"
                        status_message = job.status.message
                        self.notify_kafka(status=status, status_message=status_message)
        except Exception as exception:
            logger.error("Job watching failed due to an exception: %s", str(exception))
            return
        logger.info("Ending JobWatcher for job %s", self.job_name)

    def notify_kafka(self, status: str, status_message: str = "", output_files: List[str] = None) -> None:
        """
        Connect to kafka, send message and disconnect from kafka
        :param status: The end state of the Run
        :param status_message: The status message to be sent when status is not "Success"
        :param output_files: The names of files to be sent when status is "Success"
        :return: None
        """
        logger.info("Notifying kafka of job %s finished with status %s", self.job_name, status)
        producer = Producer(
            {
                "bootstrap.servers": self.kafka_ip,
                "client.id": f"runner-{self.job_name}",
            }
        )

        logger.info("Creating message for kafka")
        if output_files is not None:
            outputs = add_ceph_path_to_output_files(ceph_path=self.ceph_path, output_files=output_files)
        else:
            outputs = []

        if "outputs" not in locals() and "outputs" not in globals():
            logger.info("Outputs variable not set by script run, status changed to Unsuccessful")
            status = "Unsuccessful"
            status_message = "Outputs not received, nothing to update."

        if status == "Error":
            value = json.dumps({"status": status, "status message": status_message})
        elif status == "Successful":
            value = json.dumps({"status": status, "run output": outputs})
        else:
            value = json.dumps({"status": status, "status message": status_message})

        producer.produce("completed-runs", value=value, callback=self._delivery_callback)
        producer.flush()
        logger.info("Kafka notified with message: %s", value)

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err:
            logger.error("Delivery failed for message %s: %s", msg.value(), err)
        else:
            logger.info("Delivered message to %s [%s]", msg.topic(), msg.partition())
