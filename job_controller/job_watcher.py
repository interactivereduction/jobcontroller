"""
Watch a kubernetes job, and when it ends notify a kafka topic
"""
import json
from json import JSONDecodeError
from typing import Any, Optional, Dict

from kubernetes import client, watch  # type: ignore[import]
from kubernetes.client import V1Job  # type: ignore[import]

from job_controller.database.state_enum import State
from job_controller.database.db_updater import DBUpdater
from job_controller.utils import logger, load_kubernetes_config


class JobWatcher:  # pylint: disable=too-many-instance-attributes
    """
    Watch a kubernetes job, and when it ends notify a kafka topic
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        job_name: str,
        namespace: str,
        kafka_ip: str,
        ceph_path: str,
        db_updater: DBUpdater,
        db_reduction_id: int,
        job_script: str,
        reduction_inputs: Dict[str, Any],
    ):
        self.job_name = job_name
        self.namespace = namespace
        self.kafka_ip = kafka_ip
        self.ceph_path = ceph_path
        self.db_updater = db_updater
        self.db_reduction_id = db_reduction_id
        self.job_script = job_script
        self.reduction_inputs = reduction_inputs
        load_kubernetes_config()  # Should already be called in job creator, this is a defensive call.

    @staticmethod
    def grab_pod_name_from_job_name_in_namespace(job_name: str, job_namespace: str) -> Optional[str]:
        """
        Find the name of the pod, given a job name and a job namespace, this works on the assumtion that there is
        only 1 pod in a job.
        :param job_name: The name of the job that contains the pod you want
        :param job_namespace: The name of the namespace that the job lives in.
        :return: str or None, the name of the pod for the passed values. Will return None when no pod could be found
        for passed values
        """
        v1 = client.CoreV1Api()
        pods = v1.list_namespaced_pod(job_namespace)
        for pod in pods.items:
            for owner in pod.metadata.owner_references:
                if owner.name == job_name:
                    return str(pod.metadata.name)
        return None

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
                self.process_event(event)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Job watching failed due to an exception: %s", str(exception))
            return
        logger.info("Ending JobWatcher for job %s", self.job_name)

    def process_event(self, event: Dict[str, Any]) -> None:
        """
        Process events from the stream, send the success to a success event processing, send failed to failed event
        processing.
        :param event: The event to be processed
        :return:
        """
        job = event["object"]
        if self.job_name in job.metadata.name:
            if job.status.succeeded == 1:
                # Job passed
                self.process_event_success()
            elif job.status.failed == 1:
                # Job failed
                self.process_event_failed(job)

    def process_event_failed(self, job: V1Job) -> None:
        """
        Process the event that failed, and notify kafka
        :param job: The job that has failed
        :return:
        """
        logger.info("Job %s has %s, with message: %s", self.job_name, job.status.phase, job.status.message)
        self.db_updater.add_completed_run(
            db_reduction_id=self.db_reduction_id,
            state=State.Error,
            status_message=job.status.message,
            output_files=[],
            reduction_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
        )

    def process_event_success(self) -> None:
        """
        Process a successful event, grab the required data and logged output that will notify kafka
        :return:
        """
        pod_name = self.grab_pod_name_from_job_name_in_namespace(job_name=self.job_name, job_namespace=self.namespace)
        if pod_name is None:
            raise TypeError(
                f"Pod name can't be None, {self.job_name} name and {self.namespace} "
                f"namespace returned None when looking for a pod."
            )
        v1_core = client.CoreV1Api()
        logs = v1_core.read_namespaced_pod_log(name=pod_name, namespace=self.namespace)
        output = logs.split("\n")[-2]  # Get second last line (last line is empty)
        logger.info("Job %s has been completed with output: %s", self.job_name, output)
        # Convert message from JSON string to python dict
        try:
            job_output = json.loads(output)
        except JSONDecodeError as exception:
            logger.error("Last message from job is not a JSON string: %s", str(exception))
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }
        except TypeError as exception:
            logger.error("Last message from job is not a string: %s", str(exception))
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }

        # Grab status from output
        status = job_output.get("status", "Unsuccessful")
        status_message = job_output.get("status_message", "")
        output_files = job_output.get("output_files", [])
        self.db_updater.add_completed_run(
            db_reduction_id=self.db_reduction_id,
            state=status,
            status_message=status_message,
            output_files=output_files,
            reduction_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
        )

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err:
            logger.error("Delivery failed for message %s: %s", msg.value(), err)
        else:
            logger.info("Delivered message to %s [%s]", msg.topic(), msg.partition())
