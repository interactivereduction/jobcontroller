"""
Watch a kubernetes job, and when it ends notify a message broker station/topic
"""
import datetime
import json
import os
from json import JSONDecodeError
from time import sleep
from typing import Any, Optional, Tuple

from kubernetes import client, watch  # type: ignore[import]
from kubernetes.client import V1Job, V1Pod, V1Container  # type: ignore[import]

from jobwatcher.database.state_enum import State
from jobwatcher.database.db_updater import DBUpdater
from jobwatcher.utils import logger


def clean_up_pvcs_for_job(job: V1Job, namespace: str) -> None:
    v1 = client.CoreV1Api()
    pvcs_to_delete_str = job.metadata.annotations["pvcs"]
    pvcs_to_delete = pvcs_to_delete_str.strip('][').split(', ')
    for pvc in pvcs_to_delete:
        v1.delete_namespaced_persistent_volume_claim(pvc, namespace=namespace)
        logger.info(f"Deleted pv: {pvc}")


def clean_up_pvs_for_job(job: V1Job) -> None:
    v1 = client.CoreV1Api()
    pvs_to_delete_str = job.metadata.annotations["pvs"]
    pvs_to_delete = pvs_to_delete_str.strip('][').split(', ')
    for pv in pvs_to_delete:
        v1.delete_persistent_volume(pv)
        logger.info(f"Deleted pv: {pv}")


def _find_pod_from_partial_name(partial_pod_name: str, namespace: str) -> Optional[V1Pod]:
    v1 = client.CoreV1Api()
    pods_in_ir = v1.list_namespaced_pod(namespace=namespace)
    for pod in pods_in_ir.items:
        if partial_pod_name in pod.metadata.name:
            return pod
    return None


class JobWatcher:  # pylint: disable=too-many-instance-attributes
    """
    Watch a kubernetes job, and when it ends notify a message broker station/topic
    """

    def __init__(self, job_name: str, partial_pod_name: str, container_name: str,
                 db_updater: DBUpdater, max_time_to_complete: int):
        self.namespace = os.environ.get("JOB_NAMESPACE", "ir")
        self.db_updater = db_updater
        self.max_time_to_complete = max_time_to_complete
        self.done_watching = False

        v1_batch = client.BatchV1Api()
        self.job = v1_batch.read_namespaced_job(job_name, namespace=self.namespace)
        self.pod = _find_pod_from_partial_name(partial_pod_name, namespace=self.namespace)
        if self.pod is None:
            raise ValueError(f"The pod could not be found using partial pod name: {partial_pod_name}")
        self.container_name = container_name

    def watch(self) -> None:
        """
        This is the main function responsible for watching a job, and it's responsible for calling the function that
        will notify the message broker.
        :return:
        """
        logger.info("Starting job watcher, scanning for new job states.")
        while not self.done_watching:
            self.check_for_changes()
            # Brief sleep to facilitate reducing CPU and network load
            sleep(0.1)

    def check_for_changes(self) -> None:
        """
        Check if the job has a change for which we need to react to, such as the pod
        having finished or a job has stalled.
        :return:
        """
        if self.check_for_job_complete():
            self.cleanup_job()
        elif self.check_for_pod_stalled():
            self.cleanup_job()

    def check_for_job_complete(self) -> bool:
        """
        Checks if the job has finished by checking its status, if it failed then we
        need to process that, and the same for a success.
        :return:
        """
        if self.job.status.succeeded == 1:
            # Job has succeeded
            self.process_job_success()
            return True
        elif self.job.status.failed == 1:
            # Job has failed
            self.process_job_failed()
            return True
        return False

    def check_for_pod_stalled(self) -> bool:
        """
        The way this checks if a job is stalled is by checking if there has been no new
        logs for the last 30 minutes, or if the job has taken over 6 hours to complete.
        Long term 6 hours may be too little so this is configurable using the
        environment variables.
        :return:
        """
        v1_core = client.CoreV1Api()
        seconds_in_30_minutes = 60 * 30
        logs = v1_core.read_namespaced_pod_log(
            name=self.pod.metadata.name, namespace=self.pod.metadata.namespace,
            timestamps=True, tail_lines=1, since_seconds=seconds_in_30_minutes,
            container=self.container_name)
        if logs == "":
            logger.info(f"No new logs for pod {self.pod.metadata.name} in {seconds_in_30_minutes} seconds")
            return True
        if (self.pod.metadata.creation_timestamp - datetime.datetime.now()) > self.max_time_to_complete:
            logger.info(f"Pod has timed out: {self.pod.metadata.name}, ")
            return True
        return False

    def _find_start_and_end_of_pod(self, pod: V1Pod) -> Tuple[Any, Optional[Any]]:
        v1_core = client.CoreV1Api()
        pod = v1_core.read_namespaced_pod(pod.metadata.name, self.namespace)
        start_time = pod.status.start_time
        end_time = None
        for container_status in pod.status.container_statuses:
            if container_status.state.terminated:
                end_time = container_status.state.terminated.finished_at
                break
        return start_time, end_time

    def process_job_failed(self) -> None:
        """
        Process the event that failed, and notify the message broker
        :return:
        """
        message = self.job.status.conditions[0].message
        logger.info("Job %s has failed, with message: %s", self.job.metadata.name, message)
        reduction_id = self.job.metadata.annotations["reduction-id"]
        start, end = self._find_start_and_end_of_pod(self.pod)
        self.db_updater.update_completed_run(
            db_reduction_id=reduction_id,
            state=State(State.ERROR),
            status_message=message,
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
        )

    def process_job_success(self) -> None:
        """
        Process a successful event, grab the required data and logged output that will notify the message broker
        :return:
        """
        job_name = self.job.metadata.name
        reduction_id = self.job.metadata.annotations.get("reduction-id")
        if self.pod is None:
            raise TypeError(
                f"Pod name can't be None, {job_name} name and {self.namespace} "
                f"namespace returned None when looking for a pod."
            )
        v1_core = client.CoreV1Api()
        # Convert message from JSON string to python dict
        try:
            logs = v1_core.read_namespaced_pod_log(name=self.pod.metadata.name, namespace=self.namespace)
            output = logs.split("\n")[-2]  # Get second to last line (last line is empty)
            logger.info("Job %s has been completed with output: %s", job_name, output)
            job_output = json.loads(output)
        except JSONDecodeError as exception:
            logger.error("Last message from job is not a JSON string")
            logger.exception(exception)
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }
        except TypeError as exception:
            logger.error("Last message from job is not a string: %s", str(exception))
            logger.exception(exception)
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }
        except Exception as exception:  # pylint:disable=broad-exception-caught
            logger.error("There was a problem recovering the job output")
            logger.exception(exception)
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }

        # Grab status from output
        status = job_output.get("status", "Unsuccessful")
        status_message = job_output.get("status_message", "")
        output_files = job_output.get("output_files", [])
        start, end = self._find_start_and_end_of_pod(self.pod)
        self.db_updater.update_completed_run(
            db_reduction_id=reduction_id,
            state=State[status.upper()],
            status_message=status_message,
            output_files=output_files,
            reduction_end=str(end),
            reduction_start=start,
        )

    def cleanup_job(self) -> None:
        """
        Cleanup the leftovers that a job will leave behind when it cleans up itself
        after a timeout, namely PVs and PVCs
        """
        logger.info(f"Starting cleanup of job {self.job.metadata.name}")
        clean_up_pvs_for_job(self.job)
        clean_up_pvcs_for_job(self.job, self.namespace)
