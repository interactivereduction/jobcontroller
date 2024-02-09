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
from kubernetes.client import V1Job, V1Pod  # type: ignore[import]

from database.state_enum import State
from database.db_updater import DBUpdater
from utils import logger


def clean_up_pvcs_for_job(job: V1Job, namespace: str) -> None:
    v1 = client.CoreV1Api()
    pvcs_to_delete = job.metadata.annotations["pvcs"]
    for pvc in pvcs_to_delete:
        v1.delete_namespaced_persistent_volume_claim(pvc, namespace=namespace)
        logger.info(f"Deleted pv: {pvc}")


def clean_up_pvs_for_job(job: V1Job) -> None:
    v1 = client.CoreV1Api()
    pvs_to_delete = job.metadata.annotations["pvs"]
    for pv in pvs_to_delete:
        v1.delete_persistent_volume(pv)
        logger.info(f"Deleted pv: {pv}")


def job_is_watchable(job: V1Job) -> bool:
    """
    Checks that the job is watchable by the job watcher
    :param job:
    :return:
    """
    return ("run-" in job.metadata.name and
            "reduction-id" in job.metadata.annotations and
            "pvs" in job.metadata.annotations and
            "pvcs" in job.metadata.annotations)


class JobWatcher:  # pylint: disable=too-many-instance-attributes
    """
    Watch a kubernetes job, and when it ends notify a message broker station/topic
    """

    def __init__(self, db_updater: DBUpdater, max_time_to_complete: int):
        self.namespace = os.environ.get("JOB_NAMESPACE", "ir")
        self.db_updater = db_updater
        self.max_time_to_complete = max_time_to_complete

    def grab_pod_from_job(self, job: V1Job) -> Optional[V1Pod]:
        """
        Find the name of the pod, given a job name, this works on the assumption that there is
        only 1 pod in a job.
        :param job: The job that contains the pod you want
        :return: str or None, the name of the pod for the passed values. Will return None when no pod could be found
        for passed values
        """
        v1 = client.CoreV1Api()
        pods = v1.list_namespaced_pod(self.namespace)
        for pod in pods.items:
            for owner in pod.metadata.owner_references:
                if owner.name == job.metadata.name:
                    return pod
        return None

    def watch(self) -> None:
        """
        This is the main function responsible for watching a job, and it's responsible for calling the function that
        will notify the message broker.
        :return:
        """
        logger.info("Starting job watcher, scanning for new job states.")
        while True:
            job_list = client.BatchV1Api().list_namespaced_job(self.namespace)
            for job in job_list.items:
                if job_is_watchable(job):
                    self.check_for_changes(job)
            # Brief sleep to facilitate reducing CPU load and network bandwidth whilst largely maintaining performance
            sleep(0.1)

    def check_for_changes(self, job: V1Job) -> None:
        """
        Check if the job has a change for which we need to react to, such as the pod
        having finished or a job has stalled.
        :param job:
        :return:
        """
        pod = self.grab_pod_from_job(job)
        if self.check_for_job_complete(job, pod):
            self.cleanup_job(job)
        elif self.check_for_pod_stalled(pod):
            self.cleanup_job(job)

    def check_for_job_complete(self, job: V1Job, pod: V1Pod) -> bool:
        """
        Checks if the job has finished by checking its status, if it failed then we
        need to process that, and the same for a success.
        :param pod:
        :param job:
        :return:
        """
        if job.status.succeeded == 1:
            # Job has succeeded
            self.process_job_success(job, pod)
            return True
        elif job.status.failed == 1:
            # Job has failed
            self.process_job_failed(job, pod)
            return True
        return False

    def check_for_pod_stalled(self, pod: V1Pod) -> bool:
        """
        The way this checks if a job is stalled is by checking if there has been no new
        logs for the last 30 minutes, or if the job has taken over 6 hours to complete.
        Long term 6 hours may be too little so this is configurable using the
        environment variables.
        :param pod:
        :return:
        """
        v1_core = client.CoreV1Api()
        seconds_in_30_minutes = 60 * 30
        logs = v1_core.read_namespaced_pod_log(
            name=pod.metadata.name, namespace=pod.metadata.namespace, timestamps=True,
            tail_lines=1, since_seconds=seconds_in_30_minutes)
        if logs == "":
            logger.info(f"No new logs for pod {pod.metadata.name} in {seconds_in_30_minutes} seconds")
            return True
        if (pod.metadata.creation_timestamp - datetime.datetime.now()) > self.max_time_to_complete:
            logger.info(f"Pod has timed out: {pod.metadata.name}, ")
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

    def process_job_failed(self, job: V1Job, pod: V1Pod) -> None:
        """
        Process the event that failed, and notify the message broker
        :param pod:
        :param job: The job that has failed
        :return:
        """
        message = job.status.conditions[0].message
        logger.info("Job %s has failed, with message: %s", job.metadata.name, message)
        reduction_id = job.metadata.annotations["reduction-id"]
        start, end = self._find_start_and_end_of_pod(pod)
        self.db_updater.update_completed_run(
            db_reduction_id=reduction_id,
            state=State(State.ERROR),
            status_message=message,
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
        )

    def process_job_success(self, job: V1Job, pod: V1Pod) -> None:
        """
        Process a successful event, grab the required data and logged output that will notify the message broker
        :param job:
        :param pod:
        :return:
        """
        job_name = job.metadata.name
        namespace = job.metadata.namespace
        reduction_id = job.metadata.annotations.get("reduction-id")
        if pod is None:
            raise TypeError(
                f"Pod name can't be None, {job_name} name and {namespace} "
                f"namespace returned None when looking for a pod."
            )
        v1_core = client.CoreV1Api()
        # Convert message from JSON string to python dict
        try:
            logs = v1_core.read_namespaced_pod_log(name=pod.metadata.name, namespace=namespace)
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
        start, end = self._find_start_and_end_of_pod(pod)
        self.db_updater.update_completed_run(
            db_reduction_id=reduction_id,
            state=State[status.upper()],
            status_message=status_message,
            output_files=output_files,
            reduction_end=str(end),
            reduction_start=start,
        )

    def cleanup_job(self, job: V1Job) -> None:
        """
        """
        logger.info(f"Starting cleanup of job {job.metadata.name}")
        clean_up_pvs_for_job(job)
        clean_up_pvcs_for_job(job, self.namespace)
