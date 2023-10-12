"""
Watch a kubernetes job, and when it ends notify a message broker station/topic
"""
import json
from json import JSONDecodeError
from typing import Any, Optional, Dict, Tuple

from kubernetes import client, watch  # type: ignore[import]
from kubernetes.client import V1Job  # type: ignore[import]

from job_controller.database.state_enum import State
from job_controller.database.db_updater import DBUpdater
from job_controller.utils import logger, load_kubernetes_config


class JobWatcher:  # pylint: disable=too-many-instance-attributes
    """
    Watch a kubernetes job, and when it ends notify a message broker station/topic
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        job_name: str,
        pv_name: str,
        pvc_name: str,
        namespace: str,
        ceph_path: str,
        db_updater: DBUpdater,
        db_reduction_id: int,
        job_script: str,
        script_sha: str,
        reduction_inputs: Dict[str, Any],
    ):
        self.job_name = job_name
        self.pv_name = pv_name
        self.pvc_name = pvc_name
        self.namespace = namespace
        self.ceph_path = ceph_path
        self.db_updater = db_updater
        self.db_reduction_id = db_reduction_id
        self.job_script = job_script
        self.script_sha = script_sha
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
        will notify the message broker.
        :return:
        """
        logger.info("Starting JobWatcher for job %s, and in namespace: %s", self.job_name, self.namespace)
        v1 = client.BatchV1Api()
        watch_ = watch.Watch()
        try:
            for event in watch_.stream(v1.list_job_for_all_namespaces):
                self.process_event(event)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("JobWatcher for job %s failed", self.job_name)
            logger.exception(exception)
            self.clean_up_pv_and_pvc()
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
                self.clean_up_pv_and_pvc()
            elif job.status.failed == 1:
                # Job failed
                self.process_event_failed(job)
                self.clean_up_pv_and_pvc()

    def _find_start_and_end_of_job(self) -> Tuple[Any, Optional[Any]]:
        pod_name = self.grab_pod_name_from_job_name_in_namespace(job_name=self.job_name, job_namespace=self.namespace)
        if pod_name is None:
            raise TypeError(
                f"Pod name can't be None, {self.job_name} name and {self.namespace} "
                f"namespace returned None when looking for a pod."
            )
        v1_core = client.CoreV1Api()
        pod = v1_core.read_namespaced_pod(pod_name, self.namespace)
        start_time = pod.status.start_time
        end_time = None
        for container_status in pod.status.container_statuses:
            if container_status.state.terminated:
                end_time = container_status.state.terminated.finished_at
                break
        return start_time, end_time

    def process_event_failed(self, job: V1Job) -> None:
        """
        Process the event that failed, and notify the message broker
        :param job: The job that has failed
        :return:
        """
        message = job.status.conditions[0].message
        logger.info("Job %s has failed, with message: %s", self.job_name, message)
        start, end = self._find_start_and_end_of_job()
        self.db_updater.add_completed_run(
            db_reduction_id=self.db_reduction_id,
            state=State(State.ERROR),
            status_message=message,
            output_files=[],
            reduction_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
            reduction_end=str(end),
            reduction_start=start,
            script_sha=self.script_sha,
        )

    def process_event_success(self) -> None:
        """
        Process a successful event, grab the required data and logged output that will notify the message broker
        :return:
        """
        pod_name = self.grab_pod_name_from_job_name_in_namespace(job_name=self.job_name, job_namespace=self.namespace)
        if pod_name is None:
            raise TypeError(
                f"Pod name can't be None, {self.job_name} name and {self.namespace} "
                f"namespace returned None when looking for a pod."
            )
        v1_core = client.CoreV1Api()
        # Convert message from JSON string to python dict
        try:
            logs = v1_core.read_namespaced_pod_log(name=pod_name, namespace=self.namespace)
            output = logs.split("\n")[-2]  # Get second last line (last line is empty)
            logger.info("Job %s has been completed with output: %s", self.job_name, output)
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
        start, end = self._find_start_and_end_of_job()
        self.db_updater.add_completed_run(
            db_reduction_id=self.db_reduction_id,
            state=State[status.upper()],
            status_message=status_message,
            output_files=output_files,
            reduction_script=self.job_script,
            reduction_inputs=self.reduction_inputs,
            reduction_end=str(end),
            reduction_start=start,
            script_sha=self.script_sha,
        )

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err:
            logger.error("Delivery failed for message %s: %s", msg.value(), err)
        else:
            logger.info("Delivered message to %s [%s]", msg.topic(), msg.partition())

    def clean_up_pv_and_pvc(self) -> None:
        """
        Clean up the PV and PVC created for the jobs
        :return: None
        """
        logger.info("Removing PVCs and PVs")
        v1 = client.CoreV1Api()
        logger.info("Check PV %s exists", self.pv_name)
        if self.pv_name in [ii.metadata.name for ii in v1.list_persistent_volume().items]:
            logger.info("PV %s exists, removing", self.pv_name)
            v1.delete_persistent_volume(self.pv_name)
            logger.info("PV %s removed", self.pv_name)
        else:
            logger.info("PV %s does not exist", self.pv_name)
        logger.info("Check PVC %s exists", self.pvc_name)
        if self.pvc_name in [
            ii.metadata.name for ii in v1.list_namespaced_persistent_volume_claim(self.namespace).items
        ]:
            logger.info("PVC %s exists, removing", self.pvc_name)
            v1.delete_namespaced_persistent_volume_claim(self.pvc_name, self.namespace)
            logger.info("PVC %s removed", self.pvc_name)
        else:
            logger.info("PVC %s does not exist", self.pvc_name)
