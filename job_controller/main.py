"""
Main class, creates jobs by calling to the jobcreator, creates the jobwatcher for each created job, and receives
requests from the topicconsumer.
"""
import os
import threading
import uuid
from pathlib import Path
from typing import Dict, Any

from job_controller.database.db_updater import DBUpdater
from job_controller.job_watcher import JobWatcher
from job_controller.job_creator import JobCreator
from job_controller.script_aquisition import acquire_script
from job_controller.queue_consumer import QueueConsumer
from job_controller.utils import create_ceph_path, logger, ensure_ceph_path_exists


class JobController:  # pylint: disable=too-many-instance-attributes
    """
    This is the JobController class that will communicate between the consumer and the kubernetes API, it effectively
    functions as a main class.
    """

    def __init__(self) -> None:
        db_ip = os.environ.get("DB_IP", "")
        db_username = os.environ.get("DB_USERNAME", "")
        db_password = os.environ.get("DB_PASSWORD", "")
        runner_sha = os.environ.get("RUNNER_SHA", None)
        if runner_sha is None:
            raise OSError("RUNNER_SHA not set in the environment, please add it.")
        self.db_updater = DBUpdater(ip=db_ip, username=db_username, password=db_password)
        self.ir_api_host = os.environ.get("IR_API", "ir-api-service.ir.svc.cluster.local:80")
        broker_ip = os.environ.get("QUEUE_HOST", "")
        queue_name = os.environ.get("INGRESS_QUEUE_NAME", "")
        consumer_username = os.environ.get("QUEUE_USER", "")
        consumer_password = os.environ.get("QUEUE_PASSWORD", "")
        self.reduce_user_id = os.environ.get("REDUCE_USER_ID", "")
        self.job_creator = JobCreator(runner_sha=runner_sha)
        self.ir_k8s_api = "ir-jobs"
        self.consumer = QueueConsumer(  # pylint: disable=attribute-defined-outside-init
            self.on_message,
            broker_ip=broker_ip,
            username=consumer_username,
            password=consumer_password,
            queue_name=queue_name,
        )

    def on_message(self, message: Dict[str, Any]) -> None:  # pylint: disable=too-many-locals
        """
        Request that the k8s api spawns a pod
        :param message: dict, the message is a dictionary containing the needed information for spawning a pod
        :return: None
        """
        try:
            filename = Path(message["filepath"]).stem
            rb_number = message["experiment_number"]
            instrument_name = message["instrument"]
            experiment_number = message["experiment_number"]
            title = message["experiment_title"]
            users = message["users"]
            run_start = message["run_start"]
            run_end = message["run_end"]
            good_frames = message["good_frames"]
            raw_frames = message["raw_frames"]
            additional_values = message["additional_values"]
            # Add UUID which will avoid collisions for reruns
            job_name = f"run-{filename.lower()}-{str(uuid.uuid4().hex)}"
            ceph_path = create_ceph_path(instrument_name=instrument_name, rb_number=rb_number)
            db_reduction_id = self.db_updater.add_detected_run(
                filename=filename,
                title=title,
                instrument_name=instrument_name,
                users=users,
                experiment_number=experiment_number,
                run_start=run_start,
                run_end=run_end,
                good_frames=good_frames,
                raw_frames=raw_frames,
                reduction_inputs=additional_values,
            )
            script, script_sha = acquire_script(
                ir_api_host=self.ir_api_host,
                reduction_id=db_reduction_id,
                instrument=instrument_name,
            )
            ceph_path = ensure_ceph_path_exists(ceph_path)
            job, pv, pvc = self.job_creator.spawn_job(
                job_name=job_name,
                script=script,
                ceph_path=ceph_path,
                job_namespace=self.ir_k8s_api,
                user_id=self.reduce_user_id,
            )
            self.create_job_watcher(job, pv, pvc, ceph_path, db_reduction_id, script, script_sha, additional_values)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception(exception)

    def create_job_watcher(  # pylint: disable=too-many-arguments
        self,
        job_name: str,
        pv: str,
        pvc: str,
        ceph_path: str,
        db_reduction_id: int,
        job_script: str,
        script_sha: str,
        reduction_inputs: Dict[str, Any],
    ) -> None:
        """
        Start a thread with a pod manager to maintain looking at these pods that have been created, checking for it
        to finish every 1 millisecond, when it dies, do the job of sending a message to the message broker determining
        the end of the runstate, and the output result.
        :param job_name: The name of the job that was created by the k8s api
        :param pv: The persistent volume for the job that is being watched
        :param pvc: The persistent volume claim for the job that is being watched
        :param ceph_path: The path that was mounted in the container for the jobs that were created
        :param db_reduction_id: The ID for the reduction's row in the database
        :param job_script: The script used in the reduction
        :param script_sha: The git sha of the script
        :param reduction_inputs: The inputs that the reduction is using.
        :return:
        """
        watcher = JobWatcher(
            job_name,
            pv,
            pvc,
            self.ir_k8s_api,
            ceph_path,
            self.db_updater,
            db_reduction_id,
            job_script,
            script_sha,
            reduction_inputs,
        )
        threading.Thread(target=watcher.watch).start()

    def run(self) -> None:
        """
        This is effectively the main method of the program and starts the consumer
        """
        self.consumer.start_consuming()


def main() -> None:
    """
    This is the function that runs the JobController software suite
    """
    job_controller = JobController()
    job_controller.run()


if __name__ == "__main__":
    main()
