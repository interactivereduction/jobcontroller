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
from job_controller.topic_consumer import TopicConsumer
from job_controller.utils import create_ceph_path, logger


class JobController:
    """
    This is the JobController class that will communicate between the consumer and the kubernetes API, it effectively
    functions as a main class.
    """

    def __init__(self) -> None:
        db_ip = os.environ.get("DB_IP", "")
        db_username = os.environ.get("DB_USERNAME", "")
        db_password = os.environ.get("DB_PASSWORD", "")
        self.db_updater = DBUpdater(ip=db_ip, username=db_username, password=db_password)
        self.ir_api_host = "irapi.ir.svc.cluster.local"
        self.kafka_ip = os.environ.get("KAFKA_IP", "")
        self.reduce_user_id = os.environ.get("REDUCE_USER_ID", "")
        self.consumer = TopicConsumer(self.on_message, broker_ip=self.kafka_ip)
        self.job_creator = JobCreator()
        self.ir_k8s_api = "ir-jobs"

    def on_message(self, message: Dict[str, Any]) -> None:
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
            script = acquire_script(filename=filename, ir_api_host=self.ir_api_host)
            ceph_path = create_ceph_path(instrument_name=instrument_name, rb_number=rb_number)
            db_reduction_id = self.db_updater.add_detected_run(
                filename=filename,
                title=title,
                users=users,
                experiment_number=experiment_number,
                run_start=run_start,
                run_end=run_end,
                good_frames=good_frames,
                raw_frames=raw_frames,
                reduction_inputs=additional_values,
            )
            job = self.job_creator.spawn_job(
                job_name=job_name,
                script=script,
                ceph_path=ceph_path,
                job_namespace=self.ir_k8s_api,
                user_id=self.reduce_user_id,
            )
            self.create_job_watcher(job, ceph_path, db_reduction_id, script, additional_values)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception(exception)

    def create_job_watcher(
        self, job_name: str, ceph_path: str, db_reduction_id: int, job_script: str, reduction_inputs: Dict[str, Any]
    ) -> None:
        """
        Start a thread with a pod manager to maintain looking at these pods that have been created, checking for it
        to finish every 1 millisecond, when it dies, do the job of sending a message to the kafka topic determining
        the end of the runstate, and the output result.
        :param job_name: The name of the job that was created by the k8s api
        :param ceph_path: The path that was mounted in the container for the jobs that were created
        :return:
        """
        watcher = JobWatcher(
            job_name,
            self.ir_k8s_api,
            self.kafka_ip,
            ceph_path,
            self.db_updater,
            db_reduction_id,
            job_script,
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
    This is the main function that will run the entire software
    """
    job_controller = JobController()
    job_controller.run()


if __name__ == "__main__":
    main()
