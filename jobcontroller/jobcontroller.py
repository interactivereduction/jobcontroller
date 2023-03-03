"""
The RunMaker is responsible for creating k8s pods that perform the reduction. It expects the kafka IP to be present in
the environment as KAFKA_IP.
"""
import os
import threading
import uuid

from jobcontroller.jobwatcher import JobWatcher
from jobcontroller.jobcreator import JobCreator
from jobcontroller.scriptaquisition import acquire_script
from jobcontroller.topicconsumer import TopicConsumer
from jobcontroller.utils import create_ceph_path, logger


class JobController:
    """
    This is the JobController class that will communicate between the consumer and the kubernetes API, it effectively
    functions as a main class.
    """

    def __init__(self) -> None:
        self.ir_api_ip = "irapi.ir.svc.cluster.local"
        self.kafka_ip = os.environ.get("KAFKA_IP", "broker")
        self.consumer = TopicConsumer(self.on_message, broker_ip=self.kafka_ip)
        self.job_creator = JobCreator()
        self.ir_k8s_api = "ir-jobs"

    def on_message(self, message: dict) -> None:
        """
        Request that the k8s api spawns a pod
        :param message: dict, the message is a dictionary containing the needed information for spawning a pod
        :return: None
        """
        try:
            filename = os.path.splitext(os.path.basename(message["filepath"]))[0]
            rb_number = message["experiment_number"]
            instrument_name = message["instrument"]
            # Add UUID which will avoid collisions for reruns
            job_name = f"run-{filename.lower()}-{str(uuid.uuid4().hex)}"
            script = acquire_script(filename=filename, ir_api_ip=self.ir_api_ip)
            ceph_path = create_ceph_path(instrument_name=instrument_name, rb_number=rb_number)
            job = self.job_creator.spawn_job(job_name=job_name, script=script, ceph_path=ceph_path,
                                             job_namespace=self.ir_k8s_api)
            self.create_job_watcher(job, ceph_path)
        except Exception as exception:
            logger.exception(exception)

    def create_job_watcher(self, job_name: str, ceph_path: str) -> None:
        """
        Start a thread with a pod manager to maintain looking at these pods that have been created, checking for it
        to finish every 1 millisecond, when it dies, do the job of sending a message to the kafka topic determining
        the end of the runstate, and the output result.
        :param job_name: The name of the job that was created by the k8s api
        :param ceph_path: The path that was mounted in the container for the jobs that were created
        :return:
        """
        watcher = JobWatcher(job_name, self.ir_k8s_api, self.kafka_ip, ceph_path)
        threading.Thread(target=watcher.watch).start()

    def run(self) -> None:
        """
        This is effectively the main method of the program and starts the consumer
        """
        self.consumer.start_consuming()


def main():
    job_controller = JobController()
    job_controller.run()


if __name__ == "__main__":
    main()
