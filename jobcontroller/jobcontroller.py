"""
The RunMaker is responsible for creating k8s pods that perform the reduction. It expects the kafka IP to be present in
the environment as KAFKA_IP.
"""
import logging
import os
import sys
import threading

from jobcontroller.k8sapi import K8sAPI
from jobcontroller.podmanager import PodManager
from jobcontroller.scriptaquisition import aquire_script
from jobcontroller.topicconsumer import TopicConsumer

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[file_handler, stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class JobController:
    """
    This is the JobController class that will communicate between the consumer and the kubernetes API, it effectively
    functions as a main class.
    """

    def __init__(self) -> None:
        self.ir_api_ip = "irapi.ir.svc.cluster.local"
        self.kafka_ip = os.environ.get("KAFKA_IP", "broker")
        self.consumer = TopicConsumer(self.on_message, broker_ip=self.kafka_ip)
        self.k8s = K8sAPI()

    def on_message(self, message: dict) -> None:
        """
        Request that the k8s api spawns a pod
        :param message: dict, the message is a dictionary containing the needed information for spawning a pod
        :return: None
        """
        filename = os.path.basename(message["filepath"])
        rb_number = message["exeriment_number"]
        instrument_name = message["instrument"]
        job_name = f"run-{filename}"
        script = aquire_script(filename=filename, ir_api_ip=self.ir_api_ip)
        pod_name = self.k8s.spawn_job(job_name=job_name, script=script)
        self.create_pod_manager(pod_name=pod_name)

    def create_pod_manager(self, pod_name: str) -> None:
        """
        Start a thread with a pod manager to maintain looking at these pods that have been created, checking for it
        to finish every 1 millisecond, when it dies, do the job of sending a message to the kafka topic determining
        the end of the runstate, and the output result.
        :param pod_name:
        """
        manager = PodManager(pod_name)
        threading.Thread(
            target=manager.manage,
        )
        pass

    def run(self) -> None:
        """
        This is effectively the main method of the program and starts the consumer
        """
        self.consumer.start_consuming()


if __name__ == "__main__":
    job_controller = JobController()
    job_controller.run()
