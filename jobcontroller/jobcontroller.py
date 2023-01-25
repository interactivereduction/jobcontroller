"""
The RunMaker is responsible for creating k8s pods that perform the reduction. It expects the kafka IP to be present in
the environment as KAFKA_IP.
"""
import logging
import sys

from jobcontroller.k8sapi import K8sAPI
from jobcontroller.topicconsumer import TopicConsumer

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(handlers=[file_handler, stdout_handler],
                    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class JobController:
    def __init__(self):
        self.consumer = TopicConsumer(self.on_message)
        self.k8s = K8sAPI()

    def on_message(self, message: dict):
        self.k8s.spawn_pod(message)

    def run(self):
        self.consumer.start_consuming()


if __name__ == '__main__':
    job_controller = JobController()
    job_controller.run()
