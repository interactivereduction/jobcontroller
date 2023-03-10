"""
The topic consumer connects to kafka and polls for messages from the topic. It expects KAFKA_IP to be set as an
environment variable, the value should be the kafka broker ip address.
"""
import json
from typing import Callable, Dict

from confluent_kafka import Consumer  # type: ignore[import]

from jobcontroller.utils import logger


class TopicConsumer:
    """
    This class is responsible for running the listener for Kafka, and requesting the correct response from the
    JobController
    """

    def __init__(self, message_callback: Callable[[Dict[str, str]], None], broker_ip: str) -> None:
        self.message_callback = message_callback
        consumer_config = {
            "bootstrap.servers": broker_ip,
            "group.id": "consumer-group-name",
            "auto.offset.reset": "earliest",  # Consume from the earliest part of the topic
            "reconnect.backoff.max.ms": 600000,  # Retry for up to 10 minutes
        }
        self.consumer = Consumer(consumer_config)
        logger.info("Connecting to kafka using the ip: %s", broker_ip)
        self.consumer.subscribe(["detected-runs"])

    def start_consuming(self, run_once: bool = False) -> None:
        """
        Run a while loop listening for a message
        """
        run = True
        while run:
            if run_once:
                run = False

            message = self.consumer.poll()

            if message is None:
                continue
            if message.error() is not None:
                logger.error("Error on message received from kafka: %s", str(message.error()))
                continue
            message_str = message.value().decode("utf-8")
            if message_str is None:
                continue

            logger.info("Received a message from the topic: %s", message_str)
            try:
                message_obj = json.loads(message_str)
                logger.info("Message decoded as: %s", message_obj)
                self.message_callback(message_obj)
            except json.JSONDecodeError as exception:
                logger.error("Error attempting to decode JSON: %s", str(exception))
                continue
