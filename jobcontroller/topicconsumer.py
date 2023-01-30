"""
The topic consumer connects to kafka and polls for messages from the topic. It expects KAFKA_IP to be set as an
environment variable, the value should be the kafka broker ip address.
"""
import json
import os
from typing import Callable, Any

from confluent_kafka import Consumer  # type: ignore


class TopicConsumer:
    """
    This class is responsible for running the listener for Kafka, and requesting the correct response from the
    JobController
    """

    def __init__(self, message_callback: Callable[[dict[str, Any]], None], broker_ip: str) -> None:
        from jobcontroller.jobcontroller import logger

        self.message_callback = message_callback
        self.consumer = Consumer({"bootstrap.servers": broker_ip, "group.id": "consumer-group-name"})
        logger.info("Connecting to kafka using the ip: %s", broker_ip)
        self.consumer.subscribe(["detected-run"])

    def start_consuming(self) -> None:
        """
        Run a while loop listening for a message
        """
        from jobcontroller.jobcontroller import logger

        while True:
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
                self.message_callback(json.loads(message_str))
            except json.JSONDecodeError as exception:
                logger.error("Error attempting to decode JSON: %s", str(exception))
                continue
