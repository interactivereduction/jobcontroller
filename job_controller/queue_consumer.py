"""
The module is aimed to consume from a station on Memphis using the create_station_consumer
"""
import json
from typing import Callable, Dict

from pika import PlainCredentials, ConnectionParameters, BlockingConnection

from job_controller.utils import logger


class QueueConsumer:
    """
    This class is responsible for running the listener for RabbitMQ, and requesting the correct response from the
    JobController
    """

    def __init__(
        self,
        message_callback: Callable[[Dict[str, str]], None],
        queue_host: str,
        username: str,
        password: str,
        queue_name: str,
    ) -> None:
        self.message_callback = message_callback
        self.queue_host = queue_host
        self.queue_name = queue_name
        credentials = PlainCredentials(username=username, password=password)
        self.connection_parameters = ConnectionParameters(queue_host, 5672, credentials=credentials)
        self.connection = None
        self.channel = None
        self.connect_to_broker()

    def connect_to_broker(self) -> None:
        """
        Use this to connect to the broker
        :return: None
        """
        self.connection = BlockingConnection(self.connection_parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(self.queue_name, exchange_type="direct", durable=True)
        self.channel.queue_declare(self.queue_name, durable=True, arguments={"x-queue-type": "quorum"})
        self.channel.queue_bind(self.queue_name, self.queue_name, routing_key="")

    def _message_handler(self, msg: str) -> None:
        """
        Handles a message from the message broker
        :param msg: A message that need to be processed
        :return:
        """
        try:
            msg_obj = json.loads(msg)
            logger.info("Message decoded as: %s", msg_obj)
            self.message_callback(msg_obj)
        except json.JSONDecodeError as exception:
            logger.error("Error attempting to decode JSON: %s", str(exception))

    def start_consuming(self, run_once: bool = False) -> None:
        """
        The function that will start consuming from a queue, and when the consumer receives a message.
        :param run_once: Should this only run once or run until there is a raised exception or interrupt.
        :return: None
        """
        run = True
        while run:
            if run_once:
                run = False
            for mf, _, body in self.channel.consume(self.queue_name):
                try:
                    self._message_handler(body.decode())
                except Exception:
                    logger.warning("Problem processing message: %s", body)
                finally:
                    self.channel.basic_ack(mf.delivery_tag)
