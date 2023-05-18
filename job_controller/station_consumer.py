import json
from typing import Callable, Dict, Union

from memphis import Memphis, MemphisError
from memphis.consumer import Consumer

from job_controller.utils import logger


async def create_station_consumer(message_callback: Callable[[Dict[str, str]], None], broker_ip: str, username: str,
                            password: str):
    consumer = StationConsumer(message_callback, broker_ip, username, password)
    await consumer._init()
    return consumer


class StationConsumer:
    """
    This class is responsible for running the listener for Kafka, and requesting the correct response from the
    JobController
    """
    def __init__(self, message_callback: Callable[[Dict[str, str]], None], broker_ip: str, username: str,
                       password: str) -> None:
        self.message_callback = message_callback
        self.broker_ip = broker_ip
        self.memphis_username = username
        self.memphis_password = password
        self.memphis = Memphis()

    async def _init(self):
        await self.connect_to_broker()
        self.consumer: Union[Consumer, MemphisError] = await self.memphis.consumer(
            station_name="requested-jobs",
            consumer_name="jobcontroller",
            generate_random_suffix=True
        )
        if self.consumer is MemphisError:
            raise self.consumer
        logger.info("Connected to memphis using the ip: %s", self.broker_ip)

    async def connect_to_broker(self):
        await self.memphis.connect(
            host=self.broker_ip,
            username=self.memphis_username,
            password=self.memphis_password,
        )

    async def _message_handler(self, msgs, error, _):
        """
        Handles
        :param msgs: A iterator for a batch of messages that need to be processed
        :param error:
        :param _: context of the messages
        :return:
        """
        for msg in msgs:
            msg_data = msg.get_data()
            try:
                msg_obj = json.loads(msg_data)
                logger.info("Message decoded as: %s", msg_obj)
                self.message_callback(msg_obj)
            except json.JSONDecodeError as exception:
                logger.error("Error attempting to decode JSON: %s", str(exception))
                continue
            await msg.ack()
        if error:
            logger.error(error)

    async def start_consuming(self, run_once: bool = False) -> None:
        run = True
        while run:
            if run_once:
                run = False

            if not self.memphis.is_connection_active:
                # Not connected so attempt to reconnect
                await self.connect_to_broker()

            try:
                self.consumer.consume(self._message_handler)
            except MemphisError as error:
                logger.error("Memphis error occurred: %s", error)
