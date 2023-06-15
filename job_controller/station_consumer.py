"""
The module is aimed to consume from a station on Memphis using the create_station_consumer
"""
import asyncio
import json
from typing import Callable, Dict, Union, Any, List

from memphis import Memphis, MemphisError  # type: ignore
from memphis.consumer import Consumer  # type: ignore
from memphis.message import Message

from job_controller.utils import logger


class StationConsumer:
    """
    This class is responsible for running the listener for Kafka, and requesting the correct response from the
    JobController
    """

    def __init__(
        self, message_callback: Callable[[Dict[str, str]], None], broker_ip: str, username: str, password: str
    ) -> None:
        self.message_callback = message_callback
        self.broker_ip = broker_ip
        self.memphis_username = username
        self.memphis_password = password
        self.memphis = Memphis()

    async def _init(self) -> None:
        await self.connect_to_broker()
        # pylint: disable=attribute-defined-outside-init
        self.consumer: Union[Consumer, MemphisError] = await self.memphis.consumer(
            station_name="requested-jobs",
            consumer_name="jobcontroller",
            consumer_group="jobcontrollers",
            generate_random_suffix=True,
        )
        # pylint: enable=attribute-defined-outside-init
        if isinstance(self.consumer, MemphisError):
            raise self.consumer
        logger.info("Connected to memphis using the ip: %s", self.broker_ip)

    async def connect_to_broker(self) -> None:
        """
        Function to be called when not already connected to the broker, can be called again, later.
        :return:
        """
        await self.memphis.connect(
            host=self.broker_ip,
            username=self.memphis_username,
            password=self.memphis_password,
        )

    async def _message_handler(self, msgs: List[Message], error: Exception, _: Dict[Any, Any]) -> None:
        """
        Handles messages from the message broker
        :param msgs: A iterator for a batch of messages that need to be processed
        :param error: An error from the message broker.
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
        """
        The function that will start consuming from a memphis station
        :param run_once: Should this only run once or run until there is a raised exception or interrupt.
        :return: None
        """
        run = True
        while run:
            if run_once:
                run = False

            if not self.memphis.is_connection_active:
                # Not connected so attempt to reconnect
                await self.connect_to_broker()

            try:
                self.consumer.consume(self._message_handler)
                await asyncio.wait(asyncio.all_tasks())
            except MemphisError as error:
                logger.error("Memphis error occurred: %s", error)


async def create_station_consumer(
    message_callback: Callable[[Dict[str, str]], None], broker_ip: str, username: str, password: str
) -> StationConsumer:
    """
    The method that can be called to create a StationConsumer and ensure that ._init is called before the consumer is
    used.
    :param message_callback: A callback callable function that will be passed to the StationConsumer
    :param broker_ip: The ip passed to the StationConsumer
    :param username: The station username passed to the StationConsumer
    :param password: The station password passed to the StationConsumer
    :return: A fully initialised object of the StationConsumer
    """
    consumer = StationConsumer(message_callback, broker_ip, username, password)
    await consumer._init()  # pylint: disable=protected-access
    return consumer
