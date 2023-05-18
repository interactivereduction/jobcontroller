import json
from typing import Callable, Dict

from job_controller.utils import logger


class StationConsumer:
    """
    This class is responsible for running the listener for Kafka, and requesting the correct response from the
    JobController
    """

    def __init__(self, message_callback: Callable[[Dict[str, str]], None], broker_ip: str) -> None:
        self.message_callback = message_callback
        self.consumer = await memphis.consumer(
            station_name="requested-jobs",
            consumer_name="jobcontroller",
            pull_interval="100",  # ms
            generate_random_suffix=True,
        )
        self.consumer.set_context()
        logger.info("Connecting to kafka using the ip: %s", broker_ip)
        self.consumer.subscribe(["detected-runs"])

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

    def start_consuming(self, run_once: bool = False) -> None:
        run = True
        while run:
            if run_once:
                run = False

            self.consumer.consume(self._message_handler)
