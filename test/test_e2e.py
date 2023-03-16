import os
import unittest
from time import sleep

from confluent_kafka import Consumer, Producer

from utils import load_kubernetes_config


class JobControllerTest(unittest.TestCase):
    def setUp(self):
        kafka_ip = os.environ.get("KAFKA_IP")
        self.kafka_config = {
            "bootstrap.servers": kafka_ip,
            "group.id": "job-controller",
            "reconnect.backoff.max.ms": 600000,  # Retry for up to 10 minutes
        }
        load_kubernetes_config()

    def send_kafka_message(self):
        kafka_producer = Producer(self.kafka_config)
        kafka_producer.produce(
            "detected-runs",
            value='{"filepath": "/test/path/to/MARI0.nxs", ' '"experiment_number": "0", "instrument": "MARI"}',
        )
        kafka_producer.flush()

    def recieve_kafka_message(self, expected_message):
        kafka_consumer = Consumer(self.kafka_config)
        kafka_consumer.subscribe(["detected-runs"])
        for _ in range(0, 3000):
            message = kafka_consumer.poll()
            if message is not None:
                message_str = message.value().decode("utf-8")
                self.assertEqual(expected_message, message_str)
                break
            # Sleep a second before polling again
            sleep(0.1)

    def test_job_controller_sends_expected_output_on_for_plausible_run(self):
        # Send message to kafka topic detected-runs
        self.send_kafka_message()

        # Wait for message on kafka topic completed-runs with timeout 5 mins
        self.recieve_kafka_message("{}")

    def test_job_controller_sends_expected_output_on_failing_run(self):
        pass
