import os
import unittest
from time import sleep

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, ConfigResource


class JobControllerTest(unittest.TestCase):
    def setUp(self):
        kafka_ip = os.environ.get("KAFKA_IP")
        self.kafka_config = {
            "bootstrap.servers": kafka_ip,
            "group.id": "job-controller",
            "reconnect.backoff.max.ms": 600000,  # Retry for up to 10 minutes
            "auto.offset.reset": "earliest",
        }
        self.ensure_retention_of_topic_is_only_2_seconds()

    def ensure_retention_of_topic_is_only_2_seconds(self):
        config_resource = ConfigResource("topic", "completed-runs")
        config_resource.set_config("retention.ms", str(2000))
        AdminClient(self.kafka_config).alter_configs([config_resource])

    @staticmethod
    def topic_exists(admin, topic):
        metadata = admin.list_topics()
        for t in iter(metadata.topics.values()):
            if t.topic == topic:
                return True
        return False

    def wait_for_kafka_to_be_up(self):
        # Check by waiting for it to return a list of topics that we expect to a consumer that asks.
        kafka_admin = AdminClient(self.kafka_config)
        print("Checking if kafka exists....")
        for _ in range(0, 300):
            if self.topic_exists(kafka_admin, "detected-runs"):
                print("Kafka exists")
                return
            else:
                print("Waiting for Kafka to exist")
                sleep(1)

    def send_kafka_message(self):
        self.wait_for_kafka_to_be_up()
        print("Sending message to kafka...")
        kafka_producer = Producer(self.kafka_config)
        kafka_producer.produce(
            "detected-runs",
            value='{"filepath": "/test/path/to/MARI0.nxs", ' '"experiment_number": "0", "instrument": "MARI"}',
        )
        kafka_producer.flush()
        print("Kafka message sent...")

    def recieve_kafka_message(self, expected_message):
        self.wait_for_kafka_to_be_up()
        print("Waiting for kafka message...")
        kafka_consumer = Consumer(self.kafka_config)
        kafka_consumer.subscribe(["completed-runs"])
        for _ in range(0, 300):
            message = kafka_consumer.poll()
            if message is not None:
                message_str = message.value().decode("utf-8")
                print(f"Message received {message_str}")
                self.assertEqual(expected_message, message_str)
                break
            else:
                # Log something useful, for the current kafka connection state and the kubernetes description for
                # JobController
                print("No message found... try again...")
            # Sleep a second before polling again
            sleep(1)

    def test_job_controller_sends_expected_output_on_for_plausible_run(self):
        # Send message to kafka topic detected-runs
        self.send_kafka_message()

        # Wait for message on kafka topic completed-runs with timeout 5 mins
        self.recieve_kafka_message('{"status": "Successful", "run output": []}')
