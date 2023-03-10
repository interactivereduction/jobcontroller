# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import unittest
from unittest import mock

from jobcontroller.topicconsumer import TopicConsumer


class TopicConsumerTest(unittest.TestCase):
    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consumer_is_made_with_passed_ip(self, kafka_consumer):
        broker_ip = mock.MagicMock()

        TopicConsumer(message_callback=mock.MagicMock, broker_ip=broker_ip)

        kafka_consumer.assert_called_once_with({"bootstrap.servers": broker_ip, "group.id": "consumer-group-name"})

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consumer_subscribes_using_kafka_consumer(self, kafka_consumer):
        broker_ip = mock.MagicMock()

        TopicConsumer(message_callback=mock.MagicMock, broker_ip=broker_ip)

        kafka_consumer.return_value.subscribe.assert_called_once_with(["detected-runs"])

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consumer_empty_message_does_nothing(self, kafka_consumer):
        broker_ip = mock.MagicMock()
        consumer = TopicConsumer(message_callback=mock.MagicMock(), broker_ip=broker_ip)
        consumer.consumer.poll.return_value = None

        consumer.start_consuming(run_once=True)

        kafka_consumer.return_value.poll.assert_called_once_with()
        consumer.message_callback.assert_not_called()

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consumer_error_message_does_nothing(self, kafka_consumer):
        broker_ip = mock.MagicMock()
        message = mock.MagicMock()
        consumer = TopicConsumer(message_callback=mock.MagicMock(), broker_ip=broker_ip)
        consumer.consumer.poll.return_value = message

        consumer.start_consuming(run_once=True)

        kafka_consumer.return_value.poll.assert_called_once_with()
        consumer.message_callback.assert_not_called()

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consumer_decoded_message_is_none_does_nothing(self, kafka_consumer):
        broker_ip = mock.MagicMock()
        message = mock.MagicMock()
        consumer = TopicConsumer(message_callback=mock.MagicMock(), broker_ip=broker_ip)
        consumer.consumer.poll.return_value = message
        message.value.return_value.decode.return_value = None

        consumer.start_consuming(run_once=True)

        kafka_consumer.return_value.poll.assert_called_once_with()
        consumer.message_callback.assert_not_called()

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    @mock.patch("jobcontroller.topicconsumer.json")
    def test_consumer_message_callback_receives_dictionary_of_decoded_message_str(self, json, kafka_consumer):
        broker_ip = mock.MagicMock()
        message = mock.MagicMock()
        message.error.return_value = None
        consumer = TopicConsumer(message_callback=mock.MagicMock(), broker_ip=broker_ip)
        consumer.consumer.poll.return_value = message
        message_value = "{'name': message_value}"
        message.value.return_value.decode.return_value = message_value

        consumer.start_consuming(run_once=True)

        kafka_consumer.return_value.poll.assert_called_once_with()
        json.loads.assert_called_once_with(message_value)
        consumer.message_callback.assert_called_once_with(json.loads.return_value)

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consumer_with_json_decode_error_logs_exception_and_does_nothing(self, kafka_consumer):
        broker_ip = mock.MagicMock()
        message = mock.MagicMock()
        consumer = TopicConsumer(message_callback=mock.MagicMock(), broker_ip=broker_ip)
        consumer.consumer.poll.return_value = message
        message_value = "}broken-json{"
        message.value.return_value.decode.return_value = message_value

        consumer.start_consuming(run_once=True)

        kafka_consumer.return_value.poll.assert_called_once_with()
        consumer.message_callback.assert_not_called()

    @mock.patch("jobcontroller.topicconsumer.Consumer")
    def test_consuming_decodes_string_into_dict_correctly(self, _):
        broker_ip = mock.MagicMock()
        message = mock.MagicMock()
        message.error.return_value = None
        consumer = TopicConsumer(message_callback=mock.MagicMock(), broker_ip=broker_ip)
        consumer.consumer.poll.return_value = message
        message_value = '{"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}'
        message.value.return_value.decode.return_value = message_value

        consumer.start_consuming(run_once=True)

        consumer.message_callback.assert_called_once_with(
            {"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}
        )
