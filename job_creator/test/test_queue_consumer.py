# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access,
# pylint: disable=too-many-instance-attributes
import unittest
from unittest import mock

from pika import PlainCredentials

from jobcreator.queue_consumer import QueueConsumer


class QueueConsumerTest(unittest.TestCase):
    @mock.patch("jobcreator.queue_consumer.BlockingConnection")
    @mock.patch("jobcreator.queue_consumer.ConnectionParameters")
    def setUp(self, connection_parameters, blocking_connection):
        self.blocking_connection = blocking_connection
        self.connection_parameters = connection_parameters
        self.message_callback = mock.MagicMock()
        self.queue_host = mock.MagicMock()
        self.username = mock.MagicMock()
        self.password = mock.MagicMock()
        self.queue_name = mock.MagicMock()

        self.quc = QueueConsumer(self.message_callback, self.queue_host, self.username, self.password, self.queue_name)

    def test_init_creates_credentials_and_connection_parameters(self):
        self.assertEqual(self.message_callback, self.quc.message_callback)
        self.assertEqual(self.queue_host, self.quc.queue_host)
        self.assertEqual(self.queue_name, self.quc.queue_name)

        credentials = PlainCredentials(username=self.username, password=self.password)
        self.connection_parameters.assert_called_once_with(self.queue_host, 5672, credentials=credentials)
        self.assertEqual(self.connection_parameters.return_value, self.quc.connection_parameters)

        self.blocking_connection.assert_called_once_with(self.quc.connection_parameters)
        self.assertEqual(self.blocking_connection.return_value, self.quc.connection)
        self.assertEqual(self.blocking_connection.return_value.channel.return_value, self.quc.channel)

    def test_connect_to_broker(self):
        self.blocking_connection.assert_called_once_with(self.quc.connection_parameters)
        self.assertEqual(self.blocking_connection.return_value, self.quc.connection)

        channel = self.blocking_connection.return_value.channel.return_value
        self.assertEqual(channel, self.quc.channel)

        channel.exchange_declare.assert_called_once_with(self.queue_name, exchange_type="direct", durable=True)
        channel.queue_declare.assert_called_once_with(
            self.queue_name, durable=True, arguments={"x-queue-type": "quorum"}
        )
        channel.queue_bind.assert_called_once_with(self.queue_name, self.queue_name, routing_key="")

    @mock.patch("jobcreator.queue_consumer.logger")
    def test_message_handler(self, logger):
        message = '{"help": "im stuck"}'
        msg_obj = {"help": "im stuck"}

        self.quc._message_handler(message)

        logger.info.assert_called_once_with("Message decoded as: %s", msg_obj)
        self.message_callback.assert_called_once_with(msg_obj)

    @mock.patch("jobcreator.queue_consumer.logger")
    def test_message_handler_on_json_decode_error(self, logger):
        message = "{}::::::::://1//1/1!!!''''''"

        self.quc._message_handler(message)

        logger.error.assert_called_once_with(
            "Error attempting to decode JSON: %s", "Extra data: line 1 column 3 (char 2)"
        )
        self.message_callback.assert_not_called()

    def test_start_consumer(self):
        self.quc._message_handler = mock.MagicMock()
        header = mock.MagicMock()
        body = mock.MagicMock()
        self.quc.channel.consume.return_value = [(header, mock.MagicMock(), body)]

        self.quc.start_consuming(run_once=True)

        self.quc.channel.consume.assert_called_once_with(self.quc.queue_name)
        self.quc._message_handler.assert_called_once_with(body.decode.return_value)
        self.quc.channel.basic_ack.assert_called_once_with(header.delivery_tag)

    @mock.patch("jobcreator.queue_consumer.logger")
    def test_start_consumer_will_handle_exceptions_as_warnings(self, logger):
        def raise_exception():
            raise Exception("The worst exception!")  # pylint: disable=broad-exception-raised

        self.quc._message_handler = mock.MagicMock(side_effect=raise_exception)
        body = mock.MagicMock()
        self.quc.channel.consume.return_value = [(mock.MagicMock(), mock.MagicMock(), body)]

        self.quc.start_consuming(run_once=True)

        logger.warning.assert_called_once_with("Problem processing message: %s", body)
