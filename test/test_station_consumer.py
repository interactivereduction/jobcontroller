# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access

import unittest
from unittest import mock

from memphis import MemphisError

from job_controller.station_consumer import create_station_consumer


class StationConsumerTest(unittest.TestCase):
    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_consumer_is_made_and_connects_with_passed_values(self, memphis):
        broker_ip = mock.MagicMock()
        username = mock.MagicMock()
        password = mock.MagicMock()

        await create_station_consumer(
            message_callback=mock.MagicMock, broker_ip=broker_ip, username=username, password=password
        )

        memphis.connect.assert_called_once_with(host=broker_ip, username=username, password=password)

    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_consumer_subscribes_using_memphis_consumer(self, memphis):
        broker_ip = mock.MagicMock()
        username = mock.MagicMock()
        password = mock.MagicMock()

        await create_station_consumer(
            message_callback=mock.MagicMock, broker_ip=broker_ip, username=username, password=password
        )

        memphis.consumer.assert_called_once_with(
            station_name="requested-jobs",
            consumer_name="jobcontroller",
            consumer_group="jobcontrollers",
            generate_random_suffix=True,
        )

    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_consumer_throwing_memphis_error_is_raised(self, memphis):
        broker_ip = mock.MagicMock()
        username = mock.MagicMock()
        password = mock.MagicMock()
        error_message = mock.MagicMock()
        memphis.consumer.return_value = MemphisError(error_message)

        async def assert_raises_async(async_function, expected_exception, expected_error_message, *args, **kwargs):
            try:
                await async_function(*args, **kwargs)
            except expected_exception as error:
                self.assertEqual(expected_error_message, error)
                return
            raise AssertionError("Passed async function did not raise")

        await assert_raises_async(
            create_station_consumer,
            MemphisError,
            expected_error_message=error_message,
            message_callback=mock.MagicMock,
            broker_ip=broker_ip,
            username=username,
            password=password,
        )

    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_message_handler_decodes_string_into_dict_correctly(self, _):
        message = '{"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}'
        msgs = [message]

        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )

        consumer._message_handler(msgs, None, None)
        consumer.message_callback.assert_called_once_with(
            {"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}
        )

    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_message_handler_handles_json_decode_error(self, _):
        message = "}broken-json{"
        msgs = [message]

        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )

        consumer.message_callback = mock.MagicMock()
        consumer._message_handler(msgs, None, None)
        consumer.message_callback.assert_not_called()

    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_ensure_ack_is_called_per_msg(self, _):
        msg1 = mock.MagicMock()
        msg2 = mock.MagicMock()
        msgs = [msg1, msg2]

        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )
        consumer._message_handler(msgs, None, None)
        msg1.ack.assert_called_once_with()
        msg2.ack.assert_called_once_with()

    @mock.patch("job_controller.station_consumer.Memphis")
    @mock.patch("job_controller.station_consumer.logger")
    async def test_if_error_sent_to_message_handler_it_logs(self, logger, _):
        msgs = mock.MagicMock()
        error = mock.MagicMock()

        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )
        consumer._message_handler(msgs, error, None)

        logger.error.assert_called_once_with(error)

    @mock.patch("job_controller.station_consumer.Memphis")
    async def test_start_consuming_connects_if_connection_not_active(self, memphis):
        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )
        consumer.connect_to_broker = mock.MagicMock()
        memphis.is_connection_active = False

        consumer.start_consuming(run_once=True)

        consumer.connect_to_broker.assert_called_once_with()

    @mock.patch("job_controller.station_consumer.Memphis")
    @mock.patch("job_controller.station_consumer.asyncio.wait")
    async def test_start_consuming_calls_consume_and_passes_message_handler(self, asyncio_wait, _):
        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )
        consumer.consume = mock.MagicMock()

        consumer.start_consuming(run_once=True)

        consumer.consume.assert_called_once_with(consumer._message_handler)
        asyncio_wait.assert_called_once()

    @mock.patch("job_controller.station_consumer.Memphis")
    @mock.patch("job_controller.station_consumer.logger")
    async def test_if_consume_throws_an_error_is_logged(self, logger, _):
        def raise_(ex):
            raise ex

        consumer = await create_station_consumer(
            message_callback=mock.MagicMock,
            broker_ip=mock.MagicMock(),
            username=mock.MagicMock(),
            password=mock.MagicMock(),
        )
        consumer.consume = lambda: raise_(MemphisError("Problem!"))

        consumer.start_consuming(run_once=True)

        logger.error.assert_called_once_with("Memphis error occurred: %s", "Problem!")
