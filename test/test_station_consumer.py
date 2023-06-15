# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access
import asyncio
import unittest
from asyncio import Future
from typing import Iterator, Any
from unittest import mock
from unittest.mock import AsyncMock

import asynctest as asynctest
import pytest
from asynctest import MagicMock
from memphis import MemphisError, Memphis

from job_controller.station_consumer import create_station_consumer


class AwaitableMock(AsyncMock):
    def __await__(self) -> Iterator[Any]:
        self.await_count += 1
        return iter([])


class AwaitableNonAsyncMagicMock(MagicMock):
    def __await__(self) -> Iterator[Any]:
        return iter([])


class StationConsumerTest(asynctest.TestCase):
    @pytest.mark.asyncio
    async def test_consumer_is_made_and_connects_with_passed_values(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as memphis:
            memphis.is_connection_active = True
            broker_ip = mock.MagicMock()
            username = mock.MagicMock()
            password = mock.MagicMock()

            await create_station_consumer(
                message_callback=mock.MagicMock, broker_ip=broker_ip, username=username, password=password
            )

            memphis().connect.assert_called_once_with(host=broker_ip, username=username, password=password)

    @pytest.mark.asyncio
    async def test_consumer_subscribes_using_memphis_consumer(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as memphis:
            broker_ip = mock.MagicMock()
            username = mock.MagicMock()
            password = mock.MagicMock()

            await create_station_consumer(
                message_callback=mock.MagicMock, broker_ip=broker_ip, username=username, password=password
            )

            memphis().consumer.assert_called_once_with(
                station_name="requested-jobs",
                consumer_name="jobcontroller",
                consumer_group="jobcontrollers",
                generate_random_suffix=True,
            )

    @pytest.mark.asyncio
    async def test_consumer_throwing_memphis_error_is_raised(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as memphis:
            broker_ip = mock.MagicMock()
            username = mock.MagicMock()
            password = mock.MagicMock()
            memphis.MemphisError = MemphisError

            async def return_error(station_name, consumer_name, consumer_group, generate_random_suffix):
                assert station_name == "requested-jobs"
                assert consumer_name == "jobcontroller"
                assert consumer_group == "jobcontrollers"
                assert generate_random_suffix
                return MemphisError("")

            memphis().consumer = return_error

            async def assert_raises_async(async_function, expected_exception, *args, **kwargs):
                try:
                    await async_function(*args, **kwargs)
                except expected_exception as _:
                    return
                raise AssertionError("Passed async function did not raise")

            await assert_raises_async(
                create_station_consumer,
                MemphisError,
                message_callback=mock.MagicMock,
                broker_ip=broker_ip,
                username=username,
                password=password,
            )

    @pytest.mark.asyncio
    async def test_message_handler_decodes_string_into_dict_correctly(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            message = '{"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}'
            message_mock = AwaitableNonAsyncMagicMock()
            message_mock.get_data.return_value = message
            msgs = [message_mock]

            consumer = await create_station_consumer(
                message_callback=mock.MagicMock(),
                broker_ip=mock.MagicMock(),
                username=mock.MagicMock(),
                password=mock.MagicMock(),
            )

            await consumer._message_handler(msgs, None, None)
            consumer.message_callback.assert_called_once_with(
                {"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}
            )

    @pytest.mark.asyncio
    @mock.patch("job_controller.station_consumer.Memphis", new=AsyncMock)
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

    @pytest.mark.asyncio
    @mock.patch("job_controller.station_consumer.Memphis", new=AsyncMock)
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

    @pytest.mark.asyncio
    @mock.patch("job_controller.station_consumer.Memphis", new=AsyncMock)
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

    @pytest.mark.asyncio
    @mock.patch("job_controller.station_consumer.Memphis", new=AsyncMock)
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

    @pytest.mark.asyncio
    @mock.patch("job_controller.station_consumer.Memphis", new=AsyncMock)
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

    @pytest.mark.asyncio
    @mock.patch("job_controller.station_consumer.Memphis", new=AsyncMock)
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
