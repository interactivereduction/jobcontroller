# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access

from typing import Iterator, Any
from unittest import mock
from unittest.mock import AsyncMock

import asynctest as asynctest
import pytest
from asynctest import MagicMock
from memphis import MemphisError

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
    async def test_message_handler_handles_json_decode_error(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            message = "}broken-json{"
            message_mock = AwaitableNonAsyncMagicMock()
            message_mock.get_data.return_value = message
            msgs = [message_mock]

            consumer = await create_station_consumer(
                message_callback=mock.MagicMock,
                broker_ip=mock.MagicMock(),
                username=mock.MagicMock(),
                password=mock.MagicMock(),
            )

            consumer.message_callback = mock.MagicMock()
            await consumer._message_handler(msgs, None, None)
            consumer.message_callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_ack_is_called_per_msg(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            msg1 = AwaitableNonAsyncMagicMock()
            msg1.get_data.return_value = "{}"
            msg2 = AwaitableNonAsyncMagicMock()
            msg2.get_data.return_value = "{}"
            msgs = [msg1, msg2]

            consumer = await create_station_consumer(
                message_callback=mock.MagicMock,
                broker_ip=mock.MagicMock(),
                username=mock.MagicMock(),
                password=mock.MagicMock(),
            )

            await consumer._message_handler(msgs, None, None)
            msg1.ack.assert_called_once_with()
            msg2.ack.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_if_error_sent_to_message_handler_it_logs(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            with mock.patch("job_controller.station_consumer.logger") as logger:
                msgs = mock.MagicMock()
                error = mock.MagicMock()

                consumer = await create_station_consumer(
                    message_callback=mock.MagicMock,
                    broker_ip=mock.MagicMock(),
                    username=mock.MagicMock(),
                    password=mock.MagicMock(),
                )
                await consumer._message_handler(msgs, error, None)

                logger.error.assert_called_once_with(error)

    @pytest.mark.asyncio
    async def test_start_consuming_connects_if_connection_not_active(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as memphis:
            with mock.patch("job_controller.station_consumer.asyncio.wait", new=AwaitableNonAsyncMagicMock()) as _:
                consumer = await create_station_consumer(
                    message_callback=mock.MagicMock,
                    broker_ip=mock.MagicMock(),
                    username=mock.MagicMock(),
                    password=mock.MagicMock(),
                )
                consumer.consumer = mock.MagicMock()
                consumer.connect_to_broker = AwaitableNonAsyncMagicMock()
                memphis().is_connection_active = False

                await consumer.start_consuming(run_once=True)

                consumer.connect_to_broker.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_start_consuming_calls_consume_and_passes_message_handler(self):
        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            with mock.patch(
                "job_controller.station_consumer.asyncio.wait", new=AwaitableNonAsyncMagicMock()
            ) as asyncio_wait:
                consumer = await create_station_consumer(
                    message_callback=mock.MagicMock,
                    broker_ip=mock.MagicMock(),
                    username=mock.MagicMock(),
                    password=mock.MagicMock(),
                )
                consumer.consumer = mock.MagicMock()

                await consumer.start_consuming(run_once=True)

                consumer.consumer.consume.assert_called_once_with(consumer._message_handler)
                asyncio_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_if_consume_throws_an_error_is_logged(self):
        def raise_(ex):
            raise ex

        with mock.patch("job_controller.station_consumer.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            with mock.patch("job_controller.station_consumer.logger") as logger:
                consumer = await create_station_consumer(
                    message_callback=mock.MagicMock,
                    broker_ip=mock.MagicMock(),
                    username=mock.MagicMock(),
                    password=mock.MagicMock(),
                )
                memphis_error = MemphisError("Problem!")
                consumer.consumer = mock.MagicMock()
                consumer.consumer.consume = lambda _: raise_(memphis_error)

                await consumer.start_consuming(run_once=True)

                logger.error.assert_called_once_with("Memphis error occurred: %s", memphis_error)
