from cdc.streams.types import StreamMessage
from collections import deque
from datetime import datetime, timedelta
from unittest.mock import ANY, call, MagicMock
from threading import Event, Semaphore, Thread
from typing import Any, Callable, List, Mapping, Optional, Sequence, Tuple

from cdc.sources import Source, CdcMessage
from cdc.sources.backends import SourceBackend
from cdc.sources.types import (
    ChangeMessage,
    CommitMessage,
    ReplicationEvent,
    Payload,
    Position,
)
from cdc.streams.backends import ProducerBackend
from cdc.types import ScheduledTask
from cdc.producer import Producer
from cdc.streams.producer import Producer as StreamProducer
from cdc.streams.types import StreamMessage

ACQUIRE_TIMEOUT = 5


class FakeProducerBackend(ProducerBackend):
    def __init__(self, semaphore: Semaphore) -> None:
        self.__semaphore = semaphore
        self.mocked_write = MagicMock()
        self.__callbacks = deque()

    def __len__(self) -> int:
        return 0

    def write(self, payload: StreamMessage, callback: Callable[[], None]) -> None:
        self.__semaphore.release()
        self.mocked_write(payload)
        self.__callbacks.append(callback)
        # callback()

    def poll(self, timeout: float) -> None:
        while self.__callbacks:
            self.__callbacks.popleft()()

    def flush(self, timeout: float) -> None:
        pass


class FakeSourceBackend(SourceBackend):
    def __init__(
        self, messages: Sequence[ReplicationEvent], tasks: Callable[[], ScheduledTask]
    ) -> None:
        self.get_next_scheduled_task = MagicMock(side_effect=tasks)
        self.commit_positions = MagicMock()
        self.mocked_fetch = MagicMock()
        self.__messages = messages.__iter__()

    def poll(self, timeout: float) -> None:
        return

    def fetch(self) -> Optional[ReplicationEvent]:
        self.mocked_fetch()  # calls the mocked method to be able to run assertions
        try:
            return next(self.__messages)
        except:
            return None

    def get_next_scheduled_task(self, now: datetime) -> Optional[ScheduledTask]:
        pass

    def commit_positions(
        self, write_position: Optional[Position], flush_position: Optional[Position]
    ) -> None:
        pass


def run_loop(
    expect_iterations: int,
    messages: Sequence[ReplicationEvent],
    tasks: Callable[[], ScheduledTask],
    checker: Callable[[FakeSourceBackend, FakeProducerBackend], None],
) -> None:
    """
    Controls the CDC producer (in a dedicated thread since the main loop there
    would run forever otherwise). The fake producer backend releases the semaphore
    every time a message is written, thus waking up the main method. Once this is
    done the expected number of times this stops the producer like if we stopped the
    production system. The semaphore allows us not to sleep a fixed amount of time.
    """
    semaphore = Semaphore()

    fake_source_backend = FakeSourceBackend(messages, tasks)
    fake_producer_backend = FakeProducerBackend(semaphore)
    producer = Producer(
        source=Source(fake_source_backend),
        producer=StreamProducer(fake_producer_backend),
        stats=MagicMock(),
    )

    t = Thread(target=lambda producer: producer.run(), args=(producer,))
    t.start()

    for _ in range(expect_iterations):
        semaphore.acquire(ACQUIRE_TIMEOUT)
    producer.stop()
    t.join()
    checker(fake_source_backend, fake_producer_backend)


def test_empty() -> None:
    """
    Verifies the main loop without processing any message
    """

    def checker(source: FakeSourceBackend, producer: FakeProducerBackend):
        source.mocked_fetch.assert_called()
        source.commit_positions.assert_called_once()
        source.get_next_scheduled_task.assert_called()
        producer.mocked_write.assert_not_called()

    run_loop(
        0,
        messages=[],
        tasks=lambda _: ScheduledTask(
            datetime.now() + timedelta(seconds=100), None, None
        ),
        checker=checker,
    )


def test_one_message() -> None:
    """
    Test one single message.
    """

    message = ChangeMessage(Position(10), Payload(b"MY MESSAGE"), "mytable")

    def checker(source: FakeSourceBackend, producer: FakeProducerBackend):
        source.mocked_fetch.assert_called()
        source.commit_positions.assert_called_once_with(10, 10)
        source.get_next_scheduled_task.assert_called()
        producer.mocked_write.assert_called_once_with(
            StreamMessage(message.payload, metadata={"table": "mytable"})
        )

    run_loop(
        1,
        messages=[message],
        tasks=lambda _: ScheduledTask(
            datetime.now() + timedelta(seconds=100), None, None
        ),
        checker=checker,
    )


def test_multiple_messages() -> None:
    """
    Tests the order of multiple messages.
    """

    messages = [
        CommitMessage(Position(pos), Payload("M %d" % pos)) for pos in range(10)
    ]

    def checker(source: FakeSourceBackend, producer: FakeProducerBackend):
        source.mocked_fetch.assert_called()
        source.get_next_scheduled_task.assert_called()
        source.commit_positions.assert_called_once_with(9, 9)
        producer.mocked_write.assert_has_calls(
            [call(StreamMessage(m.payload)) for m in messages]
        )

    run_loop(
        10,
        messages=messages,
        tasks=lambda _: ScheduledTask(
            datetime.now() + timedelta(seconds=100), None, None
        ),
        checker=checker,
    )
