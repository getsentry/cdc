import functools
import logging
import time

from datetime import datetime

from cdc.sources import Source, CdcMessage
from cdc.streams.producer import Producer as StreamProducer

from cdc.utils.logging import LoggerAdapter
from cdc.utils.stats import Stats


logger = LoggerAdapter(logging.getLogger(__name__))


class Producer(object):
    """
    Coordinates fetching messages from the source database and publishing to
    the destination stream.
    """

    def __init__(self, source: Source, producer: StreamProducer, stats: Stats):
        self.source = source
        self.producer = producer
        self.__stats = stats

        self.__shutting_down = False

    def stop(self) -> None:
        # TODO: This doesn't wake up the blocking conditions at the end of the
        # run loop, so this can end up waiting unnecessarily if there isn't
        # much activity on the source database.
        logger.debug(
            "Waiting for %s messages to flush and committing positions before exiting...",
            len(self.producer),
        )
        self.__shutting_down = True

    def __produce_callback(self, message: CdcMessage, start: float) -> None:
        self.__stats.message_flushed(start)
        self.source.set_flush_position(message.id, message.payload.position)

    def run(self) -> None:
        iterations_without_source_message = 0
        message = None
        while not self.__shutting_down or len(self.producer):
            # In most circumstances, we won't have a message ready to be
            # published, and we'll need to fetch the next message to publish
            # from the source. There may already be a message ready to be sent
            # if we were unable to successfully publish the message during the
            # last loop iteration -- if that's the case, we don't need to get a
            # new message. We also don't need to receive any messages if we're
            # shutting down.
            if message is None and not self.__shutting_down:
                logger.trace("Trying to fetch message from %r...", self.source)
                message = self.source.fetch()
                if message is not None:
                    logger.trace(
                        "Received message (after %s attempts): %r",
                        iterations_without_source_message,
                        message,
                    )
                    iterations_without_source_message = 0
                else:
                    iterations_without_source_message += 1
                    logger.trace(
                        "Did not receive message (%s attempts so far.)",
                        iterations_without_source_message,
                    )

            # It's also possible that we're all caught up, and there are no new
            # changes available to be fetched from the source right now. In
            # that case, there is no message to be sent. Normally, we'll have a
            # message ready to go (either from the last loop iteration, or a
            # new one that we fetched above) that we can try to publish. (If
            # this fails, we'll try again on the next iteration.)
            if message is not None:
                logger.trace("Trying to write message to %r...", self.producer)
                try:
                    self.producer.write(
                        message.payload.to_stream(),
                        callback=functools.partial(
                            self.__produce_callback, message, time.time()
                        ),
                    )
                except BufferError as e:  # TODO: too coupled to kafka impl
                    logger.trace(
                        "Failed to write %r to %r due to %r, will retry.",
                        message,
                        self.producer,
                        e,
                    )
                else:
                    logger.trace("Succesfully wrote %r to %r.", message, self.producer)
                    self.source.set_write_position(message.id, message.payload.position)
                    message = None

            # Invoke any queued delivery callbacks here, since these may change
            # the deadline of any scheduled tasks.
            logger.trace("Invoking queued delivery callbacks...")
            self.producer.poll(0)

            # If there are any scheduled tasks that need to be performed on the
            # source (updating our positions, or sending keep-alive messages,
            # for example), run them now.
            now = datetime.now()
            task = self.source.get_next_scheduled_task(now)
            while now >= task.deadline:
                logger.trace("Executing scheduled task: %r", task)

                start = time.time()
                task.callable()
                self.__stats.task_executed(start, task.get_type())
                task = self.source.get_next_scheduled_task(now)
            else:
                logger.trace("There are no scheduled tasks to perform.")

            # There are three situations where we need to block to avoid busy
            # waiting until the next scheduled task is ready to be performed.
            # First, if there is still a ``message`` record waiting to be sent,
            # this indicates that we failed to publish it during this loop
            # iteration. In that case, we need to wait for the producer to have
            # the capacity to accept a new message. (This can occur whether or
            # not we are trying to shut down.) Second, if we are shutting down
            # and the producer still has messages in-flight, we need to block
            # until there are delivery callbacks ready to be fired by the
            # producer. (When a delivery callback is fired, the message
            # associated with that callback has been removed from the queue, so
            # we should not have the ability to add our message.) Third, if we
            # are not shutting down or waiting on the producer to have capacity
            # to send additional messages, we must be waiting on the source to
            # provide us with more messages. To improve performance in backlog
            # scenarios, we don't poll the source unless the we have already
            # failed to retrieve a message on least one attempt. If we needto
            # block in any scenario, we also need to keep in mind that there
            # are scheduled tasks that we have promised to execute within a
            # reasonable timeframe. We can only wait as long as the next
            # deadline. If the deadline has already passed, we shouldn't block.
            now = datetime.now()
            timeout = task.get_timeout(now)
            if timeout > 0:
                if (
                    message is not None
                    or self.__shutting_down
                    and len(self.producer) > 0
                ):
                    logger.trace(
                        "Waiting for %r for up to %0.4f seconds before next task: %r...",
                        self.producer,
                        timeout,
                        task,
                    )
                    self.producer.poll(timeout)
                elif not self.__shutting_down and iterations_without_source_message > 0:
                    logger.trace(
                        "Waiting for %r for up to %0.4f seconds before next task: %r...",
                        self.source,
                        timeout,
                        task,
                    )
                    self.source.poll(timeout)

        self.source.commit_positions()
