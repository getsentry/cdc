import functools
import logging
from datetime import datetime

from cdc.logging import LoggerAdapter
from cdc.registry import Configuration
from cdc.sources import Source, source_factory
from cdc.streams import Publisher, publisher_factory


logger = LoggerAdapter(logging.getLogger(__name__))


class Application(object):
    def __init__(self, source: Source, publisher: Publisher):
        self.source = source
        self.publisher = publisher

    def run(self) -> None:
        self.source.validate()
        self.publisher.validate()

        iterations_without_source_message = 0
        try:
            message = None
            while True:
                # In most circumstances, we won't have a message ready to be
                # published and we'll need to fetch the next message to publish
                # from the source. In some situations, there might already be a
                # message from the last loop iteration that is still waiting to
                # be sent. If that's the case, we don't need to get a new
                # message.
                if message is None:
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

                # It's also possible that we're all caught up, and there are no
                # new changes available to be fetched from the source right
                # now. In that case, there is no message to be sent. Normally,
                # we'll have a message ready to go (either from the last loop
                # iteration, or a new one that we fetched above) that we can
                # try to publish. (If this fails, we'll try again on the next
                # iteration.)
                if message is not None:
                    logger.trace("Trying to write message to %r...", self.publisher)
                    try:
                        self.publisher.write(
                            message.payload,
                            callback=functools.partial(
                                self.source.set_flush_position,
                                message.id,
                                message.position,
                            ),
                        )
                    except BufferError as e:  # TODO: too coupled to kafka impl
                        logger.trace(
                            "Failed to write %r to %r due to %r, will retry.",
                            message,
                            self.publisher,
                            e,
                        )
                    else:
                        logger.trace(
                            "Succesfully wrote %r to %r.", message, self.publisher
                        )
                        self.source.set_write_position(message.id, message.position)
                        message = None

                # Invoke any queued delivery callbacks here, since these may
                # change the deadline of any scheduled tasks.
                logger.trace("Invoking queued delivery callbacks...")
                self.publisher.poll(0)

                # If there are any scheduled tasks that need to be performed on
                # the source (updating our positions, or sending keep-alive
                # messages, for example), run them now.
                now = datetime.now()
                while True:
                    task = self.source.get_next_scheduled_task(now)
                    if not task or task.deadline > now:
                        logger.trace("There are no scheduled tasks to perform.")
                        break

                    logger.trace("Executing scheduled task: %r", task)
                    task.callable()

                # There are two situations where we need to block to avoid busy
                # waiting. The first (and more obvious) one is if we still have
                # a ``message`` record, since that signals that we failed to
                # publish it during this current loop iteration. In that case,
                # we need to wait for the publisher to have the capacity to
                # accept a new message. The second is a little trickier -- to
                # prevent a unnecessary (and potentially expensive) ``poll`` on
                # the source, we only block on the source if the last two
                # consecutive calls to ``source.fetch`` have both not returned
                # a new message. If we need to block for either scenario, we
                # also need to keep in mind that there are scheduled tasks that
                # we have promised to execute within a reasonable timeframe. We
                # can only wait as long as the next deadline.
                if message is not None or iterations_without_source_message >= 2:
                    now = datetime.now()
                    task = self.source.get_next_scheduled_task(now)
                    timeout = (task.deadline - now).total_seconds()
                    if not timeout > 0:
                        continue  # avoid blocking if we're past deadline

                    if message is None:
                        logger.trace(
                            "Waiting for %r for up to %0.4f seconds before next task: %r...",
                            self.source,
                            timeout,
                            task,
                        )
                        self.source.poll(timeout)
                    else:
                        logger.trace(
                            "Waiting for %r for up to %0.4f seconds next task: %r...",
                            self.publisher,
                            timeout,
                            task,
                        )
                        self.publisher.poll(timeout)
        except KeyboardInterrupt as e:
            logger.debug("Caught %r, shutting down...", e)
            logger.debug(
                "Waiting for %s messages to flush and committing positions before exiting...",
                len(self.publisher),
            )
            self.publisher.flush(60.0)
            self.source.commit_positions()


def application_factory(configuration: Configuration) -> Application:
    return Application(
        source=source_factory(configuration["source"]),
        publisher=publisher_factory(configuration["publisher"]),
    )
