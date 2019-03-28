import logging
from datetime import datetime

from cdc.logging import LoggerAdapter
from cdc.sources import Source
from cdc.streams import Publisher


logger = LoggerAdapter(logging.getLogger(__name__))


class Application(object):
    def __init__(self, configuration):
        self.source = Source(configuration["source"])
        self.publisher = Publisher(configuration["stream"])

    def run(self):
        self.source.validate()
        self.publisher.validate()

        try:
            message = None
            while True:
                # In most circumstances, we won't have a message ready to be
                # published and we'll need to fetch the next message to publish
                # from the database. In some situations, there might already be
                # a message from the last loop iteration that is still waiting
                # to be sent. If that's the case, we don't need to get a new
                # message.
                if message is None:
                    logger.trace("Trying to fetch message from %r...", self.source)
                    message = self.source.fetch()
                    if message is not None:
                        logger.trace("Received message: %r", message)
                    else:
                        logger.trace("Did not receive message.")

                # It's also possible that there is nothing interesting
                # happening on the database right now, so there are no messages
                # to be sent. Normally, we'll have a message ready to go
                # (either from the last loop iteration, or a new one that we
                # fetched above) that we can try to publish. (If this fails,
                # we'll try again on the next iteration.)
                if message is not None:
                    logger.trace("Trying to write message to %r...", self.publisher)
                    try:
                        self.publisher.write(
                            message, callback=self.source.set_flush_position
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

                # If there are any queued delivery callbacks, we invoke them
                # here.
                logger.trace("Invoking queued delivery callbacks...")
                self.publisher.poll(0)

                # If there are any scheduled tasks that need to be performed
                # (updating our slot positions, or sending keep-alive messages,
                # for example), we run them now. Ideally, these tasks are non-
                # or minimally blocking.
                now = datetime.now()
                while True:
                    task = self.source.get_next_scheduled_task(now)
                    if not task or task.deadline > now:
                        logger.trace("There are no scheduled tasks to perform.")
                        break

                    logger.trace("Executing scheduled task: %r", task)
                    task.callable()

                # We might need to block here if there are not any more
                # messages ready to be fetched from the database, or if the
                # publish queue is full and we are unable to add new messages
                # to it. The easiest situation to identify is when the queue is
                # full -- if this is the case, we'll still have a message ready
                # to be published. In this case, we can poll the producer for
                # updates. If there is no next message to be published, we're
                # waiting on the database instead. Regardless of which event we
                # are waiting on to occur, we need keep in mind the other tasks
                # we need to accomplish in a reasonable timeframe: sending
                # keep-alive messages, and updating our slot positions. We can
                # only wait as long as the next deadline.
                now = datetime.now()
                task = self.source.get_next_scheduled_task(now)
                max_loop_block_timeout = (
                    10.0
                )  # keep producer.poll from going totally unresponsive
                if task is not None:
                    timeout = min(
                        max((task.deadline - now).total_seconds(), 0),
                        max_loop_block_timeout,
                    )
                else:
                    timeout = max_loop_block_timeout

                waiting_for = self.source if message is None else publisher
                logger.trace(
                    "Waiting for %r for up to %0.4f seconds...", waiting_for, timeout
                )
                waiting_for.poll(timeout)

        except KeyboardInterrupt as e:
            logger.debug("Caught %r, shutting down...", e)
            logger.debug(
                "Waiting for %s messages to flush and committing positions before exiting...",
                len(self.publisher),
            )
            self.publisher.flush(60.0)
            self.source.commit_positions()
