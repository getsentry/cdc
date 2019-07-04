import atexit
import click
import logging, logging.config
import signal
import yaml
import sentry_sdk

from typing import Any
from pkg_resources import cleanup_resources, resource_filename
from sentry_sdk.integrations.logging import LoggingIntegration

atexit.register(cleanup_resources)


# This needs to occur before the `--log-level` option choices are defined,
# since we'd like this to be available as an option.
logging.addLevelName(5, "TRACE")


@click.group()
@click.option(
    "-c",
    "--configuration-file",
    type=click.File("r"),
    default=resource_filename("cdc", "configuration.yaml"),
    help="Path to configuration file.",
)
@click.option(
    "--log-level",
    type=click.Choice([name for level, name in sorted(logging._levelToName.items())]),
    default=None,
    help="Overrides the root logger log level.",
)
@click.pass_context
def main(ctx, configuration_file, log_level):
    configuration = ctx.obj = yaml.load(configuration_file, Loader=yaml.SafeLoader)
    if configuration.pop("version") != 1:
        raise Exception("Invalid configuration file version")

    if 'sentry' in configuration:
        sentry_configuration = configuration.pop('sentry')
        if sentry_configuration.get('enabled', False):
            sentry_logging = LoggingIntegration(level=logging.DEBUG, event_level=logging.WARNING)
            sentry_sdk.init(dsn=sentry_configuration["dsn"], integrations=[sentry_logging], max_breadcrumbs=10)

    if 'logging' in configuration:
        logging.config.dictConfig(configuration.pop("logging"))

    if log_level is not None:
        logging.getLogger().setLevel(logging._nameToLevel[log_level])


@main.command(
    help="Extract changes from the source and write them to the stream producer."
)
@click.pass_context
def producer(ctx):
    from cdc.producer import Producer
    from cdc.utils.loader import load

    producer: Producer = load(Producer, ctx.obj)

    def handle_interrupt(num: int, *args: Any, **kwargs: Any) -> None:
        logging.getLogger(__name__).debug("Caught %r, shutting down...", num)
        producer.stop()

    signal.signal(signal.SIGINT, handle_interrupt)
    producer.run()


@main.command(
    help="Consume changes from the stream consumer and apply them to the target."
)
@click.pass_context
def consumer(ctx):
    raise NotImplementedError


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.exception(e)
        raise e
