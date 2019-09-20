import atexit
import click
import jsonschema  # type: ignore
import logging, logging.config
import signal
import yaml
import sentry_sdk

from typing import Any
from pkg_resources import cleanup_resources, resource_filename
from sentry_sdk.integrations.logging import LoggingIntegration

from cdc.snapshots.snapshot_types import TableConfig

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
    if configuration["version"] != 1:
        raise Exception("Invalid configuration file version")

    if configuration.get("sentry") and configuration["sentry"]["enabled"]:
        sentry_logging = LoggingIntegration(
            level=logging.DEBUG, event_level=logging.WARNING
        )
        sentry_sdk.init(
            dsn=configuration["sentry"]["dsn"],
            integrations=[sentry_logging],
            max_breadcrumbs=10,
        )

    if configuration.get("logging"):
        logging.config.dictConfig(configuration["logging"])

    if log_level is not None:
        logging.getLogger().setLevel(logging._nameToLevel[log_level])


@main.command(
    help="Extract changes from the source and write them to the stream producer."
)
@click.pass_context
def producer(ctx):
    from cdc.producer import Producer
    from cdc.sources import source_factory
    from cdc.streams import producer_factory
    from cdc.utils.stats import Stats

    configuration = ctx.obj
    producer = Producer(
        source=source_factory(configuration["source"]),
        producer=producer_factory(configuration["producer"]),
        stats=Stats(configuration["dogstatsd"]),
    )

    def handle_interrupt(num: int, frame: Any) -> None:
        logging.getLogger(__name__).debug("Caught %r, shutting down...", num)
        producer.stop()

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)
    producer.run()


@main.command(
    help="Consume changes from the stream consumer and apply them to the target."
)
@click.pass_context
def consumer(ctx):
    raise NotImplementedError


@main.command(
    help="Takes a snapshot and coordinate the process to load it in Clickhouse"
)
@click.option(
    "-s",
    "--snapshot-config",
    type=click.File("r"),
    help="Path to the snapshot configuration file.",
)
@click.pass_context
def snapshot(ctx, snapshot_config):
    from cdc.snapshots.snapshot_coordinator import SnapshotCoordinator
    from cdc.snapshots.sources import registry as source_registry
    from cdc.snapshots.destinations import registry as destination_registry
    from cdc.snapshots.snapshot_control import SnapshotControl
    from cdc.streams import producer_factory
    configuration = ctx.obj
    
    snapshot_config = yaml.load(snapshot_config, Loader=yaml.SafeLoader)
    if configuration["version"] != 1:
        raise Exception("Invalid snapshot configuration file version")

    jsonschema.validate(
        snapshot_config,
        {
            "type": "object",
            "properties": {
                #TODO: make product more restrictive once we have a better idea on how to use it
                "product": {"type": "string"},
                "destination": {"type": "object"},
                "tables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "table": {"type": "string"},
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                            }
                        },
                        "required": ["table"],
                    }
                }
            },
            "required": ["product", "destination", "tables"],
        },
    )

    tables_config = [
        TableConfig(t['table'], t.get('columns'))
        for t in snapshot_config['tables']
    ]

    coordinator = SnapshotCoordinator(
        source_registry.new(
            configuration["snapshot"]["source"]["type"],
            configuration["snapshot"]["source"]["options"],
        ),
        destination_registry.new(
            snapshot_config["destination"]["type"],
            snapshot_config["destination"]["options"],
        ),
        SnapshotControl(
            producer_factory(configuration["snapshot"]["control"]["producer"]),
            configuration["snapshot"]["control"].get("options"),
        ),
        snapshot_config["product"],
        tables_config,
    )

    coordinator.start_process()


@main.command(
    help="Aborts a snapshot by sending the message on the control topic"
)
@click.option(
    "-s",
    "--snapshot-id",
    type=click.STRING,
    help="Snapshot ID to stop",
)
@click.pass_context
def snapshot_abort(ctx, snapshot_id):
    from uuid import UUID
    from cdc.snapshots.snapshot_control import SnapshotControl
    from cdc.streams import producer_factory
    configuration = ctx.obj
    
    if configuration["version"] != 1:
        raise Exception("Invalid snapshot configuration file version")

    control = SnapshotControl(
        producer_factory(configuration["snapshot"]["control"]["producer"]),
        configuration["snapshot"]["control"].get("options"),
    )
    control.abort_snapshot(UUID(snapshot_id))
    control.wait_messages_sent()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.exception(e)
        raise e
