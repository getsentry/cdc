import atexit
import click
import logging, logging.config
import yaml
from pkg_resources import cleanup_resources, resource_filename


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

    configuration = ctx.obj

    Producer(
        source=source_factory(configuration["source"]),
        producer=producer_factory(configuration["producer"]),
    ).run()


@main.command(
    help="Consume changes from the stream consumer and apply them to the target."
)
@click.pass_context
def consumer(ctx):
    raise NotImplementedError


if __name__ == "__main__":
    main()
