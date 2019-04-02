import atexit
import click
import logging, logging.config
import yaml
from pkg_resources import cleanup_resources, resource_filename


atexit.register(cleanup_resources)


logging.addLevelName(5, "TRACE")


@click.group()
@click.option(
    "--logging-configuration",
    type=click.File("r"),
    default=resource_filename("cdc", "configuration/logging.conf"),
    help="Path to logging configuration file.",
)
@click.option(
    "--log-level",
    type=click.Choice([name for level, name in sorted(logging._levelToName.items())]),
    default=None,
    help="Override root logger level.",
)
def main(log_level, logging_configuration):
    logging.config.fileConfig(logging_configuration)

    if log_level is not None:
        logging.getLogger().setLevel(logging._nameToLevel[log_level])


@main.command(help="Extract changes from the source and write them to the stream.")
@click.argument("configuration_file", type=click.File("r"))
def producer(configuration_file):
    configuration = yaml.load(configuration_file, Loader=yaml.SafeLoader)

    from cdc.producer import Producer
    from cdc.sources import source_factory
    from cdc.streams import publisher_factory

    Producer(
        source=source_factory(configuration["source"]),
        publisher=publisher_factory(configuration["publisher"]),
    ).run()


@main.command(help="Consume changes from the stream and apply them to the target.")
@click.argument("configuration_file", type=click.File("r"))
def consumer(configuration_file):
    configuration = yaml.load(configuration_file, Loader=yaml.SafeLoader)

    raise NotImplementedError


if __name__ == "__main__":
    main()
