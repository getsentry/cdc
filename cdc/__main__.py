import logging, logging.config
import optparse
import yaml

from cdc.application import Application


parser = optparse.OptionParser()
parser.add_option("-f", "--configuration-file", default="configuration.yaml")
parser.add_option("--log-level")
options, arguments = parser.parse_args()

configuration = yaml.load(open(options.configuration_file))

if configuration["logging"]:
    logging.config.dictConfig(configuration["logging"])

if options.log_level is not None:
    logging.getLogger().setLevel(logging._nameToLevel[options.log_level])

application = Application(configuration)
application.run()