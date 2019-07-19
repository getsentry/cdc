import jsonschema  # type: ignore
import logging

from abc import ABC, abstractmethod
from typing import AnyStr, IO, List

from cdc.snapshots.destinations import DestinationContext
from cdc.snapshots.sources import SnapshotSource
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))

class SnapshotCoordinator(ABC):
    """
    Coordinates the process of taking a snapshot from the source database
    and provide the snapshot to the any interested consumer.
    The process consists into:
    - communicating the process has started to all listeners TODO
    - use the SnapshotSource to take a snapshot from the DB and materialize it
      into destination.
    - communicate the details of the snapshot to all the listeners TODO
    """

    def __init__(self,
        source: SnapshotSource,
        destination: DestinationContext,
        configuration: Configuration,
        tables: List[str]) -> None:
        self.__source = source
        self.__destination = destination

        jsonschema.validate(
            configuration,
            {
                "type": "object",
                "properties": {
                    "default_tables": {
                        "type": "array",
                        "items": {
                            "type": "string",    
                        }
                    },
                    "dump": {"type": "object"}
                },
                "required": ["default_tables", "dump"],
            },
        )

        self.__tables = tables or configuration["default_tables"]


    def start_process(self) -> None:
        logger.debug("Starting snapshot process for tables %s" % (",".join(self.__tables)))
        # TODO: pause consumer

        with self.__destination as out_file:
            logger.info("Snapshot ouput: %s" % out_file.get_name())
            # do stuff
            snapshot_desc = self.__source.dump(out_file, self.__tables)
            logger.info("Snapshot taken: %s" % snapshot_desc)

    