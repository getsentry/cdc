import jsonschema  # type: ignore
import logging
import uuid

from abc import ABC, abstractmethod
from typing import Any, AnyStr, IO, Mapping, Sequence

from cdc.snapshots.destinations import DestinationContext
from cdc.snapshots.sources import SnapshotSource
from cdc.snapshots.snapshot_types import SnapshotId, TablesConfig
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
        product: str,
        tables: Sequence[TablesConfig]) -> None:
        self.__source = source
        self.__destination = destination
        self.__product = product
        self.__tables = tables


    def start_process(self) -> None:
        logger.debug("Starting snapshot process for product %s", self.__product)
        snapshot_id = uuid.uuid1()
        logger.info("Starting snapshot ID %s", snapshot_id)
        # TODO: pause consumer
        with self.__destination.open_snapshot(
            SnapshotId(str(snapshot_id)),
            self.__product) as snapshot_out:
            
            logger.info("Snapshot ouput: %s", snapshot_out.get_name())
            snapshot_desc = self.__source.dump(
                snapshot_out,
                self.__tables,   
            )
            logger.info("Snapshot taken: %r", snapshot_desc)

            # TODO: coordinate with the consumer to load the snapshot
