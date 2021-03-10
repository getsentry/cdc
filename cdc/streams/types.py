from cdc.types import Payload
from typing import Mapping, NamedTuple, NewType, Optional


class StreamMessage(NamedTuple):
    payload: Payload
    metadata: Optional[Mapping[str, str]] = None
