from typing import Mapping, NamedTuple, NewType, Optional

from cdc.types import Payload


class StreamMessage(NamedTuple):
    payload: Payload
    metadata: Optional[Mapping[str, str]] = None
