import pytest

from cdc.sources.backends.postgres_logical import parse_message_with_headers
from cdc.sources.types import (
    BeginMessage,
    ChangeMessage,
    CommitMessage,
    GenericMessage,
    ReplicationEvent,
    Payload,
    Position,
)

INSERT = b"""{
    "event": "change",
    "kind": "insert",
    "schema": "public",
    "table": "table_with_unique",
    "columnnames": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"],
    "columntypes": ["int2", "int2", "int4", "int8", "numeric", "float4", "float8", "bpchar", "varchar", "text", "varbit", "timestamp", "date", "bool", "json", "tsvector"],
    "columnvalues": [1, 1, 2, 3, 3.540, 876.563, 1.23, "teste     ", "testando", "um texto longo", "001110010101010", "Sat Nov 02 17:30:52 2013", "02-04-2013", true, "{ \"a\": 123 }", "'Old' 'Parr'"]
}
"""

PAYLOAD_WITH_ESCAPE = b"""{
    "event": "change",
    "|||": "\\\\\\"
}
"""

test_data = [
    (
        b'B|{"event":"begin", "xid":123123}',
        BeginMessage(Position(1), Payload(b'{"event":"begin", "xid":123123}')),
    ),
    (
        b'C|{"event":"commit", "xid":123123}',
        CommitMessage(Position(1), Payload(b'{"event":"commit", "xid":123123}')),
    ),
    (
        b'G|{"event":"something else", "xid":123123}',
        GenericMessage(
            Position(1), Payload(b'{"event":"something else", "xid":123123}')
        ),
    ),
    (
        b'{"event":"commit", "xid":123123}',
        GenericMessage(Position(1), Payload(b'{"event":"commit", "xid":123123}')),
    ),
    (
        b"M|table_with_unique|" + INSERT,
        ChangeMessage(Position(1), Payload(INSERT), "table_with_unique"),
    ),
    (b"M||" + INSERT, ChangeMessage(Position(1), Payload(INSERT), "")),
    (
        b"M|asd\\\\asd\\||" + INSERT,
        ChangeMessage(Position(1), Payload(INSERT), "asd\\asd|"),
    ),
    (
        b"M|table|" + PAYLOAD_WITH_ESCAPE,
        ChangeMessage(Position(1), Payload(PAYLOAD_WITH_ESCAPE), "table"),
    ),
]


@pytest.mark.parametrize("payload, expected", test_data)
def test_parse_replication_msg(payload: bytes, expected: ReplicationEvent) -> None:
    parsed = parse_message_with_headers(1, payload)
    assert parsed == expected
