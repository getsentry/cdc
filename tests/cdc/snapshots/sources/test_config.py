import yaml
from cdc.snapshots.snapshot_types import (
    ColumnConfig,
    DateFormatPrecision,
    DateTimeFormatterConfig,
    TableConfig,
)

CONFIG = """
tables:
    -   table: sentry_groupedmessage
        columns:
            -   name: 'project_id'
            -   name: 'id'
            -   name: 'status'
            -   name: 'last_seen'
                formatter:
                    type: 'datetime'
                    precision: 'second'
            -   name: 'first_seen'
                formatter:
                    type: 'datetime'
                    precision: 'second'
"""


def test_table_config_load() -> None:
    snapshot_config = yaml.load(CONFIG, Loader=yaml.SafeLoader)
    tables_config = [TableConfig.from_dict(t) for t in snapshot_config["tables"]]

    assert tables_config == [
        TableConfig(
            table="sentry_groupedmessage",
            columns=[
                ColumnConfig(name="project_id"),
                ColumnConfig(name="id"),
                ColumnConfig(name="status"),
                ColumnConfig(
                    name="last_seen",
                    formatter=DateTimeFormatterConfig(DateFormatPrecision.SECOND),
                ),
                ColumnConfig(
                    name="first_seen",
                    formatter=DateTimeFormatterConfig(DateFormatPrecision.SECOND),
                ),
            ],
        )
    ]
