import yaml
from cdc.snapshots.snapshot_types import (ColumnConfig,
                                          DateTimeFormatterConfig, TableConfig)

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
                    format: '%Y-%m-%d %H:%M:%S'
            -   name: 'first_seen'
                formatter:
                    type: 'datetime'
                    format: '%Y-%m-%d %H:%M:%S'
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
                    formatter=DateTimeFormatterConfig("%Y-%m-%d %H:%M:%S"),
                ),
                ColumnConfig(
                    name="first_seen",
                    formatter=DateTimeFormatterConfig("%Y-%m-%d %H:%M:%S"),
                ),
            ],
        )
    ]
