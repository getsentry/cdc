import pytest
import yaml

from mock import patch, ANY
from cdc.utils.registry import Configuration
from cdc.utils.stats import Stats

CONFIG = """
host: "somewhere"
port: 8125
msg_sampling_rate: 0.1
task_sampling_rate: 1
"""


@patch("datadog.DogStatsd.timing")
@patch("datadog.DogStatsd.increment")
def test(mock_incr, mock_timing) -> None:
    configuration = yaml.load(CONFIG, Loader=yaml.SafeLoader)
    stats = Stats(configuration)
    stats.message_written()
    mock_incr.assert_called_with(
        Stats.MESSAGE_WRITTEN_METRIC, tags=None, sample_rate=0.1
    )

    stats.message_flushed(10)
    mock_timing.assert_called_with(
        Stats.MESSAGE_FLUSHED_METRIC, ANY, tags=None, sample_rate=0.1
    )

    stats.task_executed(10, "task")
    mock_timing.assert_called_with(
        Stats.TASK_EXECUTED_TIME_METRIC, ANY, tags=["tasktype:task"], sample_rate=1
    )
