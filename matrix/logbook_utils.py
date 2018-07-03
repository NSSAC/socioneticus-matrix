"""
Utility stuff configuring lobook.
"""


from logbook.handlers import Handler
from logbook.base import (
    CRITICAL, ERROR, WARNING, NOTICE, INFO, DEBUG, TRACE, NOTSET
)

LOG_LEVEL_COLOR_ATTR = {
    CRITICAL: "red_bold",
    ERROR: "red_bold",
    WARNING: "yellow_bold",
    NOTICE: "green_bold",
    INFO: "green_bold",
    DEBUG: "blue",
    TRACE: "blue",
    NOTSET: "blue"
}

def log_formatter(t, record, _):
    """
    Format the log record.
    """

    time_str = t.white_bold(f"{record.time:%Y-%m-%d %H:%M:%S.%f%z}")
    channel_str = t.cyan_bold(record.channel)
    message_str = record.message

    color_attr = LOG_LEVEL_COLOR_ATTR[record.level]
    level_name_str = getattr(t, color_attr)(record.level_name)

    return f"[{time_str}] {level_name_str}: {channel_str}: {message_str}"

class ChannelFilterHandler(Handler):
    """
    A handler that gobbles up events from specific channels.
    """

    blackhole = True

    def __init__(self, channels, level=NOTSET, filter=None): # pylint: disable=redefined-builtin
        super().__init__(level=level, filter=filter, bubble=False)
        self.channels = set(channels)

    def should_handle(self, record):
        if (record.level >= self.level and record.channel in self.channels):
            return True
        return False
