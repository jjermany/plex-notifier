import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

class TZFormatter(logging.Formatter):
    """Logging formatter that uses TZ env var and 12-hour time."""
    def __init__(self, fmt=None, datefmt=None):
        tz_name = os.environ.get("TZ")
        self.tz = ZoneInfo(tz_name) if tz_name else None
        super().__init__(fmt=fmt, datefmt=datefmt)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p %Z")
