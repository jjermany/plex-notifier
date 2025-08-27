import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

TZ_NAME = os.getenv("TZ", "UTC")
try:
    LOCAL_TZ = ZoneInfo(TZ_NAME)
except Exception:
    LOCAL_TZ = ZoneInfo("UTC")

HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "100"))


class TZFormatter(logging.Formatter):
    """Formatter that uses the container's timezone and 12-hour clock."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, LOCAL_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p %Z")
