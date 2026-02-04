import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

class TZFormatter(logging.Formatter):
    """Logging formatter that uses TZ env var, 12-hour time, and short logger names."""
    def __init__(self, fmt=None, datefmt=None):
        tz_name = os.environ.get("TZ")
        self.tz = ZoneInfo(tz_name) if tz_name else None
        super().__init__(fmt=fmt, datefmt=datefmt)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        # Short format: "7:25pm" (time only, no date/timezone for cleaner console logs)
        hour = dt.strftime("%I").lstrip('0') or '0'
        minute = dt.strftime("%M")
        ampm = dt.strftime("%p").lower()
        return f"{hour}:{minute}{ampm}"

    def format(self, record):
        # Shorten logger name: "notifier_app.webapp" -> "webapp"
        record.name = record.name.rsplit('.', 1)[-1]
        return super().format(record)
