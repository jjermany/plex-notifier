"""Application-wide constants to replace magic numbers."""

# Notification history tracking
NOTIFICATION_HISTORY_LIMIT = 200  # Number of recent notifications to cache per user
NOTIFICATION_CACHE_TTL_SECONDS = 300  # 5 minutes cache for notification history

# History display
HISTORY_ENTRIES_PER_PAGE = 20
MONTHLY_STATS_MONTHS = 12  # Number of months to show in stats

# Subscriptions page
SUBSCRIPTIONS_SHOWS_PER_PAGE = 25  # Number of shows per page on subscriptions page
INACTIVE_SHOW_THRESHOLD_DAYS = 365  # Days without notifications before a show is considered inactive

# Token expiry
UNSUBSCRIBE_TOKEN_EXPIRY_DAYS = 30  # Increased from 7 for better UX
UNSUBSCRIBE_TOKEN_EXPIRY_SECONDS = 86400 * UNSUBSCRIBE_TOKEN_EXPIRY_DAYS

# Retry settings
EMAIL_RETRY_ATTEMPTS = 3
EMAIL_RETRY_MIN_WAIT_SECONDS = 2
EMAIL_RETRY_MAX_WAIT_SECONDS = 16

API_RETRY_ATTEMPTS = 3
API_RETRY_MIN_WAIT_SECONDS = 2
API_RETRY_MAX_WAIT_SECONDS = 10

# Log file settings
USER_LOG_MAX_BYTES = 500_000  # 500KB per user log file
GLOBAL_LOG_MAX_BYTES = 100_000  # 100KB for global notifications log
APP_LOG_MAX_BYTES = 250_000  # 250KB for app log file
LOG_BACKUP_COUNT = 1

# Tautulli API pagination
TAUTULLI_MAX_PAGE_LENGTH = 1000  # Maximum records per page
TAUTULLI_WATCHED_PERCENT_THRESHOLD = 80  # Minimum percent watched to qualify as "watched"

# Rate limiting
RATE_LIMIT_TEST_EMAIL = "5 per hour"
RATE_LIMIT_MANUAL_CHECK = "3 per hour"
