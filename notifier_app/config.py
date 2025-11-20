from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timezone

db = SQLAlchemy()

class Settings(db.Model):
    id                = db.Column(db.Integer, primary_key=True)
    plex_url          = db.Column(db.String,  nullable=False)
    plex_token        = db.Column(db.String,  nullable=False)
    tautulli_url      = db.Column(db.String)
    tautulli_api_key  = db.Column(db.String)
    smtp_host         = db.Column(db.String)
    smtp_port         = db.Column(db.Integer)
    smtp_user         = db.Column(db.String)
    smtp_pass         = db.Column(db.String)
    from_address      = db.Column(db.String)
    notify_new_episodes = db.Column(db.Boolean, default=True)
    notify_interval   = db.Column(db.Integer, nullable=False, default=30)
    base_url          = db.Column(db.String)  # 👈 New line


class UserPreferences(db.Model):
    __tablename__ = 'user_preferences'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, nullable=False, index=True)
    global_opt_out = db.Column(db.Boolean, default=False)
    show_key = db.Column(db.String, nullable=True, index=True)  # grandparentRatingKey
    show_opt_out = db.Column(db.Boolean, default=True)  # opt out of this show

    # Composite unique constraint: one preference record per (email, show_key) combination
    __table_args__ = (
        db.UniqueConstraint('email', 'show_key', name='uq_email_show_key'),
        db.Index('idx_email_show_key', 'email', 'show_key'),
    )


class Notification(db.Model):
    __tablename__ = 'notifications'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, nullable=False, index=True)
    show_title = db.Column(db.String, nullable=False)
    show_key = db.Column(db.String, nullable=False, index=True)
    season = db.Column(db.Integer, nullable=False)
    episode = db.Column(db.Integer, nullable=False)
    episode_title = db.Column(db.String)
    episode_key = db.Column(db.String)
    timestamp = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), index=True)

    __table_args__ = (
        # Prevent duplicate notifications for same episode to same user
        db.UniqueConstraint('email', 'show_key', 'season', 'episode', name='uq_notification'),
        db.Index('idx_email_timestamp', 'email', 'timestamp'),
        db.Index('idx_show_key_season_episode', 'show_key', 'season', 'episode'),
    )


class HealthCheck(db.Model):
    __tablename__ = 'health_checks'

    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), index=True)
    status = db.Column(db.String, nullable=False)  # 'success', 'error', 'warning'
    episodes_found = db.Column(db.Integer, default=0)
    emails_sent = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text)
    duration_seconds = db.Column(db.Float)

