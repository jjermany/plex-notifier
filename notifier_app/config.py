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
    show_guid = db.Column(db.String, nullable=True, index=True)  # stable show identifier
    show_opt_out = db.Column(db.Boolean, default=False)  # opt out of this show

    # Composite unique constraint: one preference record per (email, show_key) combination
    __table_args__ = (
        db.UniqueConstraint('email', 'show_key', name='uq_email_show_key'),
        db.Index('idx_email_show_key', 'email', 'show_key'),
        db.Index('idx_email_show_guid', 'email', 'show_guid'),
    )


class Notification(db.Model):
    __tablename__ = 'notifications'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, nullable=False, index=True)
    show_title = db.Column(db.String, nullable=False)
    show_key = db.Column(db.String, nullable=False, index=True)
    show_guid = db.Column(db.String, nullable=True, index=True)
    tvdb_id = db.Column(db.String, nullable=True)
    tmdb_id = db.Column(db.String, nullable=True)
    imdb_id = db.Column(db.String, nullable=True)
    plex_guid = db.Column(db.String, nullable=True)
    season = db.Column(db.Integer, nullable=False)
    episode = db.Column(db.Integer, nullable=False)
    episode_title = db.Column(db.String)
    episode_key = db.Column(db.String)
    timestamp = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), index=True)

    __table_args__ = (
        # Prevent duplicate notifications for same episode to same user
        db.UniqueConstraint('email', 'show_guid', 'season', 'episode', name='uq_notification_show_guid'),
        db.UniqueConstraint('email', 'tvdb_id', 'season', 'episode', name='uq_notification_tvdb_id'),
        db.UniqueConstraint('email', 'tmdb_id', 'season', 'episode', name='uq_notification_tmdb_id'),
        db.UniqueConstraint('email', 'imdb_id', 'season', 'episode', name='uq_notification_imdb_id'),
        db.UniqueConstraint('email', 'plex_guid', 'season', 'episode', name='uq_notification_plex_guid'),
        db.Index('idx_email_timestamp', 'email', 'timestamp'),
        db.Index('idx_show_key_season_episode', 'show_key', 'season', 'episode'),
        db.Index('idx_show_guid', 'show_guid'),
        db.Index('idx_notification_tvdb_id', 'tvdb_id'),
        db.Index('idx_notification_tmdb_id', 'tmdb_id'),
        db.Index('idx_notification_imdb_id', 'imdb_id'),
        db.Index('idx_notification_plex_guid', 'plex_guid'),
    )


class ShowIdentity(db.Model):
    __tablename__ = 'show_identities'

    id = db.Column(db.Integer, primary_key=True)
    show_guid = db.Column(db.String, nullable=True, index=True)
    show_key = db.Column(db.String, nullable=True, index=True)
    tvdb_id = db.Column(db.String)
    tmdb_id = db.Column(db.String)
    imdb_id = db.Column(db.String)
    plex_guid = db.Column(db.String)
    plex_rating_key = db.Column(db.String)
    title = db.Column(db.String)
    year = db.Column(db.Integer)
    fingerprint = db.Column(db.String, index=True)

    __table_args__ = (
        db.Index('idx_show_guid_key', 'show_guid', 'show_key'),
    )


class EpisodeFirstSeen(db.Model):
    __tablename__ = 'episode_first_seen'

    episode_key = db.Column(db.String, primary_key=True)
    first_seen_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), index=True)


class HealthCheck(db.Model):
    __tablename__ = 'health_checks'

    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), index=True)
    status = db.Column(db.String, nullable=False)  # 'success', 'error', 'warning'
    episodes_found = db.Column(db.Integer, default=0)
    emails_sent = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text)
    duration_seconds = db.Column(db.Float)
