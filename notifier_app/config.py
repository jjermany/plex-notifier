from flask_sqlalchemy import SQLAlchemy

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
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, nullable=False, unique=False)
    global_opt_out = db.Column(db.Boolean, default=False)
    show_key = db.Column(db.String, nullable=True)  # grandparentRatingKey
    show_opt_out = db.Column(db.Boolean, default=True)  # opt out of this show


class NotificationHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, nullable=False)
    details = db.Column(db.Text, nullable=False)
    sent_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())

