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
    notify_interval = db.Column(db.Integer, nullable=False, default=30)
