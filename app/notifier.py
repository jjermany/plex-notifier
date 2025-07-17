import os
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.background import BackgroundScheduler
from plexapi.server import PlexServer
from flask import current_app
from jinja2 import Environment, FileSystemLoader, select_autoescape
from .config import Settings, db

def start_scheduler(app):
    """Start APScheduler in the background when the app starts."""
    sched = BackgroundScheduler()
    # for testing, every minute; later switch to hours=1
    sched.add_job(func=lambda: check_new_episodes(app), trigger='interval', minutes=1)
    sched.start()
    # use the app that was passed in, not current_app
    app.logger.info("Scheduler started, will check for new episodes every minute.")

def check_new_episodes(app):
    """Job that runs periodically to find and notify about new episodes."""
    with app.app_context():
        s = Settings.query.first()
        if not s or not s.notify_new_episodes:
            current_app.logger.info("Notifications are disabled or settings not configured.")
            return

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            new_eps = plex.library.section('TV Shows').recentlyAdded(maxresults=10)
        except Exception as e:
            current_app.logger.error(f"Error connecting to Plex: {e}")
            return

        # Set up Jinja2 to load the email template
        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(
            loader=FileSystemLoader(tmpl_dir),
            autoescape=select_autoescape(['html','xml'])
        )
        template = env.get_template('email_template.html')

        # Fetch managed users from Plex
        users = plex.myPlexAccount().users()

        for ep in new_eps:
            show = ep.parent()
            for user in users:
                uid = user.id

                # 1) Has the user started this show?
                if not _user_has_history(s, uid, show.ratingKey):
                    continue
                # 2) Has the user already watched this episode?
                if _user_has_history(s, uid, ep.ratingKey):
                    continue

                # Render email
                poster_url = f"{s.plex_url}{ep.thumb}?X-Plex-Token={s.plex_token}"
                html = template.render(
                    show_title=show.title,
                    season=ep.parentIndex,
                    episode=ep.index,
                    ep_title=ep.title,
                    synopsis=ep.summary or "No synopsis available.",
                    poster_url=poster_url
                )
                subject = f"{show.title} S{ep.parentIndex:02}E{ep.index:02} Now Available!"
                _send_email(s, user.email, subject, html)
                current_app.logger.info(f"Sent notification to {user.email} for {show.title} S{ep.parentIndex}E{ep.index}.")

def _user_has_history(s: Settings, user_id: int, rating_key: str) -> bool:
    """Check Tautulli history for a given user & rating_key."""
    if not s.tautulli_url or not s.tautulli_api_key:
        return False
    resp = requests.get(
        f"{s.tautulli_url}/api/v2",
        params={
            "apikey": s.tautulli_api_key,
            "cmd": "get_history",
            "user_id": user_id,
            "rating_key": rating_key,
            "length": 1
        },
        timeout=10
    )
    data = resp.json().get('response', {}).get('data', [])
    return len(data) > 0

def _send_email(s: Settings, to: str, subject: str, html: str):
    """Send an HTML email via SMTP."""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = s.from_address
    msg['To'] = to
    msg.attach(MIMEText(html, 'html'))

    with smtplib.SMTP(s.smtp_host, s.smtp_port) as smtp:
        smtp.starttls()
        smtp.login(s.smtp_user, s.smtp_pass)
        smtp.send_message(msg)
