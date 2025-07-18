# app/notifier.py

import os
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.background import BackgroundScheduler
from plexapi.server import PlexServer
from plexapi.video import Episode
from jinja2 import Environment, FileSystemLoader, select_autoescape
from flask import current_app
from .config import Settings

def start_scheduler(app):
    """
    Start a background scheduler that polls Plex for new episodes every 30 minutes.
    """
    sched = BackgroundScheduler()
    sched.add_job(
        func=lambda: check_new_episodes(app),
        trigger='interval',
        minutes=30
    )
    sched.start()
    app.logger.info("Scheduler started, will check for new episodes every 30 minutes.")

def check_new_episodes(app):
    """
    Poll Plex for recently added TV episodes in the last 30 minutes,
    then notify relevant users via email.
    """
    with app.app_context():
        current_app.logger.info("🕒 Running check_new_episodes job")
        s = Settings.query.first()
        if not s:
            current_app.logger.info("Notifications disabled or settings not configured.")
            return

        # Connect to Plex
        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            raw = plex.library.section('TV Shows').recentlyAdded(maxresults=50)
        except Exception as e:
            current_app.logger.error(f"Error connecting to Plex: {e}")
            return

        # Filter for episodes added in the last 30 minutes
        cutoff = datetime.utcnow() - timedelta(minutes=30)
        episodes = [
            item for item in raw
            if isinstance(item, Episode)
            and datetime.utcfromtimestamp(item.addedAt) >= cutoff
        ]

        # Prepare the email template
        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(
            loader=FileSystemLoader(tmpl_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template('email_template.html')

        # Fetch your user list from Tautulli (or fallback)
        users = _get_users(s)

        for ep in episodes:
            try:
                season = ep.parent()
                show   = season.parent()
            except Exception as e:
                current_app.logger.error(f"Failed to derive show for {ep}: {e}")
                continue

            for user in users:
                uid          = user.get("user_id")
                notify_email = user.get("email") or s.from_address

                # Check per-user watch history if a Tautulli user
                if uid and not _user_has_history(s, uid, show.ratingKey):
                    continue
                if uid and _user_has_history(s, uid, ep.ratingKey):
                    continue

                # Render and send the email
                poster_url = f"{s.plex_url.rstrip('/')}{ep.thumb}?X-Plex-Token={s.plex_token}"
                html = template.render(
                    show_title=show.title,
                    season=ep.parentIndex,
                    episode=ep.index,
                    ep_title=ep.title,
                    synopsis=ep.summary or "No synopsis available.",
                    poster_url=poster_url
                )
                subject = f"{show.title} S{ep.parentIndex:02}E{ep.index:02} Now Available!"
                _send_email(s, notify_email, subject, html)
                current_app.logger.info(
                    f"Sent notification to {notify_email} for "
                    f"{show.title} S{ep.parentIndex}E{ep.index}."
                )

def _get_users(s: Settings) -> list[dict]:
    """
    Retrieve the list of users from Tautulli (user_id, username, email).
    Fallback to a single admin user if the call fails or isn't configured.
    """
    if s.tautulli_url and s.tautulli_api_key:
        try:
            base = f"{s.tautulli_url.rstrip('/')}/api/v2"
            resp = requests.get(
                base,
                params={"apikey": s.tautulli_api_key, "cmd": "get_users"},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get("response", {}).get("data", [])
            return [
                {
                    "user_id": u.get("user_id"),
                    "username": u.get("username"),
                    "email":    u.get("email") or s.from_address
                }
                for u in data
            ]
        except Exception as e:
            current_app.logger.error(f"Error fetching users from Tautulli: {e}")

    # Fallback to admin-only
    return [{"user_id": None, "username": "Admin", "email": s.from_address}]

def _user_has_history(s: Settings, user_id: int, rating_key: str) -> bool:
    """
    Check via Tautulli whether the given user has any history
    (started or watched) for the given rating_key.
    """
    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        resp = requests.get(
            base,
            params={
                "apikey":     s.tautulli_api_key,
                "cmd":        "get_history",
                "user_id":    user_id,
                "rating_key": rating_key,
                "length":     1
            },
            timeout=10
        )
        resp.raise_for_status()
        history = resp.json().get("response", {}).get("data", [])
        return len(history) > 0
    except Exception as e:
        current_app.logger.error(f"Error querying Tautulli history for user {user_id}: {e}")
        return False

def _send_email(s: Settings, to: str, subject: str, html: str):
    """
    Send a single HTML email via SMTP using the settings provided.
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = s.from_address
    msg['To']      = to
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP(s.smtp_host, s.smtp_port) as smtp:
            smtp.starttls()
            smtp.login(s.smtp_user, s.smtp_pass)
            smtp.send_message(msg)
    except Exception as e:
        current_app.logger.error(f"Error sending email to {to}: {e}")
