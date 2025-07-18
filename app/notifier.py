import os
import smtplib
import requests
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.background import BackgroundScheduler
from plexapi.server import PlexServer
from plexapi.video import Episode
from jinja2 import Environment, FileSystemLoader, select_autoescape
from flask import current_app
from .config import Settings

# Determine timezone for timestamp comparisons
tz_name = os.getenv('TZ', 'UTC')
try:
    from zoneinfo import ZoneInfo
    local_tz = ZoneInfo(tz_name)
except Exception:
    local_tz = None
    current_app = None  # placeholder if logging unavailable


def start_scheduler(app, interval):
    """
    Start a BackgroundScheduler that runs check_new_episodes every `interval` minutes.
    Returns the scheduler instance for later rescheduling.
    """
    sched = BackgroundScheduler()
    sched.add_job(
        func=lambda: check_new_episodes(app),
        trigger='interval',
        minutes=interval,
        id='check_job',
        replace_existing=True
    )
    sched.start()
    app.logger.info(f"Scheduler started, interval={interval}min")
    return sched


def check_new_episodes(app):
    """
    Poll Plex for recently added TV episodes in the last `interval` minutes,
    then notify relevant users via email.
    """
    with app.app_context():
        current_app.logger.info("🕒 Running check_new_episodes job")
        s = Settings.query.first()
        if not s:
            current_app.logger.warning("Settings not configured; skipping new-episode check.")
            return

        # Determine cutoff epoch timestamp based on configured interval and TZ
        interval_secs = (s.notify_interval or 30) * 60
        if local_tz:
            now_local = datetime.now(local_tz)
            cutoff_dt = now_local - timedelta(seconds=interval_secs)
            cutoff_ts = cutoff_dt.timestamp()
            current_app.logger.info(
                f"Using timezone '{tz_name}': now={now_local.isoformat()} cutoff={cutoff_dt.isoformat()}"
            )
        else:
            cutoff_ts = time.time() - interval_secs
            current_app.logger.info(f"Using UTC cutoff_ts={cutoff_ts}")

        # Connect to Plex
        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            raw_items = plex.library.section('TV Shows').recentlyAdded(maxresults=50)
        except Exception as e:
            current_app.logger.error(f"Error connecting to Plex: {e}")
            return

        # Debug log raw items
        current_app.logger.info(f"Found {len(raw_items)} raw items from Plex")
        for item in raw_items:
            if isinstance(item, Episode):
                dt = datetime.fromtimestamp(item.addedAt, local_tz) if local_tz else datetime.utcfromtimestamp(item.addedAt)
                current_app.logger.debug(
                    f"  -> ratingKey={item.ratingKey} addedAt={item.addedAt} ({dt.isoformat()})"
                )

        # Filter for truly new episodes by comparing raw epoch values
        episodes = [
            item for item in raw_items
            if isinstance(item, Episode)
            and item.addedAt >= cutoff_ts
        ]
        if not episodes:
            current_app.logger.info("No new episodes found in this interval.")
            return

        # Load email template
        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(
            loader=FileSystemLoader(tmpl_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template('email_template.html')

        # Get users from Tautulli (or fallback)
        users = _get_users(s)

        for ep in episodes:
            try:
                season = ep.parent()
                show = season.parent()
            except Exception as e:
                current_app.logger.error(f"Failed to derive show for {ep}: {e}")
                continue

            for user in users:
                uid = user.get('user_id')
                email = user.get('email') or s.from_address

                # skip if user has history for this show or this episode
                if uid and not _user_has_history(s, uid, show.ratingKey):
                    continue
                if uid and _user_has_history(s, uid, ep.ratingKey):
                    continue

                # build and send email
                poster_url = f"{s.plex_url.rstrip('/')}{ep.thumb}?X-Plex-Token={s.plex_token}"
                html = template.render(
                    show_title=show.title,
                    season=ep.parentIndex,
                    episode=ep.index,
                    ep_title=ep.title,
                    synopsis=ep.summary or 'No synopsis available.',
                    poster_url=poster_url
                )
                subject = f"{show.title} S{ep.parentIndex:02}E{ep.index:02} Now Available!"
                _send_email(s, email, subject, html)
                current_app.logger.info(
                    f"Sent notification to {email} for {show.title} S{ep.parentIndex}E{ep.index}"
                )


def _get_users(s: Settings) -> list[dict]:
    """
    Retrieve users from Tautulli (user_id, username, email).
    Fall back to a single admin user if unavailable.
    """
    if s.tautulli_url and s.tautulli_api_key:
        try:
            base = f"{s.tautulli_url.rstrip('/')}/api/v2"
            resp = requests.get(
                base,
                params={'apikey': s.tautulli_api_key, 'cmd': 'get_users'},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get('response', {}).get('data', [])
            return [
                {'user_id': u.get('user_id'), 'username': u.get('username'), 'email': u.get('email') or s.from_address}
                for u in data
            ]
        except Exception as e:
            current_app.logger.error(f"Error fetching users from Tautulli: {e}")
    return [{'user_id': None, 'username': 'Admin', 'email': s.from_address}]


def _user_has_history(s: Settings, user_id: int, rating_key: str) -> bool:
    """
    Check via Tautulli if a user has any history for a given rating_key.
    """
    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        resp = requests.get(
            base,
            params={
                'apikey': s.tautulli_api_key,
                'cmd': 'get_history',
                'user_id': user_id,
                'rating_key': rating_key,
                'length': 1
            },
            timeout=10
        )
        resp.raise_for_status()
        history = resp.json().get('response', {}).get('data', [])
        return len(history) > 0
    except Exception as e:
        current_app.logger.error(f"Error querying Tautulli history for user {user_id}: {e}")
        return False


def _send_email(s: Settings, to: str, subject: str, html: str):
    """
    Send an HTML email via SMTP.
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = s.from_address
    msg['To'] = to
    msg.attach(MIMEText(html, 'html'))

    try:
        smtp = smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=30)
        smtp.starttls()
        smtp.login(s.smtp_user, s.smtp_pass)
        smtp.send_message(msg)
        smtp.quit()
    except Exception as e:
        current_app.logger.error(f"Error sending email to {to}: {e}")
