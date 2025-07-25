# notifier.py
import os
import smtplib
import requests
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from typing import List, Dict, Any
from logging.handlers import RotatingFileHandler

from flask import current_app, Flask
from apscheduler.schedulers.background import BackgroundScheduler
from jinja2 import Environment, FileSystemLoader, select_autoescape
from plexapi.server import PlexServer
from plexapi.video import Episode
from apscheduler.schedulers.base import BaseScheduler
from .config import Settings

logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

# Setup notification logger
notif_logger = logging.getLogger("notifications")
notif_logger.setLevel(logging.INFO)
notif_log_path = os.path.join(os.path.dirname(__file__), "notifications.log")
notif_handler = RotatingFileHandler(notif_log_path, maxBytes=100_000, backupCount=0)
notif_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
notif_logger.addHandler(notif_handler)

def start_scheduler(app, interval) -> BackgroundScheduler:
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

    # 🔧 Register scheduler in app.extensions for access in check_new_episodes
    if not hasattr(app, 'extensions'):
        app.extensions = {}
    app.extensions['apscheduler'] = sched

    return sched

def check_new_episodes(app, override_interval_minutes: int = None) -> None:
    with app.app_context():
        current_app.logger.info("🕒 Running check_new_episodes job")
        s = Settings.query.first()
        if not s:
            current_app.logger.warning("⚠️ No settings found; skipping.")
            return

        interval = override_interval_minutes or s.notify_interval or 30
        cutoff_dt = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(minutes=interval)

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            tv = plex.library.section('TV Shows')
            all_eps = tv.search(libtype='episode')

            recent_eps = [
                ep for ep in all_eps
                if isinstance(ep, Episode) and ep.addedAt and ep.addedAt.astimezone(timezone.utc) >= cutoff_dt
            ]
            local_time = cutoff_dt.astimezone()
            current_app.logger.info(f"📺 Filtered {len(recent_eps)} recent episodes since {local_time.isoformat()}")

        except Exception as e:
            current_app.logger.error(f"Error connecting to Plex: {e}")
            return

        if not recent_eps:
            current_app.logger.info("⚠️ No recent episodes found.")
            return

        users = _get_users(s)
        if not users:
            current_app.logger.info("⚠️ No users fetched.")
            return

        user_eps: Dict[str, List[Episode]] = {}

        for user in users:
            uid = user.get('user_id')
            user_email = user.get('email')

            if not user_email or user_email == s.from_address:
                continue

            watchable: List[Episode] = []

            current_app.logger.debug(f"🔍 Checking user {user_email} (user_id={uid})")

            for ep in recent_eps:
                show_key = ep.grandparentRatingKey
                if not show_key:
                    current_app.logger.debug(f"⏩ Skipping {ep.title} — missing grandparentRatingKey")
                    continue
                if not _user_has_watched_show(s, uid, show_key):
                    current_app.logger.debug(f"⏩ Skipping {ep.title} — user hasn’t watched show (≥90%)")
                    continue
                if _user_has_history(s, uid, ep.ratingKey):
                    current_app.logger.debug(f"⏩ Skipping {ep.title} — already watched by user")
                    continue
                watchable.append(ep)

            if watchable:
                user_eps[user_email] = watchable
                current_app.logger.info(f"User {user_email} will receive {len(watchable)} episodes")

        if not user_eps:
            current_app.logger.info("⚠️ No users with watchable episodes.")
            return

        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(loader=FileSystemLoader(tmpl_dir), autoescape=select_autoescape(['html']))
        template = env.get_template('jinja2.html')

        fallback_url = "https://raw.githubusercontent.com/jjermany/plex-notifier/main/media/no-poster-dark.jpg"

        for email, eps in user_eps.items():
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"{len(eps)} New Episode{'s' if len(eps) != 1 else ''} Available"
            msg['From'] = s.from_address
            msg['To'] = email

            images_attached = {}
            grouped = {}

            for idx, ep in enumerate(eps, start=1):
                show_key = ep.grandparentTitle
                if show_key not in grouped:
                    grouped[show_key] = []

                # Handle show poster (once per group)
                if show_key not in images_attached:
                    show_poster_url = f"{s.plex_url.rstrip('/')}{ep.grandparentThumb}?X-Plex-Token={s.plex_token}" if ep.grandparentThumb else fallback_url
                    try:
                        show_img = requests.get(show_poster_url, timeout=10)
                        show_img.raise_for_status()
                        cid_show = f"show_{idx}"
                        img = MIMEImage(show_img.content)
                        img.add_header("Content-ID", f"<{cid_show}>")
                        img.add_header("Content-Disposition", "inline", filename=f"{cid_show}.jpg")
                        msg.attach(img)
                        images_attached[show_key] = f"cid:{cid_show}"
                    except Exception as e:
                        current_app.logger.warning(f"Show poster failed: {e}")
                        images_attached[show_key] = fallback_url

                # Handle episode poster
                episode_url = f"{s.plex_url.rstrip('/')}{ep.thumb}?X-Plex-Token={s.plex_token}" if ep.thumb else fallback_url
                try:
                    episode_img = requests.get(episode_url, timeout=10)
                    episode_img.raise_for_status()
                    cid_ep = f"ep_{idx}"
                    img = MIMEImage(episode_img.content)
                    img.add_header("Content-ID", f"<{cid_ep}>")
                    img.add_header("Content-Disposition", "inline", filename=f"{cid_ep}.jpg")
                    msg.attach(img)
                    episode_ref = f"cid:{cid_ep}"
                except Exception as e:
                    current_app.logger.warning(f"Episode poster failed: {e}")
                    episode_ref = fallback_url

                grouped[show_key].append({
                    'show_title': ep.grandparentTitle,
                    'season': ep.parentIndex,
                    'episode': ep.index,
                    'ep_title': ep.title,
                    'synopsis': ep.summary or 'No synopsis available.',
                    'episode_poster_ref': episode_ref,
                    'show_poster_ref': images_attached[show_key],
                })

            html_body = template.render(grouped_episodes=grouped)
            plain_body = "\n".join([
                f"{e['show_title']} S{e['season']:02}E{e['episode']:02} - {e['ep_title']}"
                for group in grouped.values() for e in group
            ])

            msg.attach(MIMEText(plain_body, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            _send_email(s, msg)
            current_app.logger.info(f"✅ Email sent to {email} with {len(eps)} episodes")
            notif_logger.info(
                f"Sent to {email} | Episodes: {', '.join(f'{e.grandparentTitle} S{e.parentIndex}E{e.index}' for e in eps)}"
            )
        current_app.logger.info("✅ check_new_episodes job completed.")
        scheduler: BaseScheduler = current_app.extensions.get('apscheduler')
        if scheduler:
            job = scheduler.get_job('check_job')
            if job and job.next_run_time:
                current_app.logger.info(f"⏭️ Next scheduled run at {job.next_run_time.isoformat()}")
            else:
                current_app.logger.warning("⚠️ Could not retrieve next_run_time from scheduler.")

def _get_users(s: Settings) -> List[Dict[str, Any]]:
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
                {
                    'user_id': u.get('user_id'),
                    'username': u.get('username'),
                    'email': u.get('email') or s.from_address
                }
                for u in data
            ]
        except Exception as e:
            current_app.logger.error(f"Error fetching users from Tautulli: {e}")
    return []

def _user_has_history(s: Settings, user_id: int, rating_key: Any) -> bool:
    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        resp = requests.get(
            base,
            params={
                'apikey': s.tautulli_api_key,
                'cmd': 'get_history',
                'user_id': user_id,
                'rating_key': rating_key,
                'length': 100
            },
            timeout=10
        )
        resp.raise_for_status()
        history = resp.json().get('response', {}).get('data', {}).get('data', [])
        return len(history) > 0
    except Exception as e:
        current_app.logger.error(f"Error querying Tautulli history for user {user_id}: {e}")
        return False

def _user_has_watched_show(s: Settings, user_id: int, grandparent_rating_key: Any) -> bool:
    """Return True if user has watched at least 90% of any episode in the show."""
    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        resp = requests.get(
            base,
            params={
                'apikey': s.tautulli_api_key,
                'cmd': 'get_history',
                'user_id': user_id,
                'grandparent_rating_key': grandparent_rating_key,
                'length': 100
            },
            timeout=10
        )
        resp.raise_for_status()
        history = resp.json().get('response', {}).get('data', {}).get('data', [])

        for item in history:
            duration = item.get('duration', 0)
            watched_status = item.get('watched_status')
            watched_duration = item.get('watched', 0)

            if watched_status and duration > 0 and watched_duration / duration >= 0.9:
                return True

        return False
    except Exception as e:
        current_app.logger.error(f"Error checking show history for user {user_id}: {e}")
        return False

def _send_email(s: Settings, msg: MIMEMultipart) -> None:
    try:
        smtp = smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=30)
        smtp.starttls()
        smtp.login(s.smtp_user, s.smtp_pass)
        smtp.send_message(msg)
        smtp.quit()
    except Exception as e:
        current_app.logger.error(f"Error sending email to {msg['To']}: {e}")

def register_debug_route(app: Flask):
    @app.route('/force-run')
    def force_run():
        check_new_episodes(app)
        return "Manual notification job complete"
