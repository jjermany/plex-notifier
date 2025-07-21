# notifier.py (fixed version)

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
from flask import current_app, Flask
from typing import List, Dict, Any
from .config import Settings
import logging

logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

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
    return sched

def check_new_episodes(app) -> None:
    with app.app_context():
        current_app.logger.info("🕒 Running check_new_episodes job")
        s = Settings.query.first()
        if not s:
            current_app.logger.warning("⚠️ No settings found; skipping.")
            return

        cutoff_dt = datetime.utcnow() - timedelta(minutes=s.notify_interval or 30)

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            tv = plex.library.section('TV Shows')
            all_eps = tv.search(libtype='episode', sort='addedAt:desc')
            recent_eps = [
                ep for ep in all_eps
                if isinstance(ep, Episode) and ep.addedAt and ep.addedAt >= cutoff_dt
            ][:50]
            current_app.logger.info(f"Found {len(recent_eps)} recent episodes since {cutoff_dt}")
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
            email = user_email or s.from_address
            watchable: List[Episode] = []

            for ep in recent_eps:
                show_key = ep.grandparentRatingKey
                if not show_key:
                    continue

                # Check if user has watched any episode from this show
                if not _user_has_watched_show(s, uid, show_key):
                    continue

                # Skip if user already watched this episode
                if _user_has_history(s, uid, ep.ratingKey):
                    continue

                watchable.append(ep)

            if watchable:
                user_eps[email] = watchable
                current_app.logger.info(f"User {user_email} will receive {len(watchable)} new episodes")

        if not user_eps:
            current_app.logger.info("⚠️ No users with watchable episodes.")
            return

        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(
            loader=FileSystemLoader(tmpl_dir),
            autoescape=select_autoescape(['html'])
        )
        template = env.get_template('jinja2.html')

        for email, eps in user_eps.items():
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"{len(eps)} New Episode{'s' if len(eps) != 1 else ''} Available"
            msg['From'] = s.from_address
            msg['To'] = email

            episodes_ctx = [{
                'cid': f"poster{idx}",
                'show_title': ep.grandparentTitle,
                'season': ep.parentIndex,
                'episode': ep.index,
                'ep_title': ep.title,
                'synopsis': ep.summary or 'No synopsis available.'
            } for idx, ep in enumerate(eps, start=1)]

            html_body = template.render(episodes=episodes_ctx)
            plain_body = "\n".join([
                f"{e['show_title']} S{e['season']:02}E{e['episode']:02} - {e['ep_title']}"
                for e in episodes_ctx
            ])

            msg.attach(MIMEText(plain_body, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            _send_email(s, msg)
            current_app.logger.info(f"✅ Email sent to {email} with {len(eps)} episodes")

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
    return [{'user_id': None, 'username': 'Admin', 'email': s.from_address}]

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

def _user_has_watched_show(s: Settings, user_id: int, grandparent_rating_key: Any) -> bool:
    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        resp = requests.get(
            base,
            params={
                'apikey': s.tautulli_api_key,
                'cmd': 'get_history',
                'user_id': user_id,
                'grandparent_rating_key': grandparent_rating_key,
                'length': 1
            },
            timeout=10
        )
        resp.raise_for_status()
        history = resp.json().get('response', {}).get('data', [])
        return len(history) > 0
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
