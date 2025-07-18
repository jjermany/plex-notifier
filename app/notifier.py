import os
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from apscheduler.schedulers.background import BackgroundScheduler
from plexapi.server import PlexServer
from plexapi.video import Episode
from jinja2 import Environment, FileSystemLoader, select_autoescape
from flask import current_app
from typing import List, Dict, Any
from .config import Settings

def start_scheduler(app, interval) -> BackgroundScheduler:
    """
    Start a BackgroundScheduler that runs check_new_episodes every `interval` minutes.
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


def check_new_episodes(app) -> None:
    """
    Poll Plex for recently added TV episodes in the last `notify_interval` minutes,
    then send each user one grouped email of all new episodes they qualify for.
    """
    with app.app_context():
        current_app.logger.info("🕒 Running check_new_episodes job")
        s = Settings.query.first()
        if not s:
            current_app.logger.warning("Settings not configured; skipping new-episode check.")
            return

        # Compute cutoff datetime threshold (UTC)
        cutoff_dt = datetime.utcnow() - timedelta(minutes=s.notify_interval or 30)
        current_app.logger.info(f"Using cutoff_dt={cutoff_dt} (UTC datetime)")

        # Fetch the most recently added 50 episodes
        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            tv = plex.library.section('TV Shows')
            all_eps = tv.search(libtype='episode', sort='addedAt:desc')
            recent_eps = [
                ep for ep in all_eps
                if isinstance(ep, Episode) and ep.addedAt and ep.addedAt >= cutoff_dt
            ][:50]
            current_app.logger.info(f"Fetched {len(recent_eps)} new episode items")
        except Exception as e:
            current_app.logger.error(f"Error connecting to Plex or fetching episodes: {e}")
            return

        if not recent_eps:
            current_app.logger.info("No new episodes found in this interval.")
            return

        # Build per-user episode lists
        users = _get_users(s)
        user_eps: Dict[str, List[Episode]] = {}
        for user in users:
            uid = user.get('user_id')
            email = user.get('email') or s.from_address
            episodes_for_user: List[Episode] = []
            for ep in recent_eps:
                show_key = ep.grandparentRatingKey
                if not show_key:
                    continue

                # only notify if user has watched the show
                if uid and not _user_has_history(s, uid, show_key):
                    continue
                # skip if they have already watched this episode
                if uid and _user_has_history(s, uid, ep.ratingKey):
                    continue

                episodes_for_user.append(ep)

            if episodes_for_user:
                user_eps[email] = episodes_for_user

        if not user_eps:
            current_app.logger.info("No users to notify.")
            return

        # Prepare Jinja environment
        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(
            loader=FileSystemLoader(tmpl_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template('jinja2.html')

        # Send one email per user
        for email, eps in user_eps.items():
            msg = MIMEMultipart('related')
            msg['Subject'] = f"{len(eps)} New Episode{'s' if len(eps) != 1 else ''} Available"
            msg['From'] = s.from_address
            msg['To'] = email

            # render html and plain text bodies
            episodes_ctx = []
            for idx, ep in enumerate(eps, start=1):
                episodes_ctx.append({
                    'cid':        f"poster{idx}",
                    'show_title': ep.grandparentTitle,
                    'season':     ep.parentIndex,
                    'episode':    ep.index,
                    'ep_title':   ep.title,
                    'synopsis':   ep.summary or 'No synopsis available.'
                })

            html_body = template.render(episodes=episodes_ctx)
            plain_body = "New episodes available:\n" + "\n".join(
                f"{e['show_title']} S{e['season']:02}E{e['episode']:02} - {e['ep_title']}" for e in episodes_ctx
            )

            # attach multipart/alternative
            alt = MIMEMultipart('alternative')
            alt.attach(MIMEText(plain_body, 'plain'))
            alt.attach(MIMEText(html_body, 'html'))
            msg.attach(alt)

            # inline attach each poster
            for idx, ep in enumerate(eps, start=1):
                try:
                    url = f"{s.plex_url.rstrip('/')}{ep.thumb}?X-Plex-Token={s.plex_token}"
                    img_data = requests.get(url, timeout=10).content
                    img = MIMEImage(img_data)
                    img.add_header('Content-ID', f"<poster{idx}>")
                    msg.attach(img)
                except Exception as e:
                    current_app.logger.error(f"Failed to fetch image for {ep.title}: {e}")

            _send_email(s, msg)
            current_app.logger.info(f"Sent summary email to {email} with {len(eps)} episodes")


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
                    'email':    u.get('email') or s.from_address
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
                'apikey':     s.tautulli_api_key,
                'cmd':        'get_history',
                'user_id':    user_id,
                'rating_key': rating_key,
                'length':     1
            },
            timeout=10
        )
        resp.raise_for_status()
        history = resp.json().get('response', {}).get('data', [])
        return len(history) > 0
    except Exception as e:
        current_app.logger.error(f"Error querying Tautulli history for user {user_id}: {e}")
        return False


def _send_email(s: Settings, msg: MIMEMultipart) -> None:
    """
    Send the prepared email message via SMTP.
    """
    try:
        smtp = smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=30)
        smtp.starttls()
        smtp.login(s.smtp_user, s.smtp_pass)
        smtp.send_message(msg)
        smtp.quit()
    except Exception as e:
        current_app.logger.error(f"Error sending email to {msg['To']}: {e}")
