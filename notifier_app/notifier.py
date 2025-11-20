import os
import smtplib
import requests
import logging
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from typing import List, Dict, Any, Set, Optional, Tuple
from collections import deque
from logging.handlers import RotatingFileHandler
from functools import lru_cache
from cachetools import TTLCache

from .logging_utils import TZFormatter
from .utils import normalize_email, email_to_filename
from .constants import (
    NOTIFICATION_HISTORY_LIMIT,
    NOTIFICATION_CACHE_TTL_SECONDS,
    EMAIL_RETRY_ATTEMPTS,
    EMAIL_RETRY_MIN_WAIT_SECONDS,
    EMAIL_RETRY_MAX_WAIT_SECONDS,
    USER_LOG_MAX_BYTES,
    GLOBAL_LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
    TAUTULLI_MAX_PAGE_LENGTH,
    API_RETRY_ATTEMPTS,
    API_RETRY_MIN_WAIT_SECONDS,
    API_RETRY_MAX_WAIT_SECONDS,
)

from flask import current_app, Flask
from apscheduler.schedulers.background import BackgroundScheduler
from jinja2 import Environment, FileSystemLoader, select_autoescape
from plexapi.server import PlexServer
from plexapi.video import Episode
from apscheduler.schedulers.base import BaseScheduler
from itsdangerous import URLSafeTimedSerializer

from .config import Settings, UserPreferences, Notification, db

# Logging
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
notif_logger = logging.getLogger("notifications")
notif_logger.setLevel(logging.INFO)
notif_log_dir = os.path.join(os.path.dirname(__file__), "../instance/logs")
os.makedirs(notif_log_dir, exist_ok=True)
notif_log_path = os.path.join(notif_log_dir, "notifications.log")
notif_handler = RotatingFileHandler(notif_log_path, maxBytes=GLOBAL_LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
notif_handler.setFormatter(TZFormatter('%(asctime)s | %(message)s'))
notif_logger.addHandler(notif_handler)
notif_logger.propagate = False  # ✅ Prevent log from appearing in Unraid console

# Common affirmative values returned by Tautulli for watched history entries
AFFIRMATIVE_WATCHED_STATUSES: Set[str] = {
    "watched",
    "played",
    "complete",
    "completed",
    "finished",
    "viewed",
    "yes",
    "true",
    "fully_watched",
    "fully watched",
}

# Token serializer - validated at startup
secret_key = os.environ.get("SECRET_KEY", "change-me")
if secret_key == "change-me":
    raise ValueError("SECRET_KEY must be set to a secure value, not 'change-me'")
serializer = URLSafeTimedSerializer(secret_key)

# In-memory cache for notification history (TTL cache with automatic expiry)
# Key: email, Value: Set of notification identifiers
notification_cache = TTLCache(maxsize=1000, ttl=NOTIFICATION_CACHE_TTL_SECONDS)


def _get_recent_notifications(email: str, limit: int = NOTIFICATION_HISTORY_LIMIT) -> Set[str]:
    """Get recent notifications for a user, using cache when available."""
    normalized_email = normalize_email(email)

    # Check cache first
    if normalized_email in notification_cache:
        return notification_cache[normalized_email].copy()

    notified: Set[str] = set()

    # Try database first (preferred method)
    try:
        recent_notifications = (
            Notification.query
            .filter_by(email=normalized_email)
            .order_by(Notification.timestamp.desc())
            .limit(limit)
            .all()
        )
        for notif in recent_notifications:
            notified.add(f"{notif.show_key}|S{notif.season}E{notif.episode}")
    except Exception as e:
        current_app.logger.warning(f"Could not query database for notifications: {e}")

    # Fallback to log file if database is empty (for backward compatibility)
    if not notified:
        filename = email_to_filename(email)
        log_dir = os.path.join(os.path.dirname(__file__), "../instance/logs")
        log_path = os.path.join(log_dir, f"{filename}-notification.log")
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    lines = deque(f, maxlen=limit)
                for line in lines:
                    line = line.strip()
                    if "Notified:" not in line:
                        continue
                    try:
                        _, body = line.split("Notified:", 1)
                        body = body.strip()
                        key_part = body.split("[Key:", 1)[1]
                        show_key, rest = key_part.split("]", 1)
                        season_ep = rest.strip().split(" - ", 1)[0]
                        notified.add(f"{show_key}|{season_ep}")
                    except Exception:
                        continue
            except Exception:
                pass

    # Cache the result
    notification_cache[normalized_email] = notified.copy()
    return notified


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

    # Log the first scheduled run time
    job = sched.get_job('check_job')
    if job and job.next_run_time:
        app.logger.info(
            f"⏭️ First scheduled run at {job.next_run_time.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
    else:
        app.logger.warning("⚠️ Could not determine first scheduled run time.")


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

        machine_id = None

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            machine_id = plex.machineIdentifier
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

            # Validate email has domain
            if '@' not in user_email:
                current_app.logger.warning(f"⚠️ Skipping user with incomplete email: {user_email}")
                continue

            canon = normalize_email(user_email)

            # 🔒 Check global opt-out
            pref = UserPreferences.query.filter_by(email=canon, show_key=None).first()
            if not pref:
                pref = UserPreferences.query.filter_by(email=user_email, show_key=None).first()
                if pref and pref.email != canon:
                    pref.email = canon
                    db.session.commit()
            if pref and pref.global_opt_out:
                continue

            watchable: List[Episode] = []
            recent_notified = _get_recent_notifications(canon)

            for ep in recent_eps:
                show_key = ep.grandparentRatingKey
                if not show_key:
                    continue

                # 🔒 Check per-show opt-out
                show_pref = UserPreferences.query.filter_by(email=canon, show_key=str(show_key)).first()
                if not show_pref:
                    show_pref = UserPreferences.query.filter_by(email=user_email, show_key=str(show_key)).first()
                    if show_pref and show_pref.email != canon:
                        show_pref.email = canon
                        db.session.commit()
                if show_pref and show_pref.show_opt_out:
                    continue

                if not _user_has_watched_show(s, uid, show_key):
                    continue
                if _user_has_history(s, uid, ep.ratingKey):
                    continue

                ep_id = f"{show_key}|S{ep.parentIndex}E{ep.index}"
                if ep_id in recent_notified:
                    continue

                watchable.append(ep)

            if watchable:
                user_eps[user_email] = watchable

        if not user_eps:
            current_app.logger.info("⚠️ No users with watchable episodes.")
            scheduler: BaseScheduler = current_app.extensions.get('apscheduler')
            if scheduler:
                job = scheduler.get_job('check_job')
                if job and job.next_run_time:
                    current_app.logger.info(
                        f"⏭️ Next scheduled run at {job.next_run_time.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    )

            return

        tmpl_dir = os.path.join(app.root_path, 'templates')
        env = Environment(loader=FileSystemLoader(tmpl_dir), autoescape=select_autoescape(['html']))
        template = env.get_template('jinja2.html')

        fallback_url = "https://raw.githubusercontent.com/jjermany/plex-notifier/main/media/no-poster-dark.jpg"

        plex_app_base = None
        if machine_id:
            plex_app_base = f"https://app.plex.tv/desktop#!/server/{machine_id}/details?key="

        for email, eps in user_eps.items():
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"{len(eps)} New Episode{'s' if len(eps) != 1 else ''} Available"
            msg['From'] = s.from_address
            msg['To'] = email

            images_attached = {}
            grouped = {}

            for idx, ep in enumerate(eps, start=1):
                show_title = ep.grandparentTitle
                show_link = None
                show_key = ep.grandparentRatingKey
                if plex_app_base and show_key:
                    show_link = f"{plex_app_base}{quote('/library/metadata/' + str(show_key))}"

                if show_title not in grouped:
                    grouped[show_title] = {
                        'show_title': show_title,
                        'show_poster_ref': fallback_url,
                        'show_link': show_link,
                        'episodes': [],
                    }
                elif not grouped[show_title]['show_link'] and show_link:
                    grouped[show_title]['show_link'] = show_link

                if show_title not in images_attached:
                    show_poster_url = f"{s.plex_url.rstrip('/')}{ep.grandparentThumb}?X-Plex-Token={s.plex_token}" if ep.grandparentThumb else fallback_url
                    try:
                        show_img = requests.get(show_poster_url, timeout=10)
                        show_img.raise_for_status()
                        cid_show = f"show_{idx}"
                        img = MIMEImage(show_img.content)
                        img.add_header("Content-ID", f"<{cid_show}>")
                        img.add_header("Content-Disposition", "inline", filename=f"{cid_show}.jpg")
                        msg.attach(img)
                        images_attached[show_title] = f"cid:{cid_show}"
                    except Exception:
                        images_attached[show_title] = fallback_url

                grouped[show_title]['show_poster_ref'] = images_attached[show_title]

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
                except Exception:
                    episode_ref = fallback_url

                episode_link = None
                if plex_app_base and ep.ratingKey:
                    episode_link = f"{plex_app_base}{quote('/library/metadata/' + str(ep.ratingKey))}"

                grouped[show_title]['episodes'].append({
                    'show_title': ep.grandparentTitle,
                    'season': ep.parentIndex,
                    'episode': ep.index,
                    'ep_title': ep.title,
                    'synopsis': ep.summary or 'No synopsis available.',
                    'episode_poster_ref': episode_ref,
                    'episode_link': episode_link,
                })

            token = serializer.dumps(email, salt="unsubscribe")
            html_body = template.render(
                grouped_episodes=grouped,
                base_url=s.base_url,
                email=email,
                token=token
            )
            plain_lines = []
            for show in grouped.values():
                if show['show_link']:
                    plain_lines.append(f"{show['show_title']} - {show['show_link']}")
                else:
                    plain_lines.append(f"{show['show_title']}")
                for ep in show['episodes']:
                    episode_line = f"  S{ep['season']:02}E{ep['episode']:02} - {ep['ep_title']}"
                    if ep['episode_link']:
                        episode_line = f"{episode_line} ({ep['episode_link']})"
                    plain_lines.append(episode_line)
            plain_body = "\n".join(plain_lines)

            msg.attach(MIMEText(plain_body, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            # Send email with retry logic
            email_success = _send_email_with_retry(s, msg)

            if email_success:
                # Log to file
                user_log = get_user_logger(email)
                for ep in eps:
                    user_log.info(f"Notified: {ep.grandparentTitle} [Key:{ep.grandparentRatingKey}] S{ep.parentIndex}E{ep.index} - {ep.title}")
                    # Save to database for better tracking
                    _save_notification_to_db(email, ep)

                current_app.logger.info(f"✅ Email sent to {email} with {len(eps)} episodes")
                notif_logger.info(
                    f"Sent to {email} | Episodes: {', '.join(f'{e.grandparentTitle} S{e.parentIndex}E{e.index}' for e in eps)}"
                )
            else:
                current_app.logger.error(f"❌ Failed to send email to {email} after all retry attempts")

        current_app.logger.info("✅ check_new_episodes job completed.")
        scheduler: BaseScheduler = current_app.extensions.get('apscheduler')
        if scheduler:
            job = scheduler.get_job('check_job')
            if job and job.next_run_time:
                current_app.logger.info(f"⏭️ Next scheduled run at {job.next_run_time.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}")
            else:
                current_app.logger.warning("⚠️ Could not retrieve next_run_time from scheduler.")


def get_user_logger(email):
    from logging.handlers import RotatingFileHandler

    safe_filename = email_to_filename(email)
    filename = f"{safe_filename}-notification.log"

    log_dir = os.path.join(os.path.dirname(__file__), "../instance/logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, filename)

    logger_name = f"userlog.{safe_filename}"
    logger = logging.getLogger(logger_name)
    logger.propagate = False  # ✅ keep console clean

    if not logger.handlers:
        handler = RotatingFileHandler(log_path, maxBytes=USER_LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
        handler.setFormatter(TZFormatter('%(asctime)s | %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


def _save_notification_to_db(email: str, episode: Episode) -> None:
    """Save notification to database for tracking and deduplication."""
    try:
        normalized_email = normalize_email(email)
        notification = Notification(
            email=normalized_email,
            show_title=episode.grandparentTitle,
            show_key=str(episode.grandparentRatingKey),
            season=episode.parentIndex,
            episode=episode.index,
            episode_title=episode.title,
            episode_key=str(episode.ratingKey) if episode.ratingKey else None
        )
        db.session.add(notification)
        db.session.commit()

        # Invalidate cache for this user
        if normalized_email in notification_cache:
            del notification_cache[normalized_email]
    except Exception as e:
        current_app.logger.error(f"Failed to save notification to database for {email}: {e}")
        db.session.rollback()


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
    def _is_affirmative_watched(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value > 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if not normalized:
                return False
            try:
                # Numeric strings such as "1" should be treated as watched
                return float(normalized) > 0
            except ValueError:
                return normalized in AFFIRMATIVE_WATCHED_STATUSES
        return False

    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        # Tautulli's API caps the history "length" parameter at 1000 records.
        page_length = 1000
        start = 0
        grandparent_key_str = str(grandparent_rating_key)

        while True:
            params = {
                'apikey': s.tautulli_api_key,
                'cmd': 'get_history',
                'user_id': user_id,
                'grandparent_rating_key': grandparent_rating_key,
                'start': start,
                'length': page_length
            }
            resp = requests.get(base, params=params, timeout=10)
            resp.raise_for_status()

            payload = resp.json().get('response', {}).get('data', {})
            history = payload.get('data') or []

            for item in history:
                gp_key = str(item.get('grandparent_rating_key'))
                if gp_key != grandparent_key_str:
                    continue
                if _is_affirmative_watched(item.get('watched_status')):
                    return True

            records_filtered = payload.get('recordsFiltered')
            if not history:
                break

            consumed = start + len(history)
            if isinstance(records_filtered, int) and consumed >= records_filtered:
                break

            start = consumed

        return False
    except Exception as e:
        current_app.logger.error(f"Error checking show history for user {user_id}: {e}")
        return False


def _send_email_with_retry(s: Settings, msg: MIMEMultipart, max_attempts: int = EMAIL_RETRY_ATTEMPTS) -> bool:
    """Send email with exponential backoff retry logic.

    Returns True if email was sent successfully, False otherwise.
    """
    last_error = None
    for attempt in range(max_attempts):
        try:
            smtp = smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=30)
            smtp.starttls()
            smtp.login(s.smtp_user, s.smtp_pass)
            smtp.send_message(msg)
            smtp.quit()
            if attempt > 0:
                current_app.logger.info(f"Email to {msg['To']} sent successfully on attempt {attempt + 1}")
            return True
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                wait_time = min(
                    EMAIL_RETRY_MIN_WAIT_SECONDS * (2 ** attempt),
                    EMAIL_RETRY_MAX_WAIT_SECONDS
                )
                current_app.logger.warning(
                    f"Email send attempt {attempt + 1}/{max_attempts} failed for {msg['To']}: {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                current_app.logger.error(
                    f"Failed to send email to {msg['To']} after {max_attempts} attempts: {e}"
                )
    return False


def _send_email(s: Settings, msg: MIMEMultipart) -> None:
    """Backward compatibility wrapper for _send_email_with_retry."""
    _send_email_with_retry(s, msg)


def register_debug_route(app: Flask):
    @app.route('/force-run')
    def force_run():
        check_new_episodes(app)
        return "Manual notification job complete"
