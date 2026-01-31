import os
import smtplib
import requests
import logging
import time
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from typing import List, Dict, Any, Set, Optional, Tuple
from collections import deque, Counter
from logging.handlers import RotatingFileHandler
from functools import lru_cache
from cachetools import TTLCache

from .logging_utils import TZFormatter
from .utils import normalize_email, email_to_filename, normalize_show_identity, redact_email
from .constants import (
    NOTIFICATION_HISTORY_LIMIT,
    NOTIFICATION_CACHE_TTL_SECONDS,
    EMAIL_RETRY_ATTEMPTS,
    EMAIL_RETRY_MIN_WAIT_SECONDS,
    EMAIL_RETRY_MAX_WAIT_SECONDS,
    USER_LOG_MAX_BYTES,
    GLOBAL_LOG_MAX_BYTES,
    APP_LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
    TAUTULLI_MAX_PAGE_LENGTH,
    API_RETRY_ATTEMPTS,
    API_RETRY_MIN_WAIT_SECONDS,
    API_RETRY_MAX_WAIT_SECONDS,
    TAUTULLI_WATCHED_PERCENT_THRESHOLD,
)

from flask import current_app, Flask
from apscheduler.schedulers.background import BackgroundScheduler
from jinja2 import Environment, FileSystemLoader, select_autoescape
from plexapi.server import PlexServer
from plexapi.video import Episode
from apscheduler.schedulers.base import BaseScheduler
from itsdangerous import URLSafeTimedSerializer

from .config import Settings, UserPreferences, Notification, EpisodeFirstSeen, db
from sqlalchemy import or_

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

app_logger = logging.getLogger("plex_notifier")
app_logger.setLevel(logging.INFO)
app_log_path = os.path.join(notif_log_dir, "app.log")
app_handler = RotatingFileHandler(app_log_path, maxBytes=APP_LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
app_handler.setFormatter(TZFormatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
app_logger.addHandler(app_handler)
app_logger.propagate = False

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


def _extract_show_guid(episode: Episode) -> Optional[str]:
    guid = getattr(episode, "grandparentGuid", None)
    if isinstance(guid, (list, tuple)):
        guid = guid[0] if guid else None
    if guid:
        return str(guid)
    return None


def _get_fallback_identity_for_episode(episode: Episode) -> str:
    title = getattr(episode, "grandparentTitle", None)
    year = getattr(episode, "grandparentYear", None)
    if year is None:
        year = getattr(episode, "year", None)
    return normalize_show_identity(title, year)


def _get_show_guid_for_episode(
    episode: Episode,
    *,
    fallback_identity: Optional[str] = None,
    prefer_fallback_identity: bool = False,
) -> Optional[str]:
    fallback_guid = fallback_identity or _get_fallback_identity_for_episode(episode)
    if prefer_fallback_identity and fallback_guid:
        return fallback_guid

    guid = _extract_show_guid(episode)
    if guid:
        return guid

    return fallback_guid or None


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
            season_episode = f"S{notif.season}E{notif.episode}"
            if notif.episode_key:
                notified.add(str(notif.episode_key))
            if notif.show_guid:
                notified.add(f"{notif.show_guid}|{season_episode}")
            if notif.show_key:
                notified.add(f"{notif.show_key}|{season_episode}")
            if notif.show_title:
                fallback_identity = normalize_show_identity(notif.show_title)
                if fallback_identity:
                    notified.add(f"{fallback_identity}|{season_episode}")
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


def _coerce_plex_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if not value:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _get_episode_availability_datetime(episode: Episode) -> Optional[datetime]:
    for attr in ("originallyAvailableAt", "availableAt"):
        candidate = getattr(episode, attr, None)
        if candidate:
            return _coerce_plex_datetime(candidate)
    return None


def _parse_fallback_identity(identity: str | None) -> tuple[str | None, int | None]:
    if not identity or not identity.startswith("title:"):
        return None, None
    title_part, _, year_part = identity.partition("|year:")
    title = title_part.replace("title:", "").replace("-", " ").strip()
    year = None
    if year_part:
        try:
            year = int(year_part.strip())
        except ValueError:
            year = None
    return title or None, year


def _normalize_stored_identity(value: Optional[str]) -> str:
    if not value:
        return ""
    raw_value = str(value).strip()
    if not raw_value:
        return ""
    lowered = raw_value.lower()
    if lowered.startswith("title:"):
        return lowered
    year_match = re.search(r"\|year:(\d{4})$", lowered)
    year = int(year_match.group(1)) if year_match else None
    title_part = raw_value[:year_match.start()] if year_match else raw_value
    return normalize_show_identity(title_part, year)


def _extract_show_year_from_title(title: str | None) -> tuple[str | None, int | None]:
    if not title:
        return None, None
    year_match = re.search(r"\((\d{4})\)\s*$", title)
    if not year_match:
        return title, None
    try:
        year = int(year_match.group(1))
    except ValueError:
        return title, None
    cleaned_title = title[:year_match.start()].strip()
    return cleaned_title or title, year


def _normalize_title_for_match(title: str | None) -> str:
    if not title:
        return ""
    return re.sub(r"[^a-z0-9]+", "", title.lower())


def _extract_show_guid_from_metadata(item: Any) -> Optional[str]:
    guid = getattr(item, "guid", None)
    if isinstance(guid, (list, tuple)):
        guid = guid[0] if guid else None
    if guid:
        return str(guid)
    return None


def reconcile_user_preferences(
    app: Flask,
    *,
    run_reason: str = "startup",
    cutoff_days: int = 30,
) -> None:
    with app.app_context():
        s = Settings.query.first()
        if not s or not s.plex_url or not s.plex_token or s.plex_token == "placeholder":
            app.logger.info("Preference reconciliation skipped: Plex settings not configured.")
            return

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            tv_section = plex.library.section("TV Shows")
        except Exception as exc:
            app.logger.warning(f"Preference reconciliation skipped: unable to connect to Plex ({exc}).")
            return

        notification_rows = (
            db.session.query(
                Notification.show_key,
                Notification.show_guid,
                Notification.show_title,
            )
            .distinct()
            .all()
        )
        preferences = UserPreferences.query.filter(
            or_(UserPreferences.show_key.isnot(None), UserPreferences.show_guid.isnot(None))
        ).all()

        show_groups: dict[str, dict[str, Any]] = {}
        guid_index: dict[str, str] = {}
        key_index: dict[str, str] = {}
        title_index: dict[str, str] = {}

        def _fetch_show_by_key(show_key_value: str) -> Any:
            if not show_key_value:
                return None
            try:
                return tv_section.get(show_key_value)
            except Exception:
                try:
                    fetch_path = (
                        show_key_value
                        if "/library/metadata/" in show_key_value
                        else f"/library/metadata/{show_key_value}"
                    )
                    return plex.fetchItem(fetch_path)
                except Exception as exc:
                    app.logger.warning(
                        "Preference reconciliation failed to fetch show metadata for key '%s': %s",
                        show_key_value,
                        exc,
                    )
                    return None

        def _search_show_by_title(search_title: str, search_year: Optional[int]) -> Any:
            if not search_title:
                return None
            try:
                if search_year:
                    search_results = tv_section.search(title=search_title, year=search_year, libtype="show")
                else:
                    search_results = tv_section.search(title=search_title, libtype="show")
            except Exception as exc:
                app.logger.warning(f"Preference reconciliation search failed for '{search_title}': {exc}")
                return None

            if not search_results:
                return None

            title_key = _normalize_title_for_match(search_title)
            for show in search_results:
                if search_year and getattr(show, "year", None) == search_year:
                    return show
                if _normalize_title_for_match(getattr(show, "title", "")) == title_key:
                    return show
            return search_results[0]

        def _log_mismatch(
            pref: UserPreferences,
            *,
            source: str,
            changes: Dict[str, Tuple[Optional[str], Optional[str]]],
        ) -> None:
            if not changes:
                return
            change_summary = ", ".join(
                f"{field} {old or 'None'} -> {new or 'None'}"
                for field, (old, new) in changes.items()
            )
            app.logger.info(
                "Preference reconciliation mismatch detected for preference %s (%s): %s",
                pref.id,
                source,
                change_summary,
            )

        def _merge_groups(primary_id: str, merge_id: str) -> None:
            if primary_id == merge_id:
                return
            primary = show_groups[primary_id]
            secondary = show_groups.pop(merge_id)
            for guid_value in secondary["match_guids"]:
                primary["match_guids"].add(guid_value)
                guid_index[guid_value] = primary_id
            for key_value in secondary["match_keys"]:
                primary["match_keys"].add(key_value)
                key_index[key_value] = primary_id
            if secondary.get("title_identity"):
                primary.setdefault("title_identity", secondary["title_identity"])
                title_index[secondary["title_identity"]] = primary_id
            if secondary.get("show_guid") and not primary.get("show_guid"):
                primary["show_guid"] = secondary["show_guid"]
            if secondary.get("show_key") and not primary.get("show_key"):
                primary["show_key"] = secondary["show_key"]
            if secondary.get("title") and not primary.get("title"):
                primary["title"] = secondary["title"]
                primary["year"] = secondary.get("year")
            for pref_id, pref in secondary["prefs"].items():
                primary["prefs"].setdefault(pref_id, pref)

        def _ensure_group(
            *,
            show_guid: Optional[str],
            show_key: Optional[str],
            title: Optional[str],
            year: Optional[int],
            title_identity: Optional[str],
        ) -> dict[str, Any]:
            group_ids: Set[str] = set()
            guid_value = None
            if show_guid:
                guid_value = str(show_guid)
                if guid_value in guid_index:
                    group_ids.add(guid_index[guid_value])
            key_value = None
            if show_key:
                key_value = str(show_key)
                if key_value in key_index:
                    group_ids.add(key_index[key_value])
            if title_identity and title_identity in title_index:
                group_ids.add(title_index[title_identity])

            if not group_ids:
                group_id = f"group-{len(show_groups) + 1}"
                show_groups[group_id] = {
                    "show_guid": None,
                    "show_key": None,
                    "title": None,
                    "year": None,
                    "title_identity": None,
                    "match_guids": set(),
                    "match_keys": set(),
                    "prefs": {},
                }
            else:
                group_id = sorted(group_ids)[0]
                for merge_id in sorted(group_ids)[1:]:
                    _merge_groups(group_id, merge_id)

            group = show_groups[group_id]

            if guid_value:
                group["match_guids"].add(guid_value)
                if not guid_value.startswith("title:"):
                    group.setdefault("show_guid", guid_value)
                    guid_index[guid_value] = group_id
                else:
                    title_identity = title_identity or _normalize_stored_identity(guid_value)
            if key_value:
                group["match_keys"].add(key_value)
                group.setdefault("show_key", key_value)
                key_index[key_value] = group_id
            if title_identity:
                group.setdefault("title_identity", title_identity)
                title_index[title_identity] = group_id
            if title and not group.get("title"):
                group["title"] = title
                group["year"] = year

            return group

        for show_key, show_guid, show_title in notification_rows:
            title = None
            year = None
            if show_title:
                title, year = _extract_show_year_from_title(show_title)
            title_identity = normalize_show_identity(title, year) if title else None
            _ensure_group(
                show_guid=str(show_guid) if show_guid else None,
                show_key=str(show_key) if show_key else None,
                title=title,
                year=year,
                title_identity=title_identity,
            )

        for pref in preferences:
            pref_guid = str(pref.show_guid) if pref.show_guid else None
            pref_key = str(pref.show_key) if pref.show_key else None
            title_identity = None
            if pref_guid and pref_guid.startswith("title:"):
                title_identity = _normalize_stored_identity(pref_guid)
            group = _ensure_group(
                show_guid=pref_guid,
                show_key=pref_key,
                title=None,
                year=None,
                title_identity=title_identity,
            )
            if pref.id is not None:
                group["prefs"].setdefault(pref.id, pref)

        updated_count = 0
        scanned_count = 0
        pending_updates = 0
        batch_size = 50

        for group in show_groups.values():
            title = group.get("title")
            year = group.get("year")
            if not title and group.get("title_identity"):
                title, year = _parse_fallback_identity(group["title_identity"])
            show_key = group.get("show_key")

            if not show_key and not title:
                continue

            scanned_count += 1

            matched_show = None
            needs_reconcile = False
            for pref in group["prefs"].values():
                stored_key = str(pref.show_key) if pref.show_key else None
                stored_guid = str(pref.show_guid) if pref.show_guid else None
                if stored_key:
                    candidate_show = _fetch_show_by_key(stored_key)
                    if candidate_show:
                        if not matched_show:
                            matched_show = candidate_show
                        canonical_key = str(getattr(candidate_show, "ratingKey", "") or "") or None
                        canonical_guid = _extract_show_guid_from_metadata(candidate_show)
                        changes = {}
                        if canonical_key and stored_key != canonical_key:
                            changes["show_key"] = (stored_key, canonical_key)
                        if canonical_guid and stored_guid and stored_guid != canonical_guid:
                            changes["show_guid"] = (stored_guid, canonical_guid)
                        if changes:
                            needs_reconcile = True
                            _log_mismatch(pref, source="show_key", changes=changes)
                    continue

                if stored_guid:
                    search_title = None
                    search_year = None
                    if stored_guid.startswith("title:"):
                        search_title, search_year = _parse_fallback_identity(stored_guid)
                    if not search_title:
                        search_title = title
                        search_year = year
                    if not search_title and group.get("title_identity"):
                        search_title, search_year = _parse_fallback_identity(group["title_identity"])
                    candidate_show = _search_show_by_title(search_title, search_year)
                    if candidate_show:
                        if not matched_show:
                            matched_show = candidate_show
                        canonical_key = str(getattr(candidate_show, "ratingKey", "") or "") or None
                        canonical_guid = _extract_show_guid_from_metadata(candidate_show)
                        changes = {}
                        if canonical_guid and stored_guid != canonical_guid:
                            changes["show_guid"] = (stored_guid, canonical_guid)
                        if canonical_key and pref.show_key and str(pref.show_key) != canonical_key:
                            changes["show_key"] = (str(pref.show_key), canonical_key)
                        if changes:
                            needs_reconcile = True
                            _log_mismatch(pref, source="show_guid", changes=changes)

            if not needs_reconcile:
                if show_key:
                    matched_show = matched_show or _fetch_show_by_key(show_key)

                if not matched_show and title:
                    matched_show = _search_show_by_title(title, year)

            if not matched_show:
                continue

            new_show_key = str(getattr(matched_show, "ratingKey", "") or "") or None
            new_show_guid = _extract_show_guid_from_metadata(matched_show)

            for pref in group["prefs"].values():
                if new_show_key and pref.show_key != new_show_key:
                    pref.show_key = new_show_key
                if new_show_guid and pref.show_guid != new_show_guid:
                    pref.show_guid = new_show_guid
                if db.session.is_modified(pref, include_collections=False):
                    updated_count += 1
                    pending_updates += 1
                    if pending_updates >= batch_size:
                        try:
                            db.session.commit()
                            pending_updates = 0
                        except Exception as exc:
                            app.logger.warning(f"Preference reconciliation failed to commit updates: {exc}")
                            db.session.rollback()
                            return

        if pending_updates:
            try:
                db.session.commit()
            except Exception as exc:
                app.logger.warning(f"Preference reconciliation failed to commit updates: {exc}")
                db.session.rollback()
                return

        app.logger.info(
            "Preference reconciliation (%s) updated %s preferences across %s scanned shows.",
            run_reason,
            updated_count,
            scanned_count,
        )


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

        processed_subscription_fallback_misses: Set[Tuple[str, str]] = set()
        subscription_fallback_miss_counts: Counter[str] = Counter()

        interval = override_interval_minutes or s.notify_interval or 30
        cutoff_dt = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(minutes=interval)
        now_dt = datetime.now(timezone.utc)

        machine_id = None

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            machine_id = plex.machineIdentifier
            tv = plex.library.section('TV Shows')
            all_eps = tv.search(libtype='episode')

            episode_keys = [
                str(ep.ratingKey)
                for ep in all_eps
                if isinstance(ep, Episode) and ep.ratingKey is not None
            ]
            existing_first_seen: Dict[str, datetime] = {}
            if episode_keys:
                first_seen_rows = (
                    EpisodeFirstSeen.query
                    .filter(EpisodeFirstSeen.episode_key.in_(episode_keys))
                    .all()
                )
                existing_first_seen = {
                    row.episode_key: _coerce_plex_datetime(row.first_seen_at)
                    for row in first_seen_rows
                }

            new_first_seen_rows: List[EpisodeFirstSeen] = []
            recent_eps: List[Episode] = []
            for ep in all_eps:
                if not isinstance(ep, Episode):
                    continue

                availability_dt = _get_episode_availability_datetime(ep)
                availability_recent = availability_dt is not None and availability_dt >= cutoff_dt

                rating_key = str(ep.ratingKey) if ep.ratingKey is not None else None
                first_seen_at = None
                if rating_key:
                    first_seen_at = existing_first_seen.get(rating_key)
                    if not first_seen_at:
                        first_seen_at = now_dt
                        new_first_seen_rows.append(
                            EpisodeFirstSeen(
                                episode_key=rating_key,
                                first_seen_at=first_seen_at,
                            )
                        )

                first_seen_recent = first_seen_at is not None and first_seen_at >= cutoff_dt
                if availability_recent or first_seen_recent:
                    recent_eps.append(ep)

            if new_first_seen_rows:
                try:
                    db.session.add_all(new_first_seen_rows)
                    db.session.commit()
                except Exception as exc:
                    current_app.logger.warning(
                        "Failed to persist episode first-seen records: %s",
                        exc,
                    )
                    db.session.rollback()
            local_time = cutoff_dt.astimezone()
            current_app.logger.info(f"📺 Filtered {len(recent_eps)} recent episodes since {local_time.isoformat()}")

        except Exception as e:
            current_app.logger.error(f"Error connecting to Plex: {e}")
            return

        if not recent_eps:
            current_app.logger.info("⚠️ No recent episodes found.")
            return

        users = _get_users(s, machine_id)
        if not users:
            current_app.logger.info("⚠️ No users fetched.")
            return

        user_eps: Dict[str, List[Dict[str, Any]]] = {}

        for user in users:
            uid = user.get('user_id')
            user_email = user.get('email')
            if not user_email or user_email == s.from_address:
                continue

            # Validate email has domain
            if '@' not in user_email:
                current_app.logger.warning(
                    "⚠️ Skipping user with incomplete email: %s",
                    redact_email(user_email),
                )
                continue

            canon = normalize_email(user_email)
            redacted_email = redact_email(user_email)

            # 🔒 Check global opt-out
            pref = UserPreferences.query.filter_by(email=canon, show_key=None).first()
            if not pref:
                pref = UserPreferences.query.filter_by(email=user_email, show_key=None).first()
                if pref and pref.email != canon:
                    pref.email = canon
                    db.session.commit()
            if pref and pref.global_opt_out:
                continue

            watchable: List[Dict[str, Any]] = []
            recent_notified = _get_recent_notifications(canon)
            recent_show_keys: Set[str] = set()
            recent_show_guids: Set[str] = set()
            recent_show_fallbacks: Set[str] = set()
            try:
                recent_notifications = (
                    Notification.query
                    .filter_by(email=canon)
                    .order_by(Notification.timestamp.desc())
                    .limit(NOTIFICATION_HISTORY_LIMIT)
                    .all()
                )
                for notif in recent_notifications:
                    if notif.show_key:
                        recent_show_keys.add(str(notif.show_key))
                    if notif.show_guid:
                        recent_show_guids.add(str(notif.show_guid))
                    if notif.show_title:
                        fallback_identity = normalize_show_identity(notif.show_title)
                        if fallback_identity:
                            recent_show_fallbacks.add(fallback_identity)
            except Exception as exc:
                current_app.logger.warning(
                    "Unable to load recent show identifiers for %s: %s",
                    redacted_email,
                    exc,
                )
            needs_commit = False

            for ep in recent_eps:
                show_key = ep.grandparentRatingKey
                show_key_str = str(show_key) if show_key is not None else None
                show_title = ep.grandparentTitle
                show_year = getattr(ep, "grandparentYear", None)
                if show_year is None:
                    show_year = getattr(ep, "year", None)
                fallback_identity = _get_fallback_identity_for_episode(ep)
                raw_show_guid = _extract_show_guid(ep)

                if not show_key_str and not fallback_identity:
                    continue

                has_recent_notification_for_show = any(
                    candidate
                    for candidate in (show_key_str, raw_show_guid, fallback_identity)
                    if candidate and candidate in (recent_show_keys | recent_show_guids | recent_show_fallbacks)
                )
                mismatch_detected = False
                if fallback_identity and has_recent_notification_for_show:
                    if show_key_str and show_key_str not in recent_show_keys:
                        mismatch_detected = True
                    if raw_show_guid and raw_show_guid not in recent_show_guids:
                        mismatch_detected = True
                    if not show_key_str and not raw_show_guid:
                        mismatch_detected = True

                prefer_fallback_identity = mismatch_detected or (show_key_str is None and bool(fallback_identity))
                fallback_log_needed = prefer_fallback_identity
                show_guid = _get_show_guid_for_episode(
                    ep,
                    fallback_identity=fallback_identity,
                    prefer_fallback_identity=prefer_fallback_identity,
                )

                # 🔒 Check per-show opt-out
                show_pref = None
                for guid_candidate in (raw_show_guid, fallback_identity):
                    if not guid_candidate:
                        continue
                    show_pref = UserPreferences.query.filter_by(email=canon, show_guid=guid_candidate).first()
                    if not show_pref:
                        show_pref = UserPreferences.query.filter_by(email=user_email, show_guid=guid_candidate).first()
                    if show_pref:
                        break
                if not show_pref and show_key_str is not None:
                    show_pref = UserPreferences.query.filter_by(email=canon, show_key=show_key_str).first()
                    if not show_pref:
                        show_pref = UserPreferences.query.filter_by(email=user_email, show_key=show_key_str).first()
                if not show_pref:
                    show_pref = None
                if show_pref:
                    if show_pref.email != canon:
                        show_pref.email = canon
                        needs_commit = True
                    if show_guid and show_pref.show_guid != show_guid:
                        show_pref.show_guid = show_guid
                        needs_commit = True
                    if show_pref.show_key != show_key_str and show_key_str is not None:
                        show_pref.show_key = show_key_str
                        needs_commit = True
                if show_pref and show_pref.show_opt_out:
                    continue

                has_watched_show, history_status = _user_has_watched_show(s, uid, show_key, fallback_identity)
                if not has_watched_show:
                    if history_status in {"empty", "error"}:
                        has_subscription, fallback_preferences = _user_has_subscription_fallback(
                            canon,
                            user_email,
                            show_key_str,
                            raw_show_guid,
                            fallback_identity,
                            show_title,
                            show_year,
                        )
                        if has_subscription:
                            current_app.logger.info(
                                "Using subscription fallback for %s (%s) because Tautulli history was %s for %s.",
                                show_title or "Unknown",
                                show_key_str or fallback_identity or "unknown",
                                history_status,
                                redacted_email,
                            )
                            has_watched_show = True
                            show_guid_update = raw_show_guid or show_guid
                            if show_guid_update or show_key_str:
                                for preference in fallback_preferences:
                                    if preference.show_opt_out:
                                        continue
                                    if show_key_str and preference.show_key != show_key_str:
                                        preference.show_key = show_key_str
                                        needs_commit = True
                                    if show_guid_update and preference.show_guid != show_guid_update:
                                        preference.show_guid = show_guid_update
                                        needs_commit = True
                        else:
                            item_id = show_key_str or fallback_identity or "unknown"
                            dedup_key = (canon, item_id)
                            if dedup_key not in processed_subscription_fallback_misses:
                                subscription_fallback_miss_counts[show_title or item_id] += 1
                                if current_app.logger.isEnabledFor(logging.DEBUG):
                                    current_app.logger.debug(
                                        "No subscription fallback found for %s (%s) after Tautulli history %s for %s.",
                                        show_title or "Unknown",
                                        item_id,
                                        history_status,
                                        redacted_email,
                                    )
                                processed_subscription_fallback_misses.add(dedup_key)
                    if not has_watched_show and has_recent_notification_for_show and fallback_identity:
                        prefer_fallback_identity = True
                        fallback_log_needed = True
                        show_guid = _get_show_guid_for_episode(
                            ep,
                            fallback_identity=fallback_identity,
                            prefer_fallback_identity=prefer_fallback_identity,
                        )
                        has_watched_show = True
                    if not has_watched_show:
                        continue
                if prefer_fallback_identity and fallback_identity and fallback_log_needed:
                    current_app.logger.info(
                        "Fallback identity match used for show "
                        f"{show_title or 'Unknown'} ({fallback_identity}) for {redacted_email}."
                    )
                    fallback_log_needed = False
                if show_pref and show_guid and show_pref.show_guid != show_guid:
                    show_pref.show_guid = show_guid
                    needs_commit = True
                if _user_has_history(s, uid, ep.ratingKey):
                    continue

                season_episode = f"S{ep.parentIndex}E{ep.index}"
                candidate_ids: List[str] = []
                if ep.ratingKey:
                    candidate_ids.append(str(ep.ratingKey))
                if raw_show_guid:
                    candidate_ids.append(f"{raw_show_guid}|{season_episode}")
                elif show_guid:
                    candidate_ids.append(f"{show_guid}|{season_episode}")
                if show_key_str:
                    candidate_ids.append(f"{show_key_str}|{season_episode}")
                if fallback_identity:
                    candidate_ids.append(f"{fallback_identity}|{season_episode}")
                if not candidate_ids:
                    continue
                if any(candidate in recent_notified for candidate in candidate_ids):
                    continue

                watchable.append({
                    "episode": ep,
                    "show_guid": show_guid,
                    "fallback_identity": fallback_identity,
                })

            if needs_commit:
                try:
                    db.session.commit()
                except Exception as exc:
                    current_app.logger.warning(
                        "Failed to backfill show identifiers for %s: %s",
                        redacted_email,
                        exc,
                    )
                    db.session.rollback()

            if watchable:
                user_eps[user_email] = watchable

        if subscription_fallback_miss_counts:
            total_misses = sum(subscription_fallback_miss_counts.values())
            top_misses = subscription_fallback_miss_counts.most_common(3)
            top_summary = ", ".join(
                f"{show} ({count})" for show, count in top_misses
            )
            current_app.logger.info(
                "Subscription fallback misses this run: %s total. Top shows: %s.",
                total_misses,
                top_summary or "None",
            )

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
        plex_mobile_base = None
        if machine_id:
            plex_app_base = f"https://app.plex.tv/desktop#!/server/{machine_id}/details?key="
            plex_mobile_base = f"plex://server/{machine_id}/details?key="

        for email, eps in user_eps.items():
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"{len(eps)} New Episode{'s' if len(eps) != 1 else ''} Available"
            msg['From'] = s.from_address
            msg['To'] = email

            images_attached = {}
            grouped = {}

            for idx, ep_payload in enumerate(eps, start=1):
                ep = ep_payload["episode"]
                show_title = ep.grandparentTitle
                show_link = None
                show_mobile_link = None
                show_key = ep.grandparentRatingKey
                if plex_app_base and show_key:
                    show_link = f"{plex_app_base}{quote('/library/metadata/' + str(show_key))}"
                if plex_mobile_base and show_key:
                    show_mobile_link = f"{plex_mobile_base}{quote('/library/metadata/' + str(show_key))}"

                if show_title not in grouped:
                    grouped[show_title] = {
                        'show_title': show_title,
                        'show_poster_ref': fallback_url,
                        'show_link': show_link,
                        'show_mobile_link': show_mobile_link,
                        'episodes': [],
                    }
                elif not grouped[show_title]['show_link'] and show_link:
                    grouped[show_title]['show_link'] = show_link
                    grouped[show_title]['show_mobile_link'] = show_mobile_link

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
                episode_mobile_link = None
                if plex_app_base and ep.ratingKey:
                    episode_link = f"{plex_app_base}{quote('/library/metadata/' + str(ep.ratingKey))}"
                if plex_mobile_base and ep.ratingKey:
                    episode_mobile_link = f"{plex_mobile_base}{quote('/library/metadata/' + str(ep.ratingKey))}"

                # Truncate synopsis to 200 characters for better email readability
                synopsis = ep.summary or 'No synopsis available.'
                if len(synopsis) > 200:
                    synopsis = synopsis[:197] + '...'

                grouped[show_title]['episodes'].append({
                    'show_title': ep.grandparentTitle,
                    'season': ep.parentIndex,
                    'episode': ep.index,
                    'ep_title': ep.title,
                    'synopsis': synopsis,
                    'episode_poster_ref': episode_ref,
                    'episode_link': episode_link,
                    'episode_mobile_link': episode_mobile_link,
                })

            # Sort episodes within each show by season and episode number
            for show_title in grouped:
                grouped[show_title]['episodes'].sort(key=lambda ep: (ep['season'], ep['episode']))

            # Sort shows alphabetically by title for consistent ordering
            grouped = dict(sorted(grouped.items(), key=lambda item: item[0].lower()))

            token = serializer.dumps(email, salt="unsubscribe")
            html_body = template.render(
                grouped_episodes=grouped,
                base_url=s.base_url,
                email=email,
                token=token
            )
            plain_lines = []
            for show in grouped.values():
                # Prefer mobile link, fallback to web link
                link = show.get('show_mobile_link') or show.get('show_link')
                if link:
                    plain_lines.append(f"{show['show_title']} - {link}")
                else:
                    plain_lines.append(f"{show['show_title']}")
                for ep in show['episodes']:
                    episode_line = f"  S{ep['season']:02}E{ep['episode']:02} - {ep['ep_title']}"
                    # Prefer mobile link, fallback to web link
                    ep_link = ep.get('episode_mobile_link') or ep.get('episode_link')
                    if ep_link:
                        episode_line = f"{episode_line} ({ep_link})"
                    plain_lines.append(episode_line)
            plain_body = "\n".join(plain_lines)

            msg.attach(MIMEText(plain_body, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            # Send email with retry logic
            email_success = _send_email_with_retry(s, msg)

            redacted_email = redact_email(email)
            if email_success:
                # Log to file
                user_log = get_user_logger(email)
                for ep_payload in eps:
                    ep = ep_payload["episode"]
                    user_log.info(f"Notified: {ep.grandparentTitle} [Key:{ep.grandparentRatingKey}] S{ep.parentIndex}E{ep.index} - {ep.title}")
                    # Save to database for better tracking
                    _save_notification_to_db(email, ep, ep_payload.get("show_guid"))

                current_app.logger.info(
                    "✅ Email sent to %s with %s episodes",
                    redacted_email,
                    len(eps),
                )
                episodes_desc = ", ".join(
                    f"{payload['episode'].grandparentTitle} "
                    f"S{payload['episode'].parentIndex}E{payload['episode'].index}"
                    for payload in eps
                )
                notif_logger.info("Sent to %s | Episodes: %s", redacted_email, episodes_desc)
            else:
                current_app.logger.error(
                    "❌ Failed to send email to %s after all retry attempts",
                    redacted_email,
                )

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


def _save_notification_to_db(
    email: str,
    episode: Episode,
    show_guid_override: Optional[str] = None,
) -> None:
    """Save notification to database for tracking and deduplication."""
    try:
        normalized_email = normalize_email(email)
        notification = Notification(
            email=normalized_email,
            show_title=episode.grandparentTitle,
            show_key=str(episode.grandparentRatingKey) if episode.grandparentRatingKey is not None else None,
            show_guid=show_guid_override or _get_show_guid_for_episode(episode),
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
        current_app.logger.error(
            "Failed to save notification to database for %s: %s",
            redact_email(email),
            e,
        )
        db.session.rollback()


def _get_users(s: Settings, machine_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if s.tautulli_url and s.tautulli_api_key:
        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            account = plex.myPlexAccount()

            whitelist: Set[str] = set()

            def _add_to_whitelist(user: Any) -> None:
                email = getattr(user, "email", None)
                username = getattr(user, "username", None) or getattr(user, "title", None)

                if email:
                    whitelist.add(normalize_email(email))
                if username and isinstance(username, str):
                    username_normalized = username.strip().lower()
                    if username_normalized:
                        whitelist.add(username_normalized)

            def _user_has_server_share(user: Any) -> bool:
                if not machine_id:
                    return True

                servers = None
                try:
                    servers_attr = getattr(user, "servers", None)
                    servers = servers_attr() if callable(servers_attr) else servers_attr
                except Exception as exc:
                    current_app.logger.warning(
                        f"⚠️ Unable to load shared servers for Plex user {getattr(user, 'username', None)}: {exc}"
                    )
                    return False

                if not servers:
                    current_app.logger.warning(
                        "⚠️ Plex user missing server share metadata; skipping share validation for user "
                        f"{getattr(user, 'username', None) or getattr(user, 'title', None)}."
                    )
                    return False

                for server in servers:
                    if isinstance(server, dict):
                        server_machine_id = server.get("machineIdentifier") or server.get("clientIdentifier")
                    else:
                        server_machine_id = getattr(server, "machineIdentifier", None) or getattr(
                            server,
                            "clientIdentifier",
                            None
                        )
                    if server_machine_id == machine_id:
                        return True

                return False

            _add_to_whitelist(account)
            for plex_user in account.users():
                if _user_has_server_share(plex_user):
                    _add_to_whitelist(plex_user)

            base = f"{s.tautulli_url.rstrip('/')}/api/v2"
            resp = requests.get(
                base,
                params={'apikey': s.tautulli_api_key, 'cmd': 'get_users'},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get('response', {}).get('data', [])
            filtered_users = []

            for u in data:
                email = u.get('email')
                username = u.get('username')

                normalized_email = normalize_email(email) if email else None
                normalized_username = username.strip().lower() if isinstance(username, str) else None

                if not email:
                    continue

                if (normalized_email and normalized_email in whitelist) or (
                    normalized_username and normalized_username in whitelist
                ):
                    filtered_users.append(
                        {
                            'user_id': u.get('user_id'),
                            'username': username,
                            'email': email
                        }
                    )

            return filtered_users
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


def _user_has_subscription_fallback(
    email: str,
    alternate_email: Optional[str],
    show_key: Optional[str],
    show_guid: Optional[str],
    fallback_identity: Optional[str],
    show_title: Optional[str],
    show_year: Optional[int],
) -> Tuple[bool, List[UserPreferences]]:
    def _preference_matches_identity(
        preference: UserPreferences,
        *,
        normalized_identity: str,
        show_key_value: Optional[str],
        show_guid_value: Optional[str],
    ) -> bool:
        if show_key_value and preference.show_key and str(preference.show_key) == str(show_key_value):
            return True
        if show_guid_value and preference.show_guid and str(preference.show_guid) == str(show_guid_value):
            return True
        for stored_value in (preference.show_key, preference.show_guid):
            stored_identity = _normalize_stored_identity(stored_value)
            if stored_identity and stored_identity == normalized_identity:
                return True
        return False

    candidates = [candidate for candidate in (show_guid, show_key, fallback_identity) if candidate]

    emails = [email]
    if alternate_email and alternate_email not in emails:
        emails.append(alternate_email)

    preferences = []
    if candidates:
        preferences = (
            UserPreferences.query
            .filter(
                UserPreferences.email.in_(emails),
                or_(
                    UserPreferences.show_key.in_(candidates),
                    UserPreferences.show_guid.in_(candidates),
                ),
            )
            .all()
        )
    if preferences:
        active_preferences = [preference for preference in preferences if not preference.show_opt_out]
        if not active_preferences:
            current_app.logger.info(
                "Preference rows exist for user %s and show candidates %s but all are opted out",
                emails,
                candidates,
            )
            return False, []
        return True, active_preferences

    cleaned_title, _ = _extract_show_year_from_title(show_title)
    normalized_title = _normalize_title_for_match(cleaned_title or show_title)
    if normalized_title:
        title_preferences = (
            UserPreferences.query
            .filter(
                UserPreferences.email.in_(emails),
                UserPreferences.show_guid.startswith("title:"),
            )
            .all()
        )
        matched_preferences: List[UserPreferences] = []
        opted_out_matches = 0

        for preference in title_preferences:
            stored_title, stored_year = _parse_fallback_identity(preference.show_guid)
            stored_normalized = _normalize_title_for_match(stored_title)
            if not stored_normalized or stored_normalized != normalized_title:
                continue
            if show_year is not None and stored_year is not None and stored_year != show_year:
                continue
            if preference.show_opt_out:
                opted_out_matches += 1
            else:
                matched_preferences.append(preference)

        if matched_preferences:
            current_app.logger.info(
                "Title-only subscription fallback match used for %s (%s).",
                show_title or "Unknown",
                normalized_title,
            )
            return True, matched_preferences

        if opted_out_matches:
            current_app.logger.info(
                "Preference rows exist for user %s and normalized title %s but all are opted out",
                emails,
                normalized_title,
            )

    normalized_identity = fallback_identity or normalize_show_identity(show_title, show_year)
    if not normalized_identity:
        return False, []

    title_preferences = UserPreferences.query.filter(UserPreferences.email.in_(emails)).all()
    matched_preferences = []
    opted_out_matches = 0

    for preference in title_preferences:
        for stored_value in (preference.show_key, preference.show_guid):
            stored_identity = _normalize_stored_identity(stored_value)
            if stored_identity and stored_identity == normalized_identity:
                if preference.show_opt_out:
                    opted_out_matches += 1
                else:
                    matched_preferences.append(preference)
                break

    if matched_preferences:
        current_app.logger.info(
            "Title-based subscription fallback match used for %s (%s).",
            show_title or "Unknown",
            normalized_identity,
        )
        return True, matched_preferences

    if opted_out_matches:
        current_app.logger.info(
            "Preference rows exist for user %s and normalized title %s but all are opted out",
            emails,
            normalized_identity,
        )

    notification_matches_identity = False
    try:
        notification_rows = Notification.query.filter(Notification.email.in_(emails)).all()
        for notification in notification_rows:
            if show_key and notification.show_key and str(notification.show_key) == str(show_key):
                notification_matches_identity = True
                break
            if show_guid and notification.show_guid and str(notification.show_guid) == str(show_guid):
                notification_matches_identity = True
                break
            if notification.show_title:
                notif_title, notif_year = _extract_show_year_from_title(notification.show_title)
                effective_year = notif_year if notif_year is not None else show_year
                notif_identity = normalize_show_identity(notif_title or notification.show_title, effective_year)
                if notif_identity and notif_identity == normalized_identity:
                    notification_matches_identity = True
                    break
    except Exception as exc:
        current_app.logger.warning(
            "Unable to query notification history for fallback subscription check: %s",
            exc,
        )

    if notification_matches_identity:
        opt_out_preferences = UserPreferences.query.filter(
            UserPreferences.email.in_(emails),
            UserPreferences.show_opt_out.is_(True),
        ).all()
        opted_out = any(
            _preference_matches_identity(
                preference,
                normalized_identity=normalized_identity,
                show_key_value=show_key,
                show_guid_value=show_guid,
            )
            for preference in opt_out_preferences
        )
        if opted_out:
            current_app.logger.info(
                "Notification history matched %s but user %s has opted out.",
                normalized_identity,
                emails,
            )
            return False, []
        synthetic_preference = UserPreferences(
            email=emails[0],
            show_key=show_key,
            show_guid=show_guid or normalized_identity,
            show_opt_out=False,
        )
        current_app.logger.info(
            "Notification history subscription fallback match used for %s (%s).",
            show_title or "Unknown",
            normalized_identity,
        )
        return True, [synthetic_preference]
    return False, []


def _user_has_watched_show(
    s: Settings,
    user_id: int,
    grandparent_rating_key: Any,
    fallback_identity: Optional[str] = None,
) -> Tuple[bool, str]:
    def _coerce_percent(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                value = float(value)
            except ValueError:
                return None
        if isinstance(value, (int, float)):
            percent_value = float(value)
            if 0 <= percent_value <= 1:
                percent_value *= 100
            return percent_value
        return None

    def _extract_completion_percent(item: Dict[str, Any]) -> Optional[float]:
        for key in (
            "percent_complete",
            "progress_percent",
            "percent",
            "watched_percent",
            "percent_watched",
        ):
            percent_value = _coerce_percent(item.get(key))
            if percent_value is not None:
                return percent_value
        return None

    def _is_affirmative_watched(value: Any, completion_percent: Optional[float]) -> bool:
        if completion_percent is not None:
            return completion_percent >= TAUTULLI_WATCHED_PERCENT_THRESHOLD
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            percent_value = _coerce_percent(value)
            if percent_value is None:
                return False
            return percent_value >= TAUTULLI_WATCHED_PERCENT_THRESHOLD
        if isinstance(value, str):
            normalized = value.strip().lower()
            if not normalized:
                return False
            percent_value = _coerce_percent(normalized)
            if percent_value is not None:
                return percent_value >= TAUTULLI_WATCHED_PERCENT_THRESHOLD
            return normalized in AFFIRMATIVE_WATCHED_STATUSES
        return False

    try:
        base = f"{s.tautulli_url.rstrip('/')}/api/v2"
        # Tautulli's API caps the history "length" parameter at 1000 records.
        page_length = 1000
        start = 0
        grandparent_key_str = str(grandparent_rating_key) if grandparent_rating_key is not None else ""

        history_found = False
        while True:
            params = {
                'apikey': s.tautulli_api_key,
                'cmd': 'get_history',
                'user_id': user_id,
                'start': start,
                'length': page_length
            }
            if grandparent_rating_key is not None:
                params['grandparent_rating_key'] = grandparent_rating_key
            resp = requests.get(base, params=params, timeout=10)
            resp.raise_for_status()

            payload = resp.json().get('response', {}).get('data', {})
            history = payload.get('data') or []
            if history:
                history_found = True

            for item in history:
                watched_status = item.get('watched_status')
                completion_percent = _extract_completion_percent(item)
                gp_key = str(item.get('grandparent_rating_key'))
                if grandparent_rating_key is not None and gp_key == grandparent_key_str:
                    if _is_affirmative_watched(watched_status, completion_percent):
                        return True, "available"
                if fallback_identity:
                    item_identity = normalize_show_identity(
                        item.get('grandparent_title'),
                        item.get('grandparent_year') or item.get('year'),
                    )
                    if item_identity == fallback_identity and _is_affirmative_watched(
                        watched_status,
                        completion_percent,
                    ):
                        return True, "available"

            records_filtered = payload.get('recordsFiltered')
            if not history:
                break

            consumed = start + len(history)
            if isinstance(records_filtered, int) and consumed >= records_filtered:
                break

            start = consumed

        if history_found:
            return False, "available"
        return False, "empty"
    except Exception as e:
        current_app.logger.error(f"Error checking show history for user {user_id}: {e}")
        return False, "error"


def _send_email_with_retry(s: Settings, msg: MIMEMultipart, max_attempts: int = EMAIL_RETRY_ATTEMPTS) -> bool:
    """Send email with exponential backoff retry logic.

    Returns True if email was sent successfully, False otherwise.
    """
    redacted_to = redact_email(msg["To"])
    last_error = None
    for attempt in range(max_attempts):
        try:
            smtp = smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=30)
            smtp.starttls()
            smtp.login(s.smtp_user, s.smtp_pass)
            smtp.send_message(msg)
            smtp.quit()
            if attempt > 0:
                current_app.logger.info(
                    "Email to %s sent successfully on attempt %s",
                    redacted_to,
                    attempt + 1,
                )
            return True
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                wait_time = min(
                    EMAIL_RETRY_MIN_WAIT_SECONDS * (2 ** attempt),
                    EMAIL_RETRY_MAX_WAIT_SECONDS
                )
                current_app.logger.warning(
                    "Email send attempt %s/%s failed for %s: %s. Retrying in %ss...",
                    attempt + 1,
                    max_attempts,
                    redacted_to,
                    e,
                    wait_time,
                )
                time.sleep(wait_time)
            else:
                current_app.logger.error(
                    "Failed to send email to %s after %s attempts: %s",
                    redacted_to,
                    max_attempts,
                    e,
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
