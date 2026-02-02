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
from logging.handlers import RotatingFileHandler
from functools import lru_cache
from cachetools import TTLCache

from .logging_utils import TZFormatter
from .utils import normalize_email, email_to_filename, redact_email
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

from .config import Settings, UserPreferences, Notification, EpisodeFirstSeen, ShowIdentity, db
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


def _coerce_guid_values(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = [value]
    results: List[str] = []
    for item in items:
        if not item:
            continue
        if isinstance(item, dict):
            candidate = item.get("id") or item.get("guid") or item.get("key")
        else:
            candidate = (
                getattr(item, "id", None)
                or getattr(item, "guid", None)
                or getattr(item, "key", None)
                or item
            )
        if candidate:
            results.append(str(candidate))
    return results


def _dedupe_guid_list(values: List[str]) -> List[str]:
    seen: Set[str] = set()
    output: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _select_primary_guid(values: List[str]) -> Optional[str]:
    if not values:
        return None
    for prefix in ("plex://",):
        for value in values:
            if value.startswith(prefix):
                return value
    return values[0]


def _extract_show_guid(episode: Episode) -> List[str]:
    guid = getattr(episode, "grandparentGuid", None)
    return _dedupe_guid_list(_coerce_guid_values(guid))


def _get_show_guid_for_episode(episode: Episode) -> Optional[str]:
    return _select_primary_guid(_extract_show_guid(episode))


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
            if notif.tvdb_id:
                notified.add(f"tvdb://{notif.tvdb_id}|{season_episode}")
            if notif.tmdb_id:
                notified.add(f"tmdb://{notif.tmdb_id}|{season_episode}")
            if notif.imdb_id:
                notified.add(f"imdb://{notif.imdb_id}|{season_episode}")
            if notif.plex_guid:
                notified.add(f"{notif.plex_guid}|{season_episode}")
    except Exception as e:
        current_app.logger.warning(f"Could not query database for notifications: {e}")

    # Cache the result
    notification_cache[normalized_email] = notified.copy()
    return notified


def _coerce_plex_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if not value:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _notification_completeness_score(notification: Notification) -> int:
    identifiers = (
        notification.show_guid,
        notification.tvdb_id,
        notification.tmdb_id,
        notification.imdb_id,
        notification.plex_guid,
        notification.episode_key,
    )
    return sum(1 for value in identifiers if value)


def _notification_identity_label(
    *,
    show_guid: Optional[str],
    tvdb_id: Optional[str],
    tmdb_id: Optional[str],
    imdb_id: Optional[str],
    plex_guid: Optional[str],
    show_key: Optional[str] = None,
) -> Optional[str]:
    if show_guid:
        return f"guid:{show_guid}"
    if tvdb_id:
        return f"tvdb:{tvdb_id}"
    if tmdb_id:
        return f"tmdb:{tmdb_id}"
    if imdb_id:
        return f"imdb:{imdb_id}"
    if plex_guid:
        return f"plex:{plex_guid}"
    if show_key:
        return f"key:{show_key}"
    return None


def _notification_identity_filters(
    *,
    show_guid: Optional[str],
    tvdb_id: Optional[str],
    tmdb_id: Optional[str],
    imdb_id: Optional[str],
    plex_guid: Optional[str],
    show_key: Optional[str] = None,
    include_show_key_fallback: bool = True,
) -> list[Any]:
    filters: list[Any] = []
    if show_guid:
        filters.append(Notification.show_guid == show_guid)
    if tvdb_id:
        filters.append(Notification.tvdb_id == tvdb_id)
    if tmdb_id:
        filters.append(Notification.tmdb_id == tmdb_id)
    if imdb_id:
        filters.append(Notification.imdb_id == imdb_id)
    if plex_guid:
        filters.append(Notification.plex_guid == plex_guid)
    if not filters and include_show_key_fallback and show_key:
        filters.append(Notification.show_key == show_key)
    return filters


def _find_notification_conflict(
    *,
    email: str,
    season: int,
    episode: int,
    show_guid: Optional[str],
    tvdb_id: Optional[str],
    tmdb_id: Optional[str],
    imdb_id: Optional[str],
    plex_guid: Optional[str],
    show_key: Optional[str] = None,
    exclude_id: Optional[int] = None,
) -> Optional[Notification]:
    identity_filters = _notification_identity_filters(
        show_guid=show_guid,
        tvdb_id=tvdb_id,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        plex_guid=plex_guid,
        show_key=show_key,
    )
    if not identity_filters:
        return None
    query = Notification.query.filter(
        Notification.email == email,
        Notification.season == season,
        Notification.episode == episode,
        or_(*identity_filters),
    )
    if exclude_id is not None:
        query = query.filter(Notification.id != exclude_id)
    return query.first()


def _select_notification_to_keep(
    current: Notification,
    conflict: Notification,
) -> Tuple[Notification, str]:
    current_score = _notification_completeness_score(current)
    conflict_score = _notification_completeness_score(conflict)
    if current_score != conflict_score:
        keep = current if current_score > conflict_score else conflict
        reason = f"completeness {current_score} vs {conflict_score}"
        return keep, reason

    current_ts = _coerce_plex_datetime(current.timestamp) or datetime.min.replace(tzinfo=timezone.utc)
    conflict_ts = _coerce_plex_datetime(conflict.timestamp) or datetime.min.replace(tzinfo=timezone.utc)
    if current_ts != conflict_ts:
        keep = current if current_ts > conflict_ts else conflict
        reason = f"timestamp {current_ts.isoformat()} vs {conflict_ts.isoformat()}"
        return keep, reason

    keep = current if (current.id or 0) >= (conflict.id or 0) else conflict
    reason = "tie-breaker by id"
    return keep, reason


def _get_episode_availability_datetime(episode: Episode) -> Optional[datetime]:
    for attr in ("originallyAvailableAt", "availableAt"):
        candidate = getattr(episode, attr, None)
        if candidate:
            return _coerce_plex_datetime(candidate)
    return None


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


def _extract_show_guid_from_metadata(item: Any) -> List[str]:
    guid_values = []
    guid_values.extend(_coerce_guid_values(getattr(item, "guid", None)))
    guid_values.extend(_coerce_guid_values(getattr(item, "guids", None)))
    return _dedupe_guid_list(guid_values)


def _extract_external_show_ids(guids: List[str]) -> Dict[str, Optional[str]]:
    parsed = {
        "tvdb_id": None,
        "tmdb_id": None,
        "imdb_id": None,
        "plex_guid": None,
    }
    if not guids:
        return parsed
    for guid in guids:
        if not guid:
            continue
        lower_guid = guid.lower()
        if lower_guid.startswith("plex://") and not parsed["plex_guid"]:
            parsed["plex_guid"] = guid
        tvdb_match = re.search(r"(?:tvdb|thetvdb):///?(?P<id>\d+)", lower_guid)
        if tvdb_match and not parsed["tvdb_id"]:
            parsed["tvdb_id"] = tvdb_match.group("id")
        tmdb_match = re.search(r"(?:tmdb|themoviedb):///?(?P<id>\d+)", lower_guid)
        if tmdb_match and not parsed["tmdb_id"]:
            parsed["tmdb_id"] = tmdb_match.group("id")
        imdb_match = re.search(r"(?:imdb):///?(?P<id>tt\d+|\d+)", lower_guid)
        if imdb_match and not parsed["imdb_id"]:
            parsed["imdb_id"] = imdb_match.group("id")
    return parsed


def _build_show_fingerprint(
    title: Optional[str],
    year: Optional[int],
    leaf_count: Optional[int] = None,
    child_count: Optional[int] = None,
) -> Optional[str]:
    if not title:
        return None
    normalized = _normalize_title_for_match(title)
    if not normalized:
        return None
    parts = [normalized]
    if year:
        parts.append(str(year))
    if leaf_count is not None:
        parts.append(f"leaf:{leaf_count}")
    if child_count is not None:
        parts.append(f"child:{child_count}")
    return "|".join(parts) if parts else None


def _extract_show_counts(show: Any) -> tuple[Optional[int], Optional[int]]:
    if not show:
        return None, None
    return (
        getattr(show, "leafCount", None),
        getattr(show, "childCount", None),
    )


def _update_identity_from_show_metadata(
    app: Flask,
    show: Any,
    *,
    show_key_hint: Optional[str] = None,
    show_guid_hint: Optional[str] = None,
) -> None:
    if not show:
        return
    try:
        show_guids = _extract_show_guid_from_metadata(show)
        if show_guid_hint and show_guid_hint not in show_guids:
            show_guids.append(show_guid_hint)
        show_key_value = (
            str(getattr(show, "ratingKey", "") or "") or (str(show_key_hint) if show_key_hint else None)
        )
        show_guid_value = _select_primary_guid(show_guids) or show_guid_hint
        leaf_count, child_count = _extract_show_counts(show)
        _upsert_show_identity(
            show_guid=show_guid_value,
            show_key=show_key_value,
            show_guids=show_guids,
            title=getattr(show, "title", None),
            year=getattr(show, "year", None),
            plex_rating_key=show_key_value,
            leaf_count=leaf_count,
            child_count=child_count,
        )
    except Exception as exc:
        app.logger.warning(
            "Failed to update show identity from Plex metadata for %s: %s",
            getattr(show, "ratingKey", None) or show_key_hint or show_guid_hint or "unknown",
            exc,
        )


def _lookup_show_identity(
    *,
    show_guid: Optional[str],
    show_key: Optional[str],
) -> Optional[ShowIdentity]:
    if show_guid:
        match = ShowIdentity.query.filter(ShowIdentity.show_guid == show_guid).first()
        if match:
            return match
    if show_key:
        return ShowIdentity.query.filter(
            or_(
                ShowIdentity.show_key == show_key,
                ShowIdentity.plex_rating_key == show_key,
            )
        ).first()
    return None


def _find_identity_by_fingerprint(
    app: Flask,
    *,
    fingerprint: Optional[str],
    base_fingerprint: Optional[str],
    record_type: str,
    record_id: Optional[int],
) -> Optional[ShowIdentity]:
    if not fingerprint and not base_fingerprint:
        return None
    if fingerprint:
        matches = ShowIdentity.query.filter(ShowIdentity.fingerprint == fingerprint).all()
    else:
        like_pattern = f"{base_fingerprint}|%" if base_fingerprint else None
        matches = ShowIdentity.query.filter(
            or_(
                ShowIdentity.fingerprint == base_fingerprint,
                ShowIdentity.fingerprint.like(like_pattern),
            )
        ).all()
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        app.logger.info(
            "%s reconciliation skipped %s: fingerprint match '%s' returned %s identities.",
            record_type,
            record_id if record_id is not None else "unknown",
            fingerprint or base_fingerprint,
            len(matches),
        )
    return None


def _resolve_show_match(
    app: Flask,
    plex: PlexServer,
    tv_section: Any,
    *,
    show_guid: Optional[str],
    show_key: Optional[str],
    title: Optional[str],
    year: Optional[int],
    record_type: str,
    record_id: Optional[int],
    force_title_fallback: bool = False,
) -> Tuple[Any | None, str]:
    identity = _lookup_show_identity(show_guid=show_guid, show_key=show_key)
    stored_guids = [show_guid] if show_guid else []
    external_ids = _extract_external_show_ids(stored_guids)
    for key in ("tvdb_id", "tmdb_id", "imdb_id"):
        if identity and getattr(identity, key, None) and not external_ids.get(key):
            external_ids[key] = getattr(identity, key)
    external_guid_candidates: list[tuple[str, str]] = []
    if external_ids.get("tvdb_id"):
        external_guid_candidates.append(("tvdb", f"tvdb://{external_ids['tvdb_id']}"))
    if external_ids.get("tmdb_id"):
        external_guid_candidates.append(("tmdb", f"tmdb://{external_ids['tmdb_id']}"))
    if external_ids.get("imdb_id"):
        external_guid_candidates.append(("imdb", f"imdb://{external_ids['imdb_id']}"))
    failure_reason = "no_match_title_fallback"
    for provider, guid_value in external_guid_candidates:
        matched_show = _fetch_show_by_guid(app, plex, tv_section, guid_value)
        if matched_show:
            resolved_key = getattr(matched_show, "ratingKey", None)
            resolved_title = getattr(matched_show, "title", None)
            resolved_details = []
            if resolved_key:
                resolved_details.append(f"plex_key={resolved_key}")
            if resolved_title:
                resolved_details.append(f"plex_title='{resolved_title}'")
            resolved_suffix = f" ({', '.join(resolved_details)})" if resolved_details else ""
            app.logger.info(
                "%s reconciliation external id match for %s: provider=%s guid=%s%s",
                record_type,
                record_id if record_id is not None else "unknown",
                provider,
                guid_value,
                resolved_suffix,
            )
            return matched_show, f"external_id_match:{provider}"
        failure_reason = "no_match_external_ids"

    plex_guid = None
    if show_guid and show_guid.startswith("plex://"):
        plex_guid = show_guid
    elif identity and identity.plex_guid:
        plex_guid = identity.plex_guid
    if plex_guid:
        matched_show = _fetch_show_by_guid(app, plex, tv_section, plex_guid)
        if matched_show:
            return matched_show, "plex_guid_match"
        failure_reason = "no_match_plex_guid"

    fingerprint = identity.fingerprint if identity and identity.fingerprint else None
    base_fingerprint = None if fingerprint else _build_show_fingerprint(title, year)
    identity_match = _find_identity_by_fingerprint(
        app,
        fingerprint=fingerprint,
        base_fingerprint=base_fingerprint,
        record_type=record_type,
        record_id=record_id,
    )
    if identity_match:
        identity_key = identity_match.plex_rating_key or identity_match.show_key
        if identity_key:
            matched_show = _fetch_show_by_key(app, plex, tv_section, identity_key)
            if matched_show:
                return matched_show, "fingerprint_match"
        identity_guid = identity_match.plex_guid or identity_match.show_guid
        if identity_guid:
            matched_show = _fetch_show_by_guid(app, plex, tv_section, identity_guid)
            if matched_show:
                return matched_show, "fingerprint_match"
    if fingerprint or base_fingerprint:
        failure_reason = "no_match_fingerprint"

    if force_title_fallback and title:
        matched_show = _search_show_by_title(app, tv_section, title, year)
        if matched_show:
            app.logger.info(
                "%s reconciliation recovered show via title fallback for %s: '%s'%s.",
                record_type,
                record_id if record_id is not None else "unknown",
                title,
                f" ({year})" if year else "",
            )
            return matched_show, "title_fallback_match"
        failure_reason = "no_match_title_fallback"

    return None, failure_reason


def _upsert_show_identity(
    *,
    show_guid: Optional[str],
    show_key: Optional[str],
    show_guids: List[str],
    title: Optional[str],
    year: Optional[int],
    plex_rating_key: Optional[str],
    leaf_count: Optional[int] = None,
    child_count: Optional[int] = None,
) -> bool:
    if not any([show_guid, show_key, show_guids, plex_rating_key]):
        return False

    ids = _extract_external_show_ids(show_guids)
    plex_guid = ids.get("plex_guid")
    fingerprint = _build_show_fingerprint(title, year, leaf_count, child_count)

    filters = []
    if show_guid:
        filters.append(ShowIdentity.show_guid == show_guid)
    if show_key:
        filters.append(ShowIdentity.show_key == show_key)
    if plex_rating_key:
        filters.append(ShowIdentity.plex_rating_key == plex_rating_key)
    if plex_guid:
        filters.append(ShowIdentity.plex_guid == plex_guid)
    if ids.get("tvdb_id"):
        filters.append(ShowIdentity.tvdb_id == ids["tvdb_id"])
    if ids.get("tmdb_id"):
        filters.append(ShowIdentity.tmdb_id == ids["tmdb_id"])
    if ids.get("imdb_id"):
        filters.append(ShowIdentity.imdb_id == ids["imdb_id"])

    identity = None
    if filters:
        identity = ShowIdentity.query.filter(or_(*filters)).first()
    if not identity:
        identity = ShowIdentity(show_guid=show_guid, show_key=show_key)

    changed = False

    def _set_attr(attr: str, value: Optional[str | int]) -> None:
        nonlocal changed
        if value is None:
            return
        if getattr(identity, attr) != value:
            setattr(identity, attr, value)
            changed = True

    _set_attr("show_guid", show_guid)
    _set_attr("show_key", show_key)
    _set_attr("tvdb_id", ids.get("tvdb_id"))
    _set_attr("tmdb_id", ids.get("tmdb_id"))
    _set_attr("imdb_id", ids.get("imdb_id"))
    _set_attr("plex_guid", plex_guid)
    _set_attr("plex_rating_key", plex_rating_key)
    _set_attr("title", title)
    if year is not None:
        _set_attr("year", year)
    _set_attr("fingerprint", fingerprint)

    if changed or identity.id is None:
        db.session.add(identity)
        return True
    return False


def _fetch_show_by_key(
    app: Flask,
    plex: PlexServer,
    tv_section: Any,
    show_key_value: str,
) -> Any:
    if not show_key_value:
        return None
    try:
        show = tv_section.get(show_key_value)
        _update_identity_from_show_metadata(app, show, show_key_hint=show_key_value)
        return show
    except Exception:
        try:
            fetch_path = (
                show_key_value
                if "/library/metadata/" in show_key_value
                else f"/library/metadata/{show_key_value}"
            )
            show = plex.fetchItem(fetch_path)
            _update_identity_from_show_metadata(app, show, show_key_hint=show_key_value)
            return show
        except Exception as exc:
            app.logger.warning(
                "Reconciliation failed to fetch show metadata for key '%s': %s",
                show_key_value,
                exc,
            )
            return None


def _fetch_show_by_guid(
    app: Flask,
    plex: PlexServer,
    tv_section: Any,
    show_guid_value: str,
) -> Any:
    if not show_guid_value:
        return None
    guid_value = str(show_guid_value)
    try:
        show = plex.fetchItem(guid_value)
        _update_identity_from_show_metadata(app, show, show_guid_hint=guid_value)
        return show
    except Exception:
        pass
    try:
        search_results = tv_section.search(guid=guid_value, libtype="show")
    except Exception as exc:
        app.logger.warning(
            "Reconciliation failed to search show metadata for guid '%s': %s",
            guid_value,
            exc,
        )
        return None
    if not search_results:
        return None
    show = search_results[0]
    _update_identity_from_show_metadata(app, show, show_guid_hint=guid_value)
    return show


def _search_show_by_title(
    app: Flask,
    tv_section: Any,
    search_title: str | None,
    search_year: Optional[int],
) -> Any:
    if not search_title:
        return None
    try:
        if search_year:
            search_results = tv_section.search(title=search_title, year=search_year, libtype="show")
        else:
            search_results = tv_section.search(title=search_title, libtype="show")
    except Exception as exc:
        app.logger.warning(f"Reconciliation search failed for '{search_title}': {exc}")
        return None

    if not search_results:
        return None

    title_key = _normalize_title_for_match(search_title)
    for show in search_results:
        if search_year and getattr(show, "year", None) == search_year:
            _update_identity_from_show_metadata(app, show)
            return show
        if _normalize_title_for_match(getattr(show, "title", "")) == title_key:
            _update_identity_from_show_metadata(app, show)
            return show
    show = search_results[0]
    _update_identity_from_show_metadata(app, show)
    return show


def _log_reconciliation_mismatch(
    app: Flask,
    *,
    record_type: str,
    record_id: Optional[int],
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
        "%s reconciliation mismatch detected for %s %s (%s): %s",
        record_type,
        record_type.lower(),
        record_id if record_id is not None else "unknown",
        source,
        change_summary,
    )


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
        empty_opt_out_preferences = UserPreferences.query.filter(
            UserPreferences.show_key.is_(None),
            UserPreferences.show_guid.is_(None),
            UserPreferences.show_opt_out.is_(True),
        ).all()
        preferences = UserPreferences.query.filter(
            or_(UserPreferences.show_key.isnot(None), UserPreferences.show_guid.isnot(None))
        ).all()
        notification_entries_by_email: dict[str, list[dict[str, Any]]] = {}
        if empty_opt_out_preferences:
            notification_identity_rows = (
                db.session.query(
                    Notification.email,
                    Notification.show_key,
                    Notification.show_guid,
                    Notification.tvdb_id,
                    Notification.tmdb_id,
                    Notification.imdb_id,
                    Notification.plex_guid,
                    Notification.show_title,
                )
                .distinct()
                .all()
            )
            seen_identity_keys: dict[str, set[tuple[Optional[str], Optional[str], Optional[str]]]] = {}
            for (
                email,
                show_key,
                show_guid,
                tvdb_id,
                tmdb_id,
                imdb_id,
                plex_guid,
                show_title,
            ) in notification_identity_rows:
                normalized_email = normalize_email(email)
                title, year = _extract_show_year_from_title(show_title)
                identity_label = _notification_identity_label(
                    show_guid=str(show_guid) if show_guid else None,
                    tvdb_id=str(tvdb_id) if tvdb_id else None,
                    tmdb_id=str(tmdb_id) if tmdb_id else None,
                    imdb_id=str(imdb_id) if imdb_id else None,
                    plex_guid=str(plex_guid) if plex_guid else None,
                    show_key=str(show_key) if show_key else None,
                )
                identity_key = (
                    identity_label,
                    str(show_guid) if show_guid else None,
                    str(show_key) if show_key else None,
                )
                if normalized_email not in seen_identity_keys:
                    seen_identity_keys[normalized_email] = set()
                if identity_key in seen_identity_keys[normalized_email]:
                    continue
                seen_identity_keys[normalized_email].add(identity_key)
                notification_entries_by_email.setdefault(normalized_email, []).append(
                    {
                        "show_guid": str(show_guid) if show_guid else None,
                        "show_key": str(show_key) if show_key else None,
                        "title": title or show_title,
                        "year": year,
                    }
                )

        show_groups: dict[str, dict[str, Any]] = {}
        guid_index: dict[str, str] = {}
        key_index: dict[str, str] = {}

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

            if not group_ids:
                group_id = f"group-{len(show_groups) + 1}"
                show_groups[group_id] = {
                    "show_guid": None,
                    "show_key": None,
                    "title": None,
                    "year": None,
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
                group.setdefault("show_guid", guid_value)
                guid_index[guid_value] = group_id
            if key_value:
                group["match_keys"].add(key_value)
                group.setdefault("show_key", key_value)
                key_index[key_value] = group_id
            if title and not group.get("title"):
                group["title"] = title
                group["year"] = year

            return group

        for show_key, show_guid, show_title in notification_rows:
            title = None
            year = None
            if show_title:
                title, year = _extract_show_year_from_title(show_title)
            _ensure_group(
                show_guid=str(show_guid) if show_guid else None,
                show_key=str(show_key) if show_key else None,
                title=title,
                year=year,
            )

        for pref in preferences:
            pref_guid = str(pref.show_guid) if pref.show_guid else None
            pref_key = str(pref.show_key) if pref.show_key else None
            if pref_guid and pref_guid.startswith("title:"):
                pref_guid = None
            group = _ensure_group(
                show_guid=pref_guid,
                show_key=pref_key,
                title=None,
                year=None,
            )
            if pref.id is not None:
                group["prefs"].setdefault(pref.id, pref)

        if empty_opt_out_preferences and notification_entries_by_email:
            prefs_by_email: dict[str, list[UserPreferences]] = {}
            for pref in empty_opt_out_preferences:
                normalized_email = normalize_email(pref.email)
                prefs_by_email.setdefault(normalized_email, []).append(pref)

            existing_identities: dict[str, set[str]] = {}
            for pref in preferences:
                normalized_email = normalize_email(pref.email)
                identity_set = existing_identities.setdefault(normalized_email, set())
                for value in (pref.show_key, pref.show_guid):
                    if value:
                        identity_set.add(str(value))

            for email, empty_prefs in prefs_by_email.items():
                notification_entries = notification_entries_by_email.get(email, [])
                if not notification_entries:
                    continue
                known_identities = existing_identities.get(email, set())
                unmatched_entries = [
                    entry
                    for entry in notification_entries
                    if not any(
                        identifier and identifier in known_identities
                        for identifier in (
                            entry.get("show_guid"),
                            entry.get("show_key"),
                        )
                    )
                ]
                if not unmatched_entries:
                    continue
                if len(unmatched_entries) != len(empty_prefs):
                    app.logger.info(
                        "Preference reconciliation skipped %s empty opt-out rows for %s due to %s unmatched shows.",
                        len(empty_prefs),
                        email,
                        len(unmatched_entries),
                    )
                    continue
                sorted_entries = sorted(
                    unmatched_entries,
                    key=lambda entry: (
                        _normalize_title_for_match(entry.get("title")),
                    ),
                )
                sorted_prefs = sorted(empty_prefs, key=lambda pref: pref.id or 0)
                for pref, entry in zip(sorted_prefs, sorted_entries):
                    group = _ensure_group(
                        show_guid=entry.get("show_guid"),
                        show_key=entry.get("show_key"),
                        title=entry.get("title"),
                        year=entry.get("year"),
                    )
                    if pref.id is not None:
                        group["prefs"].setdefault(pref.id, pref)

        updated_count = 0
        scanned_count = 0
        pending_updates = 0
        guid_only_corrected = 0
        guid_only_unresolved = 0
        batch_size = 50

        for group in show_groups.values():
            title = group.get("title")
            year = group.get("year")
            show_key = group.get("show_key")
            show_guid = group.get("show_guid")

            if not show_key and not title and not show_guid:
                continue

            scanned_count += 1

            matched_show = None
            guid_only_prefs: list[tuple[UserPreferences, str]] = []
            for pref in group["prefs"].values():
                stored_key = str(pref.show_key) if pref.show_key else None
                stored_guid = str(pref.show_guid) if pref.show_guid else None
                if stored_guid and stored_guid.startswith("title:"):
                    stored_guid = None
                if stored_guid and not stored_key:
                    guid_only_prefs.append((pref, stored_guid))

            matched_show, match_detail = _resolve_show_match(
                app,
                plex,
                tv_section,
                show_guid=show_guid,
                show_key=show_key,
                title=title,
                year=year,
                record_type="Preference",
                record_id=None,
                force_title_fallback=True,
            )

            if not matched_show:
                app.logger.info(
                    "Preference reconciliation match summary: show_key=%s, show_guid=%s, title=%s, year=%s, matched=no, detail=%s.",
                    show_key or "None",
                    show_guid or "None",
                    title or "None",
                    year if year is not None else "None",
                    match_detail,
                )
                for pref, stored_guid in guid_only_prefs:
                    app.logger.info(
                        "Preference reconciliation unable to resolve GUID-only preference %s (%s).",
                        pref.id if pref.id is not None else "unknown",
                        stored_guid,
                    )
                    guid_only_unresolved += 1
                continue
            app.logger.info(
                "Preference reconciliation match summary: show_key=%s, show_guid=%s, title=%s, year=%s, matched=yes, detail=%s.",
                show_key or "None",
                show_guid or "None",
                title or "None",
                year if year is not None else "None",
                match_detail,
            )

            new_show_key = str(getattr(matched_show, "ratingKey", "") or "") or None
            show_guids = _extract_show_guid_from_metadata(matched_show)
            new_show_guid = _select_primary_guid(show_guids)
            matched_title = getattr(matched_show, "title", None) or title
            matched_year = getattr(matched_show, "year", None) or year
            leaf_count, child_count = _extract_show_counts(matched_show)
            identity_updated = _upsert_show_identity(
                show_guid=new_show_guid,
                show_key=new_show_key,
                show_guids=show_guids,
                title=matched_title,
                year=matched_year,
                plex_rating_key=new_show_key,
                leaf_count=leaf_count,
                child_count=child_count,
            )
            if identity_updated:
                pending_updates += 1

            for pref, stored_guid in guid_only_prefs:
                app.logger.info(
                    "Preference reconciliation corrected GUID-only preference %s: %s -> %s (key %s).",
                    pref.id if pref.id is not None else "unknown",
                    stored_guid,
                    new_show_guid or stored_guid,
                    new_show_key or "None",
                )
                guid_only_corrected += 1

            for pref in group["prefs"].values():
                stored_key = str(pref.show_key) if pref.show_key else None
                stored_guid = str(pref.show_guid) if pref.show_guid else None
                if stored_guid and stored_guid.startswith("title:"):
                    stored_guid = None
                changes = {}
                if new_show_key and stored_key and stored_key != new_show_key:
                    changes["show_key"] = (stored_key, new_show_key)
                if new_show_guid and stored_guid and stored_guid != new_show_guid:
                    changes["show_guid"] = (stored_guid, new_show_guid)
                if changes:
                    source = "show_key" if stored_key else "show_guid"
                    _log_reconciliation_mismatch(
                        app,
                        record_type="Preference",
                        record_id=pref.id,
                        source=source,
                        changes=changes,
                    )
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
        app.logger.info(
            "Preference reconciliation (%s) resolved %s GUID-only preferences; %s remain unresolved.",
            run_reason,
            guid_only_corrected,
            guid_only_unresolved,
        )


def reconcile_notifications(
    app: Flask,
    *,
    run_reason: str = "startup",
) -> None:
    with app.app_context():
        s = Settings.query.first()
        if not s or not s.plex_url or not s.plex_token or s.plex_token == "placeholder":
            app.logger.info("Notification reconciliation skipped: Plex settings not configured.")
            return

        try:
            plex = PlexServer(s.plex_url, s.plex_token)
            tv_section = plex.library.section("TV Shows")
        except Exception as exc:
            app.logger.warning(f"Notification reconciliation skipped: unable to connect to Plex ({exc}).")
            return

        notifications = Notification.query.all()

        updated_count = 0
        scanned_count = 0
        mismatch_count = 0
        missing_identifier_corrected = 0
        missing_identifier_skipped = 0
        pending_updates = 0
        batch_size = 100

        for notif in notifications:
            stored_key = str(notif.show_key) if notif.show_key else None
            stored_guid = str(notif.show_guid) if notif.show_guid else None
            if stored_guid and stored_guid.startswith("title:"):
                stored_guid = None
            with db.session.no_autoflush:
                existing_conflict = _find_notification_conflict(
                    email=notif.email,
                    season=notif.season,
                    episode=notif.episode,
                    show_guid=stored_guid,
                    tvdb_id=notif.tvdb_id,
                    tmdb_id=notif.tmdb_id,
                    imdb_id=notif.imdb_id,
                    plex_guid=notif.plex_guid,
                    show_key=stored_key,
                    exclude_id=notif.id,
                )

            if existing_conflict:
                app.logger.info(
                    "Notification reconciliation skipped notification %s: "
                    "duplicate record exists for email=%s show_key=%s season=%s episode=%s.",
                    notif.id if notif.id is not None else "unknown",
                    notif.email,
                    stored_key,
                    notif.season,
                    notif.episode,
                )
                continue

            if not stored_key and not stored_guid:
                title, year = _extract_show_year_from_title(notif.show_title)
                search_title = title or notif.show_title
                if not search_title:
                    missing_identifier_skipped += 1
                    app.logger.info(
                        "Notification reconciliation skipped notification %s: missing show title for recovery.",
                        notif.id if notif.id is not None else "unknown",
                    )
                    continue
                matched_show, match_reason = _resolve_show_match(
                    app,
                    plex,
                    tv_section,
                    show_guid=None,
                    show_key=None,
                    title=search_title,
                    year=year,
                    record_type="Notification",
                    record_id=notif.id,
                    force_title_fallback=True,
                )
                if not matched_show:
                    missing_identifier_skipped += 1
                    app.logger.info(
                        "Notification reconciliation skipped notification %s: no identity match for '%s'%s (reason=%s).",
                        notif.id if notif.id is not None else "unknown",
                        search_title,
                        f" ({year})" if year else "",
                        match_reason,
                    )
                    continue
                new_show_key = str(getattr(matched_show, "ratingKey", "") or "") or None
                show_guids = _extract_show_guid_from_metadata(matched_show)
                new_show_guid = _select_primary_guid(show_guids)
                external_ids = _extract_external_show_ids(show_guids)
                leaf_count, child_count = _extract_show_counts(matched_show)
                identity_updated = _upsert_show_identity(
                    show_guid=new_show_guid,
                    show_key=new_show_key,
                    show_guids=show_guids,
                    title=getattr(matched_show, "title", None) or search_title,
                    year=getattr(matched_show, "year", None) or year,
                    plex_rating_key=new_show_key,
                    leaf_count=leaf_count,
                    child_count=child_count,
                )
                if identity_updated:
                    pending_updates += 1
                if not new_show_key and not new_show_guid:
                    missing_identifier_skipped += 1
                    app.logger.info(
                        "Notification reconciliation skipped notification %s: Plex match for '%s'%s missing identifiers.",
                        notif.id if notif.id is not None else "unknown",
                        search_title,
                        f" ({year})" if year else "",
                    )
                    continue
                if new_show_key and notif.show_key != new_show_key:
                    with db.session.no_autoflush:
                        conflict = _find_notification_conflict(
                            email=notif.email,
                            season=notif.season,
                            episode=notif.episode,
                            show_guid=new_show_guid,
                            tvdb_id=external_ids.get("tvdb_id"),
                            tmdb_id=external_ids.get("tmdb_id"),
                            imdb_id=external_ids.get("imdb_id"),
                            plex_guid=external_ids.get("plex_guid"),
                            show_key=new_show_key,
                            exclude_id=notif.id,
                        )
                    if conflict:
                        keep, reason = _select_notification_to_keep(notif, conflict)
                        if keep is conflict:
                            missing_identifier_skipped += 1
                            app.logger.info(
                                "Notification reconciliation deleted notification %s in favor of %s: "
                                "target show_key=%s conflict for email=%s season=%s episode=%s (reason=%s).",
                                notif.id if notif.id is not None else "unknown",
                                conflict.id if conflict.id is not None else "unknown",
                                new_show_key,
                                notif.email,
                                notif.season,
                                notif.episode,
                                reason,
                            )
                            db.session.delete(notif)
                            pending_updates += 1
                            continue
                        app.logger.info(
                            "Notification reconciliation deleted conflicting notification %s: "
                            "keeping notification %s for target show_key=%s email=%s season=%s episode=%s (reason=%s).",
                            conflict.id if conflict.id is not None else "unknown",
                            notif.id if notif.id is not None else "unknown",
                            new_show_key,
                            notif.email,
                            notif.season,
                            notif.episode,
                            reason,
                        )
                        db.session.delete(conflict)
                        pending_updates += 1
                    notif.show_key = new_show_key
                if new_show_guid and notif.show_guid != new_show_guid:
                    notif.show_guid = new_show_guid
                if external_ids.get("tvdb_id") and notif.tvdb_id != external_ids.get("tvdb_id"):
                    notif.tvdb_id = external_ids.get("tvdb_id")
                if external_ids.get("tmdb_id") and notif.tmdb_id != external_ids.get("tmdb_id"):
                    notif.tmdb_id = external_ids.get("tmdb_id")
                if external_ids.get("imdb_id") and notif.imdb_id != external_ids.get("imdb_id"):
                    notif.imdb_id = external_ids.get("imdb_id")
                if external_ids.get("plex_guid") and notif.plex_guid != external_ids.get("plex_guid"):
                    notif.plex_guid = external_ids.get("plex_guid")
                if db.session.is_modified(notif, include_collections=False):
                    updated_count += 1
                    missing_identifier_corrected += 1
                    app.logger.info(
                        "Notification reconciliation recovered identifiers for notification %s: "
                        "title '%s'%s -> key %s, guid %s (reason=%s).",
                        notif.id if notif.id is not None else "unknown",
                        search_title,
                        f" ({year})" if year else "",
                        new_show_key or "None",
                        new_show_guid or "None",
                        match_reason,
                    )
                    pending_updates += 1
                    if pending_updates >= batch_size:
                        try:
                            db.session.commit()
                            pending_updates = 0
                        except Exception as exc:
                            app.logger.warning(f"Notification reconciliation failed to commit updates: {exc}")
                            db.session.rollback()
                            return
                continue

            scanned_count += 1
            title, year = _extract_show_year_from_title(notif.show_title)
            with db.session.no_autoflush:
                matched_show, failure_reason = _resolve_show_match(
                    app,
                    plex,
                    tv_section,
                    show_guid=stored_guid,
                    show_key=stored_key,
                    title=title or notif.show_title,
                    year=year,
                    record_type="Notification",
                    record_id=notif.id,
                    force_title_fallback=True,
                )

            if not matched_show:
                app.logger.info(
                    "Notification reconciliation could not resolve show match for notification %s "
                    "(record_type=\"Notification\"): stored_key=%s stored_guid=%s title='%s'%s reason=%s.",
                    notif.id if notif.id is not None else "unknown",
                    stored_key or "None",
                    stored_guid or "None",
                    title or notif.show_title or "",
                    f" ({year})" if year else "",
                    failure_reason,
                )
                continue

            new_show_key = str(getattr(matched_show, "ratingKey", "") or "") or None
            show_guids = _extract_show_guid_from_metadata(matched_show)
            new_show_guid = _select_primary_guid(show_guids)
            external_ids = _extract_external_show_ids(show_guids)
            leaf_count, child_count = _extract_show_counts(matched_show)
            identity_updated = _upsert_show_identity(
                show_guid=new_show_guid,
                show_key=new_show_key,
                show_guids=show_guids,
                title=getattr(matched_show, "title", None) or notif.show_title,
                year=getattr(matched_show, "year", None),
                plex_rating_key=new_show_key,
                leaf_count=leaf_count,
                child_count=child_count,
            )
            if identity_updated:
                pending_updates += 1

            changes = {}
            if new_show_key and stored_key and stored_key != new_show_key:
                changes["show_key"] = (stored_key, new_show_key)
            if new_show_guid and stored_guid and stored_guid != new_show_guid:
                changes["show_guid"] = (stored_guid, new_show_guid)
            if changes:
                mismatch_count += 1
                source = "show_key" if stored_key else "show_guid"
                _log_reconciliation_mismatch(
                    app,
                    record_type="Notification",
                    record_id=notif.id,
                    source=source,
                    changes=changes,
                )

            if new_show_key and notif.show_key != new_show_key:
                with db.session.no_autoflush:
                    conflict = _find_notification_conflict(
                        email=notif.email,
                        season=notif.season,
                        episode=notif.episode,
                        show_guid=new_show_guid,
                        tvdb_id=external_ids.get("tvdb_id"),
                        tmdb_id=external_ids.get("tmdb_id"),
                        imdb_id=external_ids.get("imdb_id"),
                        plex_guid=external_ids.get("plex_guid"),
                        show_key=new_show_key,
                        exclude_id=notif.id,
                    )
                if conflict:
                    keep, reason = _select_notification_to_keep(notif, conflict)
                    if keep is conflict:
                        app.logger.info(
                            "Notification reconciliation deleted notification %s in favor of %s: "
                            "target show_key=%s conflict for email=%s season=%s episode=%s (reason=%s).",
                            notif.id if notif.id is not None else "unknown",
                            conflict.id if conflict.id is not None else "unknown",
                            new_show_key,
                            notif.email,
                            notif.season,
                            notif.episode,
                            reason,
                        )
                        db.session.delete(notif)
                        pending_updates += 1
                        continue
                    app.logger.info(
                        "Notification reconciliation deleted conflicting notification %s: "
                        "keeping notification %s for target show_key=%s email=%s season=%s episode=%s (reason=%s).",
                        conflict.id if conflict.id is not None else "unknown",
                        notif.id if notif.id is not None else "unknown",
                        new_show_key,
                        notif.email,
                        notif.season,
                        notif.episode,
                        reason,
                    )
                    db.session.delete(conflict)
                    pending_updates += 1
                notif.show_key = new_show_key
            if new_show_guid and notif.show_guid != new_show_guid:
                notif.show_guid = new_show_guid
            if external_ids.get("tvdb_id") and notif.tvdb_id != external_ids.get("tvdb_id"):
                notif.tvdb_id = external_ids.get("tvdb_id")
            if external_ids.get("tmdb_id") and notif.tmdb_id != external_ids.get("tmdb_id"):
                notif.tmdb_id = external_ids.get("tmdb_id")
            if external_ids.get("imdb_id") and notif.imdb_id != external_ids.get("imdb_id"):
                notif.imdb_id = external_ids.get("imdb_id")
            if external_ids.get("plex_guid") and notif.plex_guid != external_ids.get("plex_guid"):
                notif.plex_guid = external_ids.get("plex_guid")

            if db.session.is_modified(notif, include_collections=False):
                updated_count += 1
                pending_updates += 1
                if pending_updates >= batch_size:
                    try:
                        db.session.commit()
                        pending_updates = 0
                    except Exception as exc:
                        app.logger.warning(f"Notification reconciliation failed to commit updates: {exc}")
                        db.session.rollback()
                        return

        if pending_updates:
            try:
                db.session.commit()
            except Exception as exc:
                app.logger.warning(f"Notification reconciliation failed to commit updates: {exc}")
                db.session.rollback()
                return

        app.logger.info(
            "Notification reconciliation (%s) updated %s notifications with %s mismatches across %s scanned rows. "
            "Repaired %s missing-identifier notifications; skipped %s.",
            run_reason,
            updated_count,
            mismatch_count,
            scanned_count,
            missing_identifier_corrected,
            missing_identifier_skipped,
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
                existing_first_seen_at = None
                if rating_key:
                    existing_first_seen_at = existing_first_seen.get(rating_key)
                    if existing_first_seen_at:
                        first_seen_at = existing_first_seen_at
                    else:
                        first_seen_at = now_dt
                        new_first_seen_rows.append(
                            EpisodeFirstSeen(
                                episode_key=rating_key,
                                first_seen_at=first_seen_at,
                            )
                        )

                first_seen_recent = (
                    existing_first_seen_at is not None
                    and first_seen_at is not None
                    and first_seen_at >= cutoff_dt
                )
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
                raw_show_guids = _extract_show_guid(ep)
                guid_candidates = list(raw_show_guids)

                if not show_key_str and not raw_show_guids:
                    continue

                has_recent_notification_for_show = any(
                    candidate
                    for candidate in ([show_key_str] + guid_candidates)
                    if candidate and candidate in (recent_show_keys | recent_show_guids)
                )
                show_guid = _get_show_guid_for_episode(ep)
                if show_guid and show_guid not in guid_candidates:
                    guid_candidates.append(show_guid)

                # 🔒 Check per-show opt-out
                show_pref = None
                for guid_candidate in guid_candidates:
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

                has_watched_show, _ = _user_has_watched_show(s, uid, show_key)
                is_subscribed, subscription_reason = _user_is_subscribed_for_show(
                    email=canon,
                    alternate_email=user_email,
                    show_key=show_key_str,
                    show_guid=show_guid,
                    guid_candidates=guid_candidates,
                    season=ep.parentIndex,
                    episode=ep.index,
                    recent_show_keys=recent_show_keys,
                    recent_show_guids=recent_show_guids,
                )
                if not has_watched_show and not is_subscribed:
                    continue
                if has_watched_show:
                    current_app.logger.info(
                        "Eligibility for %s on %s granted via watch history.",
                        redacted_email,
                        show_title or show_key_str or show_guid or "unknown show",
                    )
                else:
                    current_app.logger.info(
                        "Eligibility for %s on %s granted via %s.",
                        redacted_email,
                        show_title or show_key_str or show_guid or "unknown show",
                        subscription_reason or "prior notification/subscription",
                    )
                if show_pref and show_guid and show_pref.show_guid != show_guid:
                    show_pref.show_guid = show_guid
                    needs_commit = True
                if _user_has_history(s, uid, ep.ratingKey):
                    continue

                season_episode = f"S{ep.parentIndex}E{ep.index}"
                candidate_ids: List[str] = []
                if ep.ratingKey:
                    candidate_ids.append(str(ep.ratingKey))
                for guid_candidate in guid_candidates:
                    candidate_ids.append(f"{guid_candidate}|{season_episode}")
                if show_key_str:
                    candidate_ids.append(f"{show_key_str}|{season_episode}")
                if not candidate_ids:
                    continue
                if any(candidate in recent_notified for candidate in candidate_ids):
                    continue

                watchable.append({
                    "episode": ep,
                    "show_guid": show_guid,
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
        show_key = str(episode.grandparentRatingKey) if episode.grandparentRatingKey is not None else None
        show_guids = _extract_show_guid(episode)
        if show_guid_override and show_guid_override not in show_guids:
            show_guids.append(show_guid_override)
        show_guid = show_guid_override or _select_primary_guid(show_guids)
        show_title = episode.grandparentTitle
        normalized_title, title_year = _extract_show_year_from_title(show_title)
        identity_title = normalized_title or show_title
        identity_year = (
            getattr(episode, "grandparentYear", None)
            or getattr(episode, "year", None)
            or title_year
        )
        _upsert_show_identity(
            show_guid=show_guid,
            show_key=show_key,
            show_guids=show_guids,
            title=identity_title,
            year=identity_year,
            plex_rating_key=show_key,
        )
        external_ids = _extract_external_show_ids(show_guids)
        identity = _lookup_show_identity(show_guid=show_guid, show_key=show_key)
        if identity:
            for key in ("tvdb_id", "tmdb_id", "imdb_id", "plex_guid"):
                if not external_ids.get(key) and getattr(identity, key, None):
                    external_ids[key] = getattr(identity, key)
        existing = _find_notification_conflict(
            email=normalized_email,
            season=episode.parentIndex,
            episode=episode.index,
            show_guid=show_guid,
            tvdb_id=external_ids.get("tvdb_id"),
            tmdb_id=external_ids.get("tmdb_id"),
            imdb_id=external_ids.get("imdb_id"),
            plex_guid=external_ids.get("plex_guid"),
            show_key=show_key,
        )
        if existing:
            updated = False
            if show_guid and existing.show_guid != show_guid:
                existing.show_guid = show_guid
                updated = True
            for key, value in external_ids.items():
                if value and getattr(existing, key) != value:
                    setattr(existing, key, value)
                    updated = True
            if updated:
                db.session.commit()
            return
        notification = Notification(
            email=normalized_email,
            show_title=show_title,
            show_key=show_key,
            show_guid=show_guid,
            tvdb_id=external_ids.get("tvdb_id"),
            tmdb_id=external_ids.get("tmdb_id"),
            imdb_id=external_ids.get("imdb_id"),
            plex_guid=external_ids.get("plex_guid"),
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
    show_guids: Optional[List[str]],
) -> Tuple[bool, List[UserPreferences]]:
    guid_candidates = [str(guid) for guid in (show_guids or []) if guid]
    candidates = [candidate for candidate in ([show_key] + guid_candidates) if candidate]

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

    notification_matches_identity = False
    try:
        notification_rows = Notification.query.filter(Notification.email.in_(emails)).all()
        for notification in notification_rows:
            if show_key and notification.show_key and str(notification.show_key) == str(show_key):
                notification_matches_identity = True
                break
            if guid_candidates and notification.show_guid:
                stored_guid = str(notification.show_guid)
                if stored_guid in guid_candidates:
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
            (show_key and preference.show_key and str(preference.show_key) == str(show_key))
            or (
                guid_candidates
                and preference.show_guid
                and str(preference.show_guid) in guid_candidates
            )
            for preference in opt_out_preferences
        )
        if opted_out:
            current_app.logger.info(
                "Notification history matched %s but user %s has opted out.",
                show_key or (guid_candidates[0] if guid_candidates else "unknown"),
                emails,
            )
            return False, []
        synthetic_preference = UserPreferences(
            email=emails[0],
            show_key=show_key,
            show_guid=guid_candidates[0] if guid_candidates else None,
            show_opt_out=False,
        )
        current_app.logger.info(
            "Notification history subscription fallback match used for %s.",
            show_key or (guid_candidates[0] if guid_candidates else "unknown"),
        )
        return True, [synthetic_preference]
    return False, []


def _user_is_subscribed_for_show(
    *,
    email: str,
    alternate_email: Optional[str],
    show_key: Optional[str],
    show_guid: Optional[str],
    guid_candidates: Optional[List[str]],
    season: Optional[int],
    episode: Optional[int],
    recent_show_keys: Set[str],
    recent_show_guids: Set[str],
) -> Tuple[bool, str]:
    candidates = []
    for candidate in [show_key, show_guid] + (guid_candidates or []):
        if candidate:
            candidate_str = str(candidate)
            if candidate_str not in candidates:
                candidates.append(candidate_str)

    emails = [email]
    if alternate_email and alternate_email not in emails:
        emails.append(alternate_email)

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
        active_preferences = [preference for preference in preferences if not preference.show_opt_out]
        if active_preferences:
            return True, "preference"

    if any(candidate in recent_show_keys or candidate in recent_show_guids for candidate in candidates):
        return True, "recent notification history"

    if not candidates:
        return False, ""

    notifications = Notification.query.filter(
        Notification.email.in_(emails),
        or_(
            Notification.show_guid.in_(candidates),
            Notification.show_key.in_(candidates),
        ),
    )
    if season is not None and episode is not None:
        if notifications.filter(
            Notification.season == season,
            Notification.episode == episode,
        ).first():
            return True, "prior notification for episode"
    if notifications.first():
        return True, "prior notification for show"

    return False, ""


def _user_has_watched_show(
    s: Settings,
    user_id: int,
    grandparent_rating_key: Any,
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
