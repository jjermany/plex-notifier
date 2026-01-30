import os
import re
import logging
import threading
from functools import wraps
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler
from flask import Flask, render_template, redirect, url_for, flash, request, session, send_from_directory, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from itsdangerous import URLSafeTimedSerializer, BadSignature
from .config import db, Settings, UserPreferences, Notification
from .utils import normalize_email, email_to_filename, normalize_show_identity
from .forms import SettingsForm, TestEmailForm, ManualCheckForm, LoginForm
from .constants import (
    DEFAULT_HISTORY_LIMIT,
    HISTORY_ENTRIES_PER_PAGE,
    MONTHLY_STATS_MONTHS,
    SUBSCRIPTIONS_SHOWS_PER_PAGE,
    INACTIVE_SHOW_THRESHOLD_DAYS,
)
from .notifier import start_scheduler, _send_email, check_new_episodes, register_debug_route
from .logging_utils import TZFormatter
from .constants import (
    RATE_LIMIT_TEST_EMAIL,
    RATE_LIMIT_MANUAL_CHECK,
    APP_LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
)
from sqlalchemy import inspect, text

serializer = URLSafeTimedSerializer(os.environ.get("SECRET_KEY", "change-me"))


# 🔐 Auth helpers
def _is_safe_next_url(target: str | None) -> bool:
    return bool(target) and target.startswith("/") and not target.startswith("//")


def _is_admin_authed() -> bool:
    return session.get("admin_authed", False)


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _is_admin_authed():
            next_url = request.full_path if request.query_string else request.path
            return redirect(url_for("login", next=next_url))
        return f(*args, **kwargs)

    return decorated

def create_app():
    log_format = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    level = logging.DEBUG if os.getenv("DEBUG", "false").lower() == "true" else logging.INFO
    handler = logging.StreamHandler()
    handler.setFormatter(TZFormatter(log_format))
    logging.basicConfig(level=level, handlers=[handler])

    # Suppress overly verbose logs
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

    app = Flask(__name__, instance_relative_config=True)
    app.logger.setLevel(logging.DEBUG)
    log_dir = os.path.abspath(os.path.join(app.root_path, "..", "instance", "logs"))
    os.makedirs(log_dir, exist_ok=True)
    app_log_path = os.path.join(log_dir, "app.log")
    app_file_handler = RotatingFileHandler(
        app_log_path,
        maxBytes=APP_LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
    )
    app_file_handler.setFormatter(TZFormatter(log_format))
    root_logger = logging.getLogger()
    if not any(
        isinstance(existing, RotatingFileHandler)
        and getattr(existing, "baseFilename", None) == app_log_path
        for existing in root_logger.handlers
    ):
        root_logger.addHandler(app_file_handler)

    @app.route('/media/<path:filename>')
    def media_file(filename):
        media_dir = os.path.abspath(os.path.join(app.root_path, "..", "media"))
        return send_from_directory(media_dir, filename)

    @app.route('/manifest.webmanifest')
    def manifest():
        static_dir = os.path.abspath(os.path.join(app.root_path, "static"))
        return send_from_directory(
            static_dir,
            "manifest.webmanifest",
            mimetype="application/manifest+json",
        )

    @app.route('/icons/<path:filename>')
    def icon_file(filename):
        icons_dir = os.path.abspath(os.path.join(app.root_path, "static", "icons"))
        return send_from_directory(icons_dir, filename)

    # Validate SECRET_KEY
    secret_key = os.environ.get('SECRET_KEY', 'change-me')
    if secret_key == 'change-me':
        app.logger.error("=" * 80)
        app.logger.error("SECURITY ERROR: SECRET_KEY is set to default 'change-me' value!")
        app.logger.error("Please set a secure SECRET_KEY environment variable.")
        app.logger.error("You can generate one with: python -c 'import secrets; print(secrets.token_hex(32))'")
        app.logger.error("=" * 80)
        raise ValueError("Insecure SECRET_KEY detected. Application will not start.")

    if len(secret_key) < 32:
        app.logger.warning("!" * 80)
        app.logger.warning("WARNING: SECRET_KEY is shorter than recommended (32+ characters)")
        app.logger.warning("!" * 80)

    os.makedirs(app.instance_path, exist_ok=True)
    db_path = os.path.join(app.instance_path, 'config.sqlite3')
    if not os.path.exists(db_path):
        open(db_path, 'a').close()

    app.config.from_mapping(
        SECRET_KEY=secret_key,
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{db_path}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        PERMANENT_SESSION_LIFETIME=timedelta(
            hours=int(os.environ.get("SESSION_LIFETIME_HOURS", "12"))
        ),
    )

    session_cookie_secure = os.environ.get("SESSION_COOKIE_SECURE")
    if session_cookie_secure is not None:
        app.config["SESSION_COOKIE_SECURE"] = session_cookie_secure.lower() == "true"

    # Initialize rate limiter
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=["200 per day", "50 per hour"],
        storage_uri="memory://",
    )

    db.init_app(app)

    with app.app_context():
        # Create all tables (will only create if they don't exist)
        db.create_all()

        # Handle legacy schema migrations
        inspector = inspect(db.engine)

        # Migrate settings table if needed
        if 'settings' in inspector.get_table_names():
            existing_cols = {c['name'] for c in inspector.get_columns('settings')}
            with db.engine.begin() as conn:
                if 'notify_interval' not in existing_cols:
                    conn.execute(text('ALTER TABLE settings ADD COLUMN notify_interval INTEGER DEFAULT 30'))
                    app.logger.info("Added notify_interval column to settings table")
                if 'base_url' not in existing_cols:
                    conn.execute(text('ALTER TABLE settings ADD COLUMN base_url VARCHAR'))
                    app.logger.info("Added base_url column to settings table")
        # Migrate user_preferences table to add unique constraint if it doesn't exist
        if 'user_preferences' in inspector.get_table_names():
            try:
                # Try to create the unique constraint if it doesn't exist
                # SQLite doesn't support ALTER TABLE ADD CONSTRAINT, so we need to check if constraint exists
                constraints = inspector.get_unique_constraints('user_preferences')
                constraint_names = [c['name'] for c in constraints]
                if 'uq_email_show_key' not in constraint_names:
                    # For SQLite, we'd need to recreate the table, but since we're using db.create_all()
                    # it should handle this. For now, just log a warning.
                    app.logger.warning("Legacy user_preferences table detected. New unique constraints will apply to new records.")
            except Exception as e:
                app.logger.warning(f"Could not check user_preferences constraints: {e}")

        # Add show_guid columns if missing
        with db.engine.begin() as conn:
            if 'notifications' in inspector.get_table_names():
                existing_cols = {c['name'] for c in inspector.get_columns('notifications')}
                if 'show_guid' not in existing_cols:
                    conn.execute(text('ALTER TABLE notifications ADD COLUMN show_guid VARCHAR'))
                    app.logger.info("Added show_guid column to notifications table")
            if 'user_preferences' in inspector.get_table_names():
                existing_cols = {c['name'] for c in inspector.get_columns('user_preferences')}
                if 'show_guid' not in existing_cols:
                    conn.execute(text('ALTER TABLE user_preferences ADD COLUMN show_guid VARCHAR'))
                    app.logger.info("Added show_guid column to user_preferences table")
        # Backfill show_guid for existing notifications and preferences
        try:
            notifications = Notification.query.filter(Notification.show_guid.is_(None)).all()
            if notifications:
                for notif in notifications:
                    notif.show_guid = normalize_show_identity(notif.show_title)
                db.session.commit()

            show_guid_map = {
                notif.show_key: notif.show_guid
                for notif in Notification.query.filter(Notification.show_guid.isnot(None)).all()
                if notif.show_key and notif.show_guid
            }
            prefs = UserPreferences.query.filter(
                UserPreferences.show_guid.is_(None),
                UserPreferences.show_key.isnot(None)
            ).all()
            updates = False
            for pref in prefs:
                if pref.show_key in show_guid_map:
                    pref.show_guid = show_guid_map[pref.show_key]
                    updates = True
            if updates:
                db.session.commit()
        except Exception as exc:
            app.logger.warning(f"Failed to backfill show identifiers: {exc}")
            db.session.rollback()

        # Create default settings if none exist
        s = Settings.query.first()
        if not s:
            s = Settings(
                plex_url="http://localhost:32400",
                plex_token="placeholder",
                notify_interval=30,
            )
            db.session.add(s)
            db.session.commit()
            app.logger.info("Created default settings")

        interval = s.notify_interval or 30
        sched = start_scheduler(app, interval)
        app.config['scheduler'] = sched
        app.logger.info("✅ App initialized successfully.")

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        form = LoginForm()
        next_url = request.args.get("next")
        if request.method == "GET" and next_url:
            form.next.data = next_url

        if form.validate_on_submit():
            username = form.username.data
            password = form.password.data
            if (
                username == os.environ.get("WEBUI_USER")
                and password == os.environ.get("WEBUI_PASS")
            ):
                session.permanent = True
                session["admin_authed"] = True
                redirect_target = form.next.data or url_for("history")
                if not _is_safe_next_url(redirect_target):
                    redirect_target = url_for("history")
                return redirect(redirect_target)
            flash("Invalid username or password.", "danger")

        return render_template("login.html", form=form)

    @app.route("/logout")
    def logout():
        session.pop("admin_authed", None)
        return redirect(url_for("login"))

    @app.route('/settings', methods=['GET', 'POST'])
    @requires_auth
    def settings():
        s = Settings.query.first() or Settings(notify_interval=30)
        form = SettingsForm(obj=s)
        test_form = TestEmailForm()
        manual_check_form = ManualCheckForm()

        if form.validate_on_submit():
            form.populate_obj(s)
            s.notify_interval = s.notify_interval or 30
            db.session.add(s)
            db.session.commit()
            flash('Settings saved!', 'success')

            sched = app.config.get('scheduler')
            if sched:
                sched.reschedule_job(
                    'check_job',
                    trigger='interval',
                    minutes=s.notify_interval
                )
                app.logger.info(
                    f"Rescheduled check_new_episodes to every {s.notify_interval} min"
                )

            return redirect(url_for('settings'))

        return render_template('settings.html', form=form, test_form=test_form, manual_check_form=manual_check_form)

    @app.route('/log-viewer')
    @requires_auth
    def log_viewer():
        return render_template('log_viewer.html')

    @app.route('/test-email', methods=['POST'])
    @requires_auth
    @limiter.limit(RATE_LIMIT_TEST_EMAIL)
    def send_test_email():
        s = Settings.query.first()
        if not s:
            flash('Please save settings first.', 'warning')
            return redirect(url_for('settings'))

        form = TestEmailForm()
        if form.validate_on_submit():
            try:
                from email.mime.multipart import MIMEMultipart
                from email.mime.text import MIMEText

                msg = MIMEMultipart('alternative')
                msg['Subject'] = '📧 Plex Notifier Test Email'
                msg['From'] = s.from_address
                msg['To'] = form.test_email.data

                html_body = '<p>If you can read this, your email settings are correct!</p>'
                msg.attach(MIMEText(html_body, 'html'))

                _send_email(s, msg)
                flash(f'Test email sent to {form.test_email.data}!', 'success')
            except Exception as e:
                flash(f'Failed to send test email: {e}', 'danger')

        return redirect(url_for('settings'))

    @app.route('/run-check', methods=['POST'])
    @requires_auth
    @limiter.limit(RATE_LIMIT_MANUAL_CHECK)
    def run_check():
        s = Settings.query.first()
        if not s:
            flash('Please save settings first.', 'warning')
            return redirect(url_for('settings'))

        manual_check_form = ManualCheckForm()
        time_window_minutes = 1440  # default to 24 hours

        if manual_check_form.validate_on_submit():
            time_window_minutes = int(manual_check_form.time_window.data)

        def run_async():
            try:
                check_new_episodes(app, override_interval_minutes=time_window_minutes)
            except Exception as e:
                app.logger.error(f"Manual check failed: {e}")

        threading.Thread(target=run_async).start()
        hours = time_window_minutes / 60
        if hours < 1:
            time_desc = f"{time_window_minutes} minutes"
        elif hours == 1:
            time_desc = "1 hour"
        elif hours < 24:
            time_desc = f"{int(hours)} hours"
        elif hours == 24:
            time_desc = "24 hours"
        else:
            time_desc = f"{int(hours / 24)} days"

        flash(f'✅ Manual check started (looking back {time_desc})! You will be notified if there are new episodes.', 'info')
        return redirect(url_for('log_viewer'))

    @app.route('/subscriptions', methods=['GET', 'POST'])
    def subscriptions():
        token = request.args.get("token") if request.method == "GET" else request.form.get("token")
        if not token:
            return render_template("subscriptions.html", email=None)

        try:
            email = serializer.loads(token, salt="unsubscribe", max_age=86400 * 7)
            canon = normalize_email(email)
        except BadSignature:
            return render_template("subscriptions.html", email=None)

        if request.method == "POST":
            def _parse_show_token(value: str) -> tuple[str, str]:
                if "::" in value:
                    show_id, show_key = value.split("::", 1)
                else:
                    show_id, show_key = value, value
                return show_id, show_key

            global_opt_out = bool(request.form.get("global_opt_out"))
            show_optouts = request.form.getlist("show_optouts")
            visible_shows = request.form.getlist("visible_shows")  # Track shows on current page
            visible_show_ids = []
            visible_show_keys = []
            for entry in visible_shows:
                show_id, show_key = _parse_show_token(entry)
                if show_id:
                    visible_show_ids.append(show_id)
                if show_key:
                    visible_show_keys.append(show_key)

            pref = UserPreferences.query.filter_by(email=canon, show_key=None).first()
            if not pref:
                pref = UserPreferences.query.filter_by(email=email, show_key=None).first()
                if pref:
                    pref.email = canon
                else:
                    pref = UserPreferences(email=canon)
            pref.global_opt_out = global_opt_out
            db.session.add(pref)

            # Only delete opt-outs for shows that are visible on the current page
            # This preserves opt-outs for shows not currently displayed
            if visible_show_ids or visible_show_keys:
                UserPreferences.query.filter(
                    UserPreferences.email.in_([canon, email]),
                    UserPreferences.show_key.in_(visible_show_keys)
                ).delete(synchronize_session=False)
                UserPreferences.query.filter(
                    UserPreferences.email.in_([canon, email]),
                    UserPreferences.show_guid.in_(visible_show_ids)
                ).delete(synchronize_session=False)

            # Add opt-outs for checked shows
            for show_value in show_optouts:
                show_id, show_key = _parse_show_token(show_value)
                pref = None
                if show_id:
                    pref = UserPreferences.query.filter_by(email=canon, show_guid=show_id).first()
                if not pref and show_key:
                    pref = UserPreferences.query.filter_by(email=canon, show_key=show_key).first()
                if pref:
                    pref.show_opt_out = True
                    pref.show_key = show_key or pref.show_key
                    pref.show_guid = show_id or pref.show_guid
                else:
                    db.session.add(UserPreferences(
                        email=canon,
                        show_guid=show_id or None,
                        show_key=show_key,
                        show_opt_out=True
                    ))

            db.session.commit()
            flash("Preferences updated.", "success")
            return redirect(url_for("subscriptions") + f"?token={token}")

        # Get pagination and filter parameters
        page = max(int(request.args.get('page', 1)), 1)
        per_page = SUBSCRIPTIONS_SHOWS_PER_PAGE
        show_inactive = request.args.get('show_inactive', 'false').lower() == 'true'
        search_query = request.args.get('search', '').strip()

        # Get shows from database notifications (primary source) with last notification date
        show_map = {}  # key -> {title, last_notified} mapping

        # Get most recent notification for each show
        from sqlalchemy import func
        show_latest = (
            db.session.query(
                Notification.show_key,
                Notification.show_guid,
                Notification.show_title,
                func.max(Notification.timestamp).label('last_notified')
            )
            .filter_by(email=canon)
            .group_by(Notification.show_key, Notification.show_guid, Notification.show_title)
            .all()
        )

        for show_key, show_guid, show_title, last_notified in show_latest:
            fallback_id = normalize_show_identity(show_title)
            show_id = show_guid or fallback_id or show_key
            if show_id in show_map:
                if last_notified and show_map[show_id]['last_notified']:
                    if last_notified > show_map[show_id]['last_notified']:
                        show_map[show_id]['last_notified'] = last_notified
                elif last_notified:
                    show_map[show_id]['last_notified'] = last_notified
                if show_guid and not show_map[show_id]['show_guid']:
                    show_map[show_id]['show_guid'] = show_guid
                if show_key and not show_map[show_id]['show_key']:
                    show_map[show_id]['show_key'] = show_key
                if fallback_id and not show_map[show_id].get('show_fallback_id'):
                    show_map[show_id]['show_fallback_id'] = fallback_id
                continue
            show_map[show_id] = {
                'title': show_title,
                'last_notified': last_notified,
                'show_guid': show_guid,
                'show_key': show_key,
                'show_fallback_id': fallback_id,
            }

        # Fallback to log file if database is empty (backward compatibility)
        if not show_map:
            log_dir = os.path.join(os.path.dirname(__file__), "../instance/logs")
            safe_filename = email_to_filename(email)
            log_file = os.path.join(log_dir, f"{safe_filename}-notification.log")

            if os.path.exists(log_file):
                with open(log_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if "Notified:" in line and "[Key:" in line:
                            try:
                                parts = line.split("Notified: ")[1]
                                show_title = parts.split(" [Key:")[0].strip()
                                key_part = parts.split("[Key:")[1]
                                show_key = key_part.split("]")[0]
                                # For log file entries, we don't have timestamp, set to None
                                fallback_id = normalize_show_identity(show_title)
                                show_id = fallback_id or show_key
                                show_map[show_id] = {
                                    'title': show_title,
                                    'last_notified': None,
                                    'show_guid': None,
                                    'show_key': show_key,
                                    'show_fallback_id': fallback_id,
                                }
                            except Exception:
                                continue

        show_key_to_id = {
            info['show_key']: show_id
            for show_id, info in show_map.items()
            if info.get('show_key')
        }
        fallback_id_to_id = {
            info['show_fallback_id']: show_id
            for show_id, info in show_map.items()
            if info.get('show_fallback_id')
        }

        user_prefs = UserPreferences.query.filter(
            UserPreferences.email.in_([canon, email])
        ).all()
        global_opt_out = any(p.global_opt_out for p in user_prefs if p.show_key is None)
        opted_out_shows = set()
        prefs_updated = False
        for pref in user_prefs:
            if pref.show_key is None:
                continue
            show_id = pref.show_guid
            if show_id and show_id in fallback_id_to_id:
                show_id = fallback_id_to_id[show_id]
            if not show_id and pref.show_key in show_key_to_id:
                show_id = show_key_to_id[pref.show_key]
                pref.show_guid = show_id
                prefs_updated = True
            opted_out_shows.add(show_id or pref.show_key)

        if prefs_updated:
            db.session.commit()

        # Build list of shows with their opt-out status and last notification date
        shows_list = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=INACTIVE_SHOW_THRESHOLD_DAYS)

        for key, info in show_map.items():
            # Filter inactive shows unless show_inactive is True
            if not show_inactive and info['last_notified']:
                # Make timezone-aware if needed
                last_notified = info['last_notified']
                if last_notified.tzinfo is None:
                    last_notified = last_notified.replace(tzinfo=timezone.utc)

                # Skip shows that haven't notified in the threshold period
                if last_notified < cutoff_date:
                    continue

            shows_list.append({
                'key': key,
                'show_key': info.get('show_key') or "",
                'show_guid': info.get('show_guid') or "",
                'show_fallback_id': info.get('show_fallback_id') or "",
                'title': info['title'],
                'opted_out': key in opted_out_shows,
                'last_notified': info['last_notified']
            })

        # Sort by most recent notification date (descending), shows without dates at the end
        shows_list.sort(key=lambda x: x['last_notified'] if x['last_notified'] else datetime.min.replace(tzinfo=timezone.utc), reverse=True)

        # Apply search filter if provided
        if search_query:
            shows_list = [
                show for show in shows_list
                if search_query.lower() in show['title'].lower()
            ]

        # Calculate pagination
        total_shows = len(shows_list)
        total_pages = max((total_shows - 1) // per_page + 1, 1) if total_shows > 0 else 1

        # Get shows for current page
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_shows = shows_list[start_idx:end_idx]

        # Count inactive shows for display
        inactive_count = 0
        if not show_inactive:
            for key, info in show_map.items():
                if info['last_notified']:
                    last_notified = info['last_notified']
                    if last_notified.tzinfo is None:
                        last_notified = last_notified.replace(tzinfo=timezone.utc)
                    if last_notified < cutoff_date:
                        inactive_count += 1

        return render_template(
            "subscriptions.html",
            email=email,
            token=token,
            global_opt_out=global_opt_out,
            shows=paginated_shows,
            opted_out_shows=opted_out_shows,
            page=page,
            total_pages=total_pages,
            show_inactive=show_inactive,
            inactive_count=inactive_count,
            search_query=search_query,
        )

    @app.route('/health')
    def health():
        """Public health check endpoint for monitoring."""
        try:
            # Check database connectivity
            db.session.execute(text('SELECT 1'))

            # Get scheduler status
            sched = app.config.get('scheduler')
            scheduler_running = sched is not None and sched.running

            next_run = None
            if scheduler_running:
                job = sched.get_job('check_job')
                if job and job.next_run_time:
                    next_run = job.next_run_time.astimezone().isoformat()

            # Get recent notification count
            from datetime import timedelta
            tz = ZoneInfo(os.environ.get("TZ")) if os.environ.get("TZ") else timezone.utc
            one_hour_ago = datetime.now(tz) - timedelta(hours=1)
            recent_notifications = Notification.query.filter(
                Notification.timestamp >= one_hour_ago
            ).count()

            return {
                'status': 'healthy',
                'database': 'connected',
                'scheduler': 'running' if scheduler_running else 'stopped',
                'next_run': next_run,
                'recent_notifications_1h': recent_notifications
            }, 200
        except Exception as e:
            app.logger.error(f"Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e)
            }, 500

    @app.route('/api/admin/logs')
    @requires_auth
    def admin_logs():
        log_path = os.path.abspath(os.path.join(app.root_path, "..", "instance", "logs", "app.log"))
        try:
            offset = int(request.args.get("offset", "0"))
        except ValueError:
            offset = 0
        max_bytes = 50_000
        try:
            requested_max = int(request.args.get("max_bytes", max_bytes))
            max_bytes = max(1_000, min(requested_max, 200_000))
        except ValueError:
            pass

        if not os.path.exists(log_path):
            return jsonify({
                "lines": ["Log file not available yet."],
                "offset": 0,
                "file_size": 0,
                "ends_with_newline": True,
            })

        file_size = os.path.getsize(log_path)
        if offset < 0 or offset > file_size:
            offset = 0

        with open(log_path, "rb") as log_file:
            log_file.seek(offset)
            chunk = log_file.read(max_bytes)
            new_offset = log_file.tell()

        decoded = chunk.decode("utf-8", errors="replace")
        lines = decoded.splitlines()
        ends_with_newline = chunk.endswith(b"\n")
        return jsonify({
            "lines": lines,
            "offset": new_offset,
            "file_size": file_size,
            "ends_with_newline": ends_with_newline,
        })

    @app.route('/')
    @requires_auth
    def history():
        tz = ZoneInfo(os.environ.get("TZ")) if os.environ.get("TZ") else None

        def fmt_dt(dt: datetime | None):
            if not dt:
                return ""
            # Database stores timestamps as naive UTC datetimes
            # First mark as UTC, then convert to local timezone
            if tz and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc).astimezone(tz)
            date = dt.strftime("%m/%d/%Y")
            hour = dt.strftime("%I").lstrip('0') or '0'
            minute = dt.strftime("%M")
            ampm = dt.strftime("%p").lower()
            zone = dt.strftime("%Z") if dt.tzinfo else ""
            return f"{date} {hour}:{minute}{ampm} {zone}".strip()

        # Get user list from database
        users = {
            normalize_email(p.email)
            for p in UserPreferences.query.with_entities(UserPreferences.email).distinct()
        }
        # Also get users from Notification table
        notif_users = {
            n.email
            for n in Notification.query.with_entities(Notification.email).distinct()
        }
        users = users.union(notif_users)
        users = sorted(users)

        # Calculate user counts from database
        user_counts = {}
        for u in users:
            count = Notification.query.filter_by(email=u).count()
            user_counts[u] = count
        user_counts = dict(sorted(user_counts.items()))

        # Get filter parameters
        selected_raw = request.args.get('email')
        selected = normalize_email(selected_raw) if selected_raw else None
        query = selected
        page = max(int(request.args.get('page', 1)), 1)
        per_page = HISTORY_ENTRIES_PER_PAGE

        # Build database query
        base_query = Notification.query

        if query:
            # Filter to specific users matching query
            matched_users = [u for u in users if query in normalize_email(u)]
            if matched_users:
                base_query = base_query.filter(Notification.email.in_(matched_users))

        # Get total count for pagination
        total_count = base_query.count()
        total_pages = max((total_count - 1) // per_page + 1, 1) if total_count > 0 else 1

        # Get paginated notifications
        notifications = (
            base_query
            .order_by(Notification.timestamp.desc())
            .limit(per_page)
            .offset((page - 1) * per_page)
            .all()
        )

        # Convert to entries format
        entries = []
        for notif in notifications:
            message = f"Sent to {notif.email} | Episodes: {notif.show_title} S{notif.season}E{notif.episode}"
            entries.append({
                'time': fmt_dt(notif.timestamp),
                'message': message,
                'dt': notif.timestamp
            })

        # Get user preferences for single user view
        global_opt_out = False
        opted_out = []
        single_user = False
        subscription_token = None
        prev_user = None
        next_user = None

        if query:
            matched_users = [u for u in users if query in normalize_email(u)]
            if len(matched_users) == 1:
                single_user = True
                u = matched_users[0]
                prefs = UserPreferences.query.filter_by(email=u).all()
                global_opt_out = any(p.global_opt_out for p in prefs if p.show_key is None)

                # Get show titles from notifications for opted-out shows
                show_map = {}
                user_notifs = Notification.query.filter_by(email=u).limit(100).all()
                for n in user_notifs:
                    if n.show_key not in show_map:
                        show_map[n.show_key] = {
                            "title": n.show_title,
                            "show_key": n.show_key,
                            "show_guid": n.show_guid,
                            "show_fallback_id": normalize_show_identity(n.show_title),
                        }

                opted_out = []
                for p in prefs:
                    if not p.show_key:
                        continue
                    info = show_map.get(p.show_key)
                    if info:
                        opted_out.append({
                            "title": info["title"],
                            "show_key": info["show_key"],
                            "show_guid": info.get("show_guid") or p.show_guid or "",
                            "show_fallback_id": info.get("show_fallback_id") or "",
                        })
                    else:
                        opted_out.append({
                            "title": p.show_key,
                            "show_key": p.show_key,
                            "show_guid": p.show_guid or "",
                            "show_fallback_id": "",
                        })

                # Generate subscription token for this user
                subscription_token = serializer.dumps(u, salt="unsubscribe")

                # Calculate prev/next users for navigation
                if users:
                    user_list = list(users)
                    try:
                        current_idx = user_list.index(u)
                        if current_idx > 0:
                            prev_user = user_list[current_idx - 1]
                        if current_idx < len(user_list) - 1:
                            next_user = user_list[current_idx + 1]
                    except ValueError:
                        pass

        # Calculate monthly stats from database
        today = datetime.now()
        months_ago_limit = MONTHLY_STATS_MONTHS
        cutoff_date = today.replace(year=today.year - 1)

        monthly_stats = db.session.query(
            db.func.strftime('%Y-%m', Notification.timestamp).label('month'),
            db.func.count(Notification.id).label('count')
        ).filter(
            Notification.timestamp >= cutoff_date
        ).group_by('month').all()

        monthly_totals = [
            (datetime.strptime(month, "%Y-%m").strftime("%b %Y"), count)
            for month, count in sorted(monthly_stats)
        ]

        return render_template(
            'history.html',
            email=query,
            selected=selected_raw,
            entries=entries,
            global_opt_out=global_opt_out,
            opted_out=opted_out,
            users=users,
            user_counts=user_counts,
            monthly_totals=monthly_totals,
            page=page,
            total_pages=total_pages,
            show_prefs=single_user,
            subscription_token=subscription_token,
            prev_user=prev_user,
            next_user=next_user,
        )

    register_debug_route(app)
    return app
