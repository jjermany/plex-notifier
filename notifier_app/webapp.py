import os
import re
import logging
import threading
from functools import wraps
from collections import Counter
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import Flask, render_template, redirect, url_for, flash, request, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from itsdangerous import URLSafeTimedSerializer, BadSignature
from .config import db, Settings, UserPreferences, Notification
from .utils import normalize_email, email_to_filename
from .forms import SettingsForm, TestEmailForm, ManualCheckForm
from .constants import DEFAULT_HISTORY_LIMIT, HISTORY_ENTRIES_PER_PAGE, MONTHLY_STATS_MONTHS
from .notifier import start_scheduler, _send_email, check_new_episodes, register_debug_route
from .logging_utils import TZFormatter
from .constants import RATE_LIMIT_TEST_EMAIL, RATE_LIMIT_MANUAL_CHECK
from sqlalchemy import inspect, text

serializer = URLSafeTimedSerializer(os.environ.get("SECRET_KEY", "change-me"))


def _migrate_legacy_notifications(app):
    """
    One-time migration of legacy log files to database.

    Checks if Notification table is empty and if legacy log files exist.
    If both conditions are met, parses log files and imports data into database.
    """
    # Check if migration is needed (Notification table is empty)
    notification_count = Notification.query.count()
    if notification_count > 0:
        app.logger.info("Notification table already has data, skipping legacy migration")
        return

    log_dir = os.path.join(os.path.dirname(__file__), "../instance/logs")
    if not os.path.exists(log_dir):
        app.logger.info("No legacy log directory found, skipping migration")
        return

    # Find all user notification log files
    log_files = [
        f for f in os.listdir(log_dir)
        if f.endswith('-notification.log') and f != 'notifications.log'
    ]

    if not log_files:
        app.logger.info("No legacy user notification logs found, skipping migration")
        return

    app.logger.info("=" * 80)
    app.logger.info("🔄 Legacy notification logs detected - starting migration to database")
    app.logger.info("=" * 80)

    total_imported = 0
    total_errors = 0

    for log_file in log_files:
        log_path = os.path.join(log_dir, log_file)

        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            file_imported = 0
            file_errors = 0

            for line in lines:
                if "Notified:" not in line or "[Key:" not in line:
                    continue

                try:
                    # Parse timestamp
                    timestamp_str = line.split("|")[0].strip()
                    try:
                        # Try multiple timestamp formats
                        for fmt in ["%m/%d/%Y %I:%M%p %Z", "%Y-%m-%d %H:%M:%S,%f"]:
                            try:
                                timestamp = datetime.strptime(timestamp_str, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            # If no format worked, use current time
                            timestamp = datetime.now()
                    except:
                        timestamp = datetime.now()

                    # Parse notification details
                    # Format: "Notified: Show Title [Key:12345] S1E2 - Episode Title"
                    notif_part = line.split("Notified:")[1].strip()

                    # Extract show title
                    show_title = notif_part.split("[Key:")[0].strip()

                    # Extract show key
                    key_part = notif_part.split("[Key:")[1]
                    show_key = key_part.split("]")[0].strip()

                    # Extract season/episode and title
                    rest = key_part.split("]", 1)[1].strip()
                    season_ep_part = rest.split(" - ")[0].strip()

                    # Parse S1E2 format
                    season = None
                    episode = None
                    if season_ep_part.startswith("S") and "E" in season_ep_part:
                        parts = season_ep_part[1:].split("E")
                        season = int(parts[0])
                        episode = int(parts[1])

                    episode_title = rest.split(" - ", 1)[1].strip() if " - " in rest else ""

                    # Extract email from filename (reverse the email_to_filename logic)
                    # Filename format: localpart_hash-notification.log
                    # We need to look up the actual email from UserPreferences
                    filename_base = log_file.replace("-notification.log", "")

                    # Try to find matching email in UserPreferences
                    user_email = None
                    all_users = UserPreferences.query.with_entities(UserPreferences.email).distinct().all()
                    for (pref_email,) in all_users:
                        if pref_email and email_to_filename(pref_email) == filename_base:
                            user_email = normalize_email(pref_email)
                            break

                    # If we couldn't find the email, skip this entry
                    if not user_email or not season or not episode:
                        file_errors += 1
                        continue

                    # Create notification record
                    notification = Notification(
                        email=user_email,
                        show_title=show_title,
                        show_key=show_key,
                        season=season,
                        episode=episode,
                        episode_title=episode_title,
                        timestamp=timestamp
                    )

                    db.session.add(notification)
                    file_imported += 1

                except Exception as e:
                    file_errors += 1
                    continue

            # Commit after each file
            try:
                db.session.commit()
                total_imported += file_imported
                total_errors += file_errors
                app.logger.info(f"  ✓ Migrated {file_imported} notifications from {log_file} ({file_errors} errors)")
            except Exception as e:
                db.session.rollback()
                app.logger.error(f"  ✗ Failed to commit {log_file}: {e}")

        except Exception as e:
            app.logger.error(f"  ✗ Failed to read {log_file}: {e}")
            continue

    app.logger.info("=" * 80)
    app.logger.info(f"✅ Migration complete: {total_imported} notifications imported, {total_errors} errors")
    app.logger.info("=" * 80)


# 🔐 Auth helpers
def check_auth(username, password):
    return username == os.environ.get("WEBUI_USER") and password == os.environ.get("WEBUI_PASS")

def authenticate():
    return Response('Login required.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
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
    )

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

        # Create default settings if none exist
        s = Settings.query.first()
        if not s:
            s = Settings(
                plex_url="http://localhost:32400",
                plex_token="placeholder",
                notify_interval=30
            )
            db.session.add(s)
            db.session.commit()
            app.logger.info("Created default settings")

        # Migrate legacy log files to database on first run
        _migrate_legacy_notifications(app)

        interval = s.notify_interval or 30
        sched = start_scheduler(app, interval)
        app.config['scheduler'] = sched
        app.logger.info("✅ App initialized successfully.")

    @app.route('/', methods=['GET', 'POST'])
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
        return redirect(url_for('settings'))

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
            global_opt_out = bool(request.form.get("global_opt_out"))
            show_optouts = request.form.getlist("show_optouts")

            pref = UserPreferences.query.filter_by(email=canon, show_key=None).first()
            if not pref:
                pref = UserPreferences.query.filter_by(email=email, show_key=None).first()
                if pref:
                    pref.email = canon
                else:
                    pref = UserPreferences(email=canon)
            pref.global_opt_out = global_opt_out
            db.session.add(pref)

            UserPreferences.query.filter(
                UserPreferences.email.in_([canon, email]),
                UserPreferences.show_key != None
            ).delete(synchronize_session=False)

            for show_key in show_optouts:
                db.session.add(UserPreferences(email=canon, show_key=show_key, show_opt_out=True))

            db.session.commit()
            flash("Preferences updated.", "success")
            return redirect(url_for("subscriptions") + f"?token={token}")

        user_prefs = UserPreferences.query.filter(
            UserPreferences.email.in_([canon, email])
        ).all()
        global_opt_out = any(p.global_opt_out for p in user_prefs if p.show_key is None)
        opted_out_shows = {p.show_key for p in user_prefs if p.show_key}

        # Get shows from database notifications (primary source)
        show_map = {}  # key -> title mapping
        user_notifications = (
            Notification.query
            .filter_by(email=canon)
            .with_entities(Notification.show_key, Notification.show_title)
            .distinct()
            .all()
        )

        for show_key, show_title in user_notifications:
            show_map[show_key] = show_title

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
                                show_map[show_key] = show_title
                            except Exception:
                                continue

        # Build list of shows with their opt-out status
        shows_list = [
            {
                'key': key,
                'title': title,
                'opted_out': key in opted_out_shows
            }
            for key, title in show_map.items()
        ]
        shows_list.sort(key=lambda x: x['title'])

        return render_template(
            "subscriptions.html",
            email=email,
            token=token,
            global_opt_out=global_opt_out,
            shows=shows_list,
            opted_out_shows=opted_out_shows,
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

    @app.route('/history')
    @requires_auth
    def history():
        tz = ZoneInfo(os.environ.get("TZ")) if os.environ.get("TZ") else None

        def fmt_dt(dt: datetime | None):
            if not dt:
                return ""
            if tz and dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
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

        # Get search/filter parameters
        search_raw = request.args.get('search')
        search_term = normalize_email(search_raw) if search_raw else None
        selected_raw = request.args.get('email')
        selected = normalize_email(selected_raw) if selected_raw else None
        query = search_term or selected
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
                        show_map[n.show_key] = n.show_title

                opted_out = [show_map.get(p.show_key, p.show_key) for p in prefs if p.show_key]

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
            search_term=search_raw,
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
        )

    register_debug_route(app)
    return app
