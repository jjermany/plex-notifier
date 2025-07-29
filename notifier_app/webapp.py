import os
import logging
import threading
from functools import wraps
from flask import Flask, render_template, redirect, url_for, flash, request, Response
from itsdangerous import URLSafeTimedSerializer, BadSignature
from .config import db, Settings, UserPreferences
from .forms import SettingsForm, TestEmailForm
from .notifier import start_scheduler, _send_email, check_new_episodes, register_debug_route

serializer = URLSafeTimedSerializer(os.environ.get("SECRET_KEY", "change-me"))

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
    logging.basicConfig(level=logging.DEBUG)
    app = Flask(__name__, instance_relative_config=True)
    app.logger.setLevel(logging.DEBUG)

    os.makedirs(app.instance_path, exist_ok=True)
    db_path = os.path.join(app.instance_path, 'config.sqlite3')
    if not os.path.exists(db_path):
        open(db_path, 'a').close()

    app.config.from_mapping(
        SECRET_KEY=os.environ.get('SECRET_KEY', 'change-me'),
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{db_path}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )

    db.init_app(app)

    with app.app_context():
        db.create_all()
        s = Settings.query.first()
        if not s:
            s = Settings(
                plex_url="http://localhost:32400",
                plex_token="placeholder",
                notify_interval=30
            )
            db.session.add(s)
            db.session.commit()

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

        return render_template('settings.html', form=form, test_form=test_form)

    @app.route('/test-email', methods=['POST'])
    @requires_auth
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
    def run_check():
        s = Settings.query.first()
        if not s:
            flash('Please save settings first.', 'warning')
            return redirect(url_for('settings'))

        def run_async():
            try:
                check_new_episodes(app, override_interval_minutes=1440)
            except Exception as e:
                app.logger.error(f"Manual check failed: {e}")

        threading.Thread(target=run_async).start()
        flash('✅ Manual check started! You will be notified if there are new episodes.', 'info')
        return redirect(url_for('settings'))

    @app.route('/subscriptions', methods=['GET', 'POST'])
    def subscriptions():
        token = request.args.get("token") if request.method == "GET" else request.form.get("token")
        if not token:
            return render_template("subscriptions.html", email=None)

        try:
            email = serializer.loads(token, salt="unsubscribe", max_age=86400 * 7)
        except BadSignature:
            return render_template("subscriptions.html", email=None)

        if request.method == "POST":
            global_opt_out = bool(request.form.get("global_opt_out"))
            show_optouts = request.form.getlist("show_optouts")

            pref = UserPreferences.query.filter_by(email=email, show_key=None).first()
            if not pref:
                pref = UserPreferences(email=email)
            pref.global_opt_out = global_opt_out
            db.session.add(pref)

            UserPreferences.query.filter(UserPreferences.email == email, UserPreferences.show_key != None).delete()

            for show_key in show_optouts:
                db.session.add(UserPreferences(email=email, show_key=show_key, show_opt_out=True))

            db.session.commit()
            flash("Preferences updated.", "success")
            return redirect(url_for("subscriptions") + f"?token={token}")

        user_prefs = UserPreferences.query.filter_by(email=email).all()
        global_opt_out = any(p.global_opt_out for p in user_prefs if p.show_key is None)
        opted_out_shows = {p.show_key for p in user_prefs if p.show_key}

        log_dir = os.path.join(os.path.dirname(__file__), "user_logs")
        safe_email = email.replace("@", "_at_").replace(".", "_dot_")
        log_file = os.path.join(log_dir, f"{safe_email}.log")

        shows = set()
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    if "Notified:" in line:
                        try:
                            parts = line.split("Notified: ")[1]
                            show = parts.split(" [Key:")[0].strip()
                            shows.add(show)
                        except Exception:
                            continue

        return render_template(
            "subscriptions.html",
            email=email,
            token=token,
            global_opt_out=global_opt_out,
            shows=sorted(shows),
            opted_out_shows=opted_out_shows,
        )

    register_debug_route(app)
    return app
