import os
import logging
from flask import Flask, render_template, redirect, url_for, flash
from .config import db, Settings
from .forms import SettingsForm, TestEmailForm
from .notifier import start_scheduler, _send_email, check_new_episodes, register_debug_route

def create_app():
    logging.basicConfig(level=logging.DEBUG)
    app = Flask(__name__, instance_relative_config=True)
    app.logger.setLevel(logging.DEBUG)

    # Create DB file if missing
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

    # Settings page
    @app.route('/', methods=['GET', 'POST'])
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

    # Test email
    @app.route('/test-email', methods=['POST'])
    def send_test_email():
        s = Settings.query.first()
        if not s:
            flash('Please save settings first.', 'warning')
            return redirect(url_for('settings'))

        form = TestEmailForm()
        if form.validate_on_submit():
            try:
                _send_email(
                    s,
                    form.test_email.data,
                    '📧 Plex Notifier Test Email',
                    '<p>If you can read this, your email settings are correct!</p>'
                )
                flash(f'Test email sent to {form.test_email.data}!', 'success')
            except Exception as e:
                flash(f'Failed to send test email: {e}', 'danger')

        return redirect(url_for('settings'))

    # Manual run-check endpoint
    @app.route('/run-check', methods=['POST'])
    def run_check():
        s = Settings.query.first()
        if not s:
            flash('Please save settings first.', 'warning')
            return redirect(url_for('settings'))

        orig_interval = s.notify_interval
        s.notify_interval = 1440  # 24 hours
        db.session.commit()

        try:
            check_new_episodes(app)
            flash('Manual check for the last 24h completed. See logs.', 'info')
        finally:
            s.notify_interval = orig_interval
            db.session.commit()

        return redirect(url_for('settings'))

    # Start scheduler
    with app.app_context():
        s = Settings.query.first()
        interval = s.notify_interval if s and s.notify_interval else 30
        sched = start_scheduler(app, interval)
        app.config['scheduler'] = sched

    # Register optional debug route: /force-run
    register_debug_route(app)

    return app
