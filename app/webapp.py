import os
import logging
from flask import Flask, render_template, redirect, url_for, flash, request
from .config import db, Settings
from .forms import SettingsForm, TestEmailForm
from .notifier import start_scheduler, _send_email

def create_app():
    # Basic logging
    logging.basicConfig(level=logging.INFO)
    
    app = Flask(__name__, instance_relative_config=True)
    app.logger.setLevel(logging.INFO)

    # Ensure instance folder & SQLite DB file
    os.makedirs(app.instance_path, exist_ok=True)
    db_path = os.path.join(app.instance_path, 'config.sqlite3')
    app.config.from_mapping(
        SECRET_KEY=os.environ.get('SECRET_KEY', 'change-me'),
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{db_path}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )

    # Initialize database
    db.init_app(app)
    with app.app_context():
        db.create_all()

    # Settings GUI
    @app.route('/', methods=['GET', 'POST'])
    def settings():
        s = Settings.query.first() or Settings()
        form = SettingsForm(obj=s)
        test_form = TestEmailForm()
        if form.validate_on_submit():
            form.populate_obj(s)
            db.session.add(s)
            db.session.commit()
            flash('Settings saved!', 'success')
            return redirect(url_for('settings'))
        return render_template('settings.html', form=form, test_form=test_form)

    # Test email endpoint
    @app.route('/test-email', methods=['POST'])
    def send_test_email():
        s = Settings.query.first()
        if not s:
            flash('Please save settings first.', 'warning')
            return redirect(url_for('settings'))
        form = TestEmailForm()
        if form.validate_on_submit():
            to_addr = form.test_email.data
            subject = '📧 Plex Notifier Test Email'
            html    = '<p>If you can read this, your email settings are correct!</p>'
            try:
                _send_email(s, to_addr, subject, html)
                flash(f'Test email sent to {to_addr}!', 'success')
            except Exception as e:
                flash(f'Failed to send test email: {e}', 'danger')
        else:
            for err in form.test_email.errors:
                flash(err, 'danger')
        return redirect(url_for('settings'))

    # Start scheduler-based polling (every 30 minutes)
    start_scheduler(app)

    return app
