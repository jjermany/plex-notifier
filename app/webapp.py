from .notifier import start_scheduler

import os
from flask import Flask, render_template, redirect, url_for, flash
from .config import db, Settings
from .forms import SettingsForm

def create_app():
    # enable instance_relative_config so app.instance_path points at ./instance
    app = Flask(__name__, instance_relative_config=True)

    # 1) Make sure the instance folder exists
    os.makedirs(app.instance_path, exist_ok=True)

    # 2) Point SQLAlchemy at the absolute path inside instance/
    db_path = os.path.join(app.instance_path, 'config.sqlite3')
    app.config.from_mapping(
        SECRET_KEY=os.environ.get('SECRET_KEY', 'change-me'),
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{db_path}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False
    )

    db.init_app(app)

    # now create tables (will touch config.sqlite3)
    with app.app_context():
        db.create_all()

    @app.route('/', methods=['GET','POST'])
    def settings():
        s = Settings.query.first() or Settings()
        form = SettingsForm(obj=s)
        if form.validate_on_submit():
            form.populate_obj(s)
            db.session.add(s)
            db.session.commit()
            flash('Settings saved!', 'success')
            return redirect(url_for('settings'))
        return render_template('settings.html', form=form)
    start_scheduler(app)
    return app
