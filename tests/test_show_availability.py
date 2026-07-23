import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from flask import Flask

os.environ.setdefault("SECRET_KEY", "testing-secret-key")

from notifier_app import notifier
from notifier_app.config import Notification, Settings, ShowIdentity, db
from notifier_app.constants import SHOW_MISSING_GRACE_DAYS
from notifier_app.notifier import (
    _notification_show_group_key,
    _record_show_availability,
    reconcile_notifications,
)


def _app():
    app = Flask(__name__)
    app.config.update(
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    return app


def _notification(**overrides):
    values = {
        "email": "viewer@example.com",
        "show_title": "Example Show (2020)",
        "show_key": "show-1",
        "show_guid": "plex://show-1",
        "tvdb_id": "1234",
        "plex_guid": "plex://show-1",
        "season": 1,
        "episode": 1,
    }
    values.update(overrides)
    return Notification(**values)


def test_group_key_is_shared_by_episode_notifications():
    first = _notification(episode=1)
    second = _notification(episode=2, show_key="stale-key")

    assert _notification_show_group_key(first) == _notification_show_group_key(second)


def test_missing_show_transitions_to_removed_after_grace_period():
    app = _app()
    checked_at = datetime.now(timezone.utc)

    with app.app_context():
        db.create_all()
        notification = _notification()
        db.session.add(notification)
        db.session.commit()

        status, transitioned = _record_show_availability(
            notification,
            matched_show=None,
            checked_at=checked_at,
        )
        db.session.commit()

        assert (status, transitioned) == ("missing", True)
        identity = ShowIdentity.query.one()
        identity.missing_since = checked_at - timedelta(days=SHOW_MISSING_GRACE_DAYS)
        db.session.commit()

        status, transitioned = _record_show_availability(
            notification,
            matched_show=None,
            checked_at=checked_at,
        )

        assert (status, transitioned) == ("removed", True)
        assert Notification.query.count() == 1


def test_reappearing_show_clears_missing_state():
    app = _app()
    checked_at = datetime.now(timezone.utc)

    with app.app_context():
        db.create_all()
        notification = _notification()
        db.session.add(notification)
        db.session.commit()
        _record_show_availability(notification, matched_show=None, checked_at=checked_at)
        db.session.commit()

        status, transitioned = _record_show_availability(
            notification,
            matched_show=object(),
            checked_at=checked_at + timedelta(hours=1),
        )

        identity = ShowIdentity.query.one()
        assert (status, transitioned) == ("available", True)
        assert identity.missing_since is None
        assert identity.last_seen_at is not None


def test_reconciliation_checks_show_once_and_warns_only_on_transition(monkeypatch):
    app = _app()
    resolve_match = Mock(return_value=(None, "no_match"))
    fake_plex = SimpleNamespace(
        library=SimpleNamespace(section=lambda _name: object()),
    )
    monkeypatch.setattr(notifier, "PlexServer", lambda _url, _token: fake_plex)
    monkeypatch.setattr(notifier, "_resolve_show_match", resolve_match)

    with app.app_context():
        db.create_all()
        db.session.add(Settings(plex_url="http://plex", plex_token="token"))
        db.session.add_all(
            [
                _notification(email="one@example.com", episode=1),
                _notification(email="two@example.com", episode=2),
            ]
        )
        db.session.commit()

    warning = Mock()
    app.logger.warning = warning
    reconcile_notifications(app, run_reason="test")

    assert resolve_match.call_count == 1
    with app.app_context():
        assert Notification.query.count() == 2
        assert ShowIdentity.query.one().availability_status == "missing"
    assert any(
        "availability changed" in str(call.args[0])
        for call in warning.call_args_list
    )

    warning.reset_mock()
    resolve_match.reset_mock()
    reconcile_notifications(app, run_reason="test")

    assert resolve_match.call_count == 1
    assert not any(
        "availability changed" in str(call.args[0])
        for call in warning.call_args_list
    )
