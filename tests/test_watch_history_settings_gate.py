import os
import re
from unittest.mock import patch

os.environ.setdefault("SECRET_KEY", "testing-secret-key-that-is-long-enough")

from flask import Flask as RealFlask

from notifier_app import webapp
from notifier_app.config import Settings


def test_settings_save_requires_matching_connection_proof(monkeypatch, tmp_path):
    monkeypatch.setenv("SECRET_KEY", "testing-secret-key-that-is-long-enough")
    monkeypatch.setattr(
        webapp,
        "Flask",
        lambda *args, **kwargs: RealFlask(
            *args,
            instance_path=str(tmp_path),
            **kwargs,
        ),
    )
    monkeypatch.setattr(webapp, "start_scheduler", lambda app, interval: None)
    monkeypatch.setattr(
        webapp,
        "reconcile_notifications",
        lambda app, run_reason=None: None,
    )
    monkeypatch.setattr(
        webapp,
        "reconcile_user_preferences",
        lambda app, run_reason=None, cutoff_days=None: None,
    )

    app = webapp.create_app()
    app.config.update(TESTING=True)
    client = app.test_client()
    with client.session_transaction() as session:
        session["admin_authed"] = True

    settings_page = client.get("/settings")
    csrf_token = re.search(
        rb'name="csrf_token"[^>]*value="([^"]+)"',
        settings_page.data,
    ).group(1).decode()
    form_data = {
        "csrf_token": csrf_token,
        "save_action": "watch_history",
        "plex_url": "http://plex.test",
        "plex_token": "updated-token",
        "watch_history_source": "tracearr",
        "tautulli_url": "http://tautulli.test",
        "tautulli_api_key": "tautulli-key",
        "tracearr_url": "http://tracearr.test",
        "tracearr_api_key": "trr_pub_test",
        "notify_interval": "30",
    }

    blocked = client.post("/settings", data=form_data)
    assert blocked.status_code == 200
    assert b"Verify the selected watch history connection before saving it." in blocked.data
    with app.app_context():
        assert Settings.query.first().plex_token == "placeholder"
        assert Settings.query.first().watch_history_source == "tautulli"

    with patch(
        "notifier_app.webapp._test_watch_history_connection",
        return_value={
            "provider": "Tracearr",
            "version": "1.5.0",
            "message": "Connected to Tracearr 1.5.0.",
        },
    ):
        tested = client.post("/api/test-watch-history", data=form_data)
    assert tested.status_code == 200
    assert tested.get_json()["success"] is True

    saved = client.post("/settings", data=form_data)
    assert saved.status_code == 302
    with app.app_context():
        assert Settings.query.first().watch_history_source == "tracearr"
        assert Settings.query.first().tracearr_url == "http://tracearr.test"
        assert Settings.query.first().plex_token == "placeholder"

    with client.session_transaction() as session:
        session.pop("watch_history_connection_fingerprint", None)
    form_data["save_action"] = "general"
    general_saved = client.post("/settings", data=form_data)
    assert general_saved.status_code == 302
    with app.app_context():
        assert Settings.query.first().plex_token == "updated-token"
