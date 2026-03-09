import pytest
from pathlib import Path


@pytest.fixture
def app_client(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "testing-secret-key-that-is-long-enough-123")
    monkeypatch.setenv("WEBUI_USER", "admin")
    monkeypatch.setenv("WEBUI_PASS", "pass")

    from notifier_app import webapp

    monkeypatch.setattr(webapp, "start_scheduler", lambda app, interval: None)
    monkeypatch.setattr(webapp, "reconcile_notifications", lambda app, run_reason=None: None)
    monkeypatch.setattr(
        webapp,
        "reconcile_user_preferences",
        lambda app, run_reason=None, cutoff_days=None: None,
    )

    app = webapp.create_app()
    app.config.update(TESTING=True)

    with app.test_client() as client:
        with client.session_transaction() as session:
            session["admin_authed"] = True
        yield app, client


def test_admin_logs_tail_returns_recent_chunk(app_client):
    app, client = app_client

    log_path = Path(app.root_path).parent / "instance" / "logs" / "app.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    first_line = "A" * 1500
    second_line = "tail-line-visible"
    content = f"{first_line}\n{second_line}\n"
    log_path.write_text(content, encoding="utf-8")

    response = client.get("/api/admin/logs?file=app&offset=tail&max_bytes=1000")
    assert response.status_code == 200

    data = response.get_json()
    assert data["offset"] == len(content.encode("utf-8"))
    assert second_line in data["lines"]


def test_admin_logs_invalid_offset_falls_back_to_start(app_client):
    app, client = app_client
    log_path = Path(app.root_path).parent / "instance" / "logs" / "app.log"
    log_path.write_text("first\nsecond\n", encoding="utf-8")

    response = client.get("/api/admin/logs?file=app&offset=oops")
    assert response.status_code == 200

    data = response.get_json()
    assert data["lines"] == ["first", "second"]
