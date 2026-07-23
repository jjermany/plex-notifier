import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("SECRET_KEY", "testing-secret-key")

from notifier_app.config import Notification, db
from notifier_app.notifier import (
    TracearrHistorySnapshot,
    _get_users,
    _notification_already_sent,
    _tracearr_get_all,
    _user_has_history,
    _user_has_watched_newer_episode,
    _user_has_watched_show,
)


class TracearrSettings:
    watch_history_source = "tracearr"
    tracearr_url = "http://tracearr.test"
    tracearr_api_key = "trr_pub_secret"
    plex_url = "http://plex.test"
    plex_token = "plex-token"


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakePlexUser:
    def __init__(self, email, username):
        self.email = email
        self.username = username


class FakePlexAccount(FakePlexUser):
    def __init__(self, email, username, users):
        super().__init__(email, username)
        self._users = users

    def users(self):
        return self._users


class FakePlexServer:
    def myPlexAccount(self):
        return FakePlexAccount(
            "owner@example.com",
            "Owner",
            [FakePlexUser("viewer@example.com", "Viewer")],
        )


class TracearrSupportTests(unittest.TestCase):
    def test_public_api_paginates_with_bearer_auth(self):
        settings = TracearrSettings()
        captured = []

        def fake_get(url, headers=None, params=None, timeout=None):
            captured.append((url, dict(headers or {}), dict(params or {}), timeout))
            page = params["page"]
            data = [{"id": str(index)} for index in range(100)] if page == 1 else [{"id": "last"}]
            return DummyResponse(
                {"data": data, "meta": {"total": 101, "page": page, "pageSize": 100}}
            )

        with patch("notifier_app.notifier.requests.get", side_effect=fake_get):
            rows = _tracearr_get_all(settings, "users")

        self.assertEqual(len(rows), 101)
        self.assertEqual([call[2]["page"] for call in captured], [1, 2])
        self.assertEqual(
            captured[0][1]["Authorization"],
            f"Bearer {settings.tracearr_api_key}",
        )
        self.assertEqual(
            captured[0][0],
            "http://tracearr.test/api/v1/public/users",
        )

    def test_maps_plex_recipient_to_tracearr_identity(self):
        settings = TracearrSettings()
        snapshot = TracearrHistorySnapshot(
            users=[
                {"id": "identity-owner", "username": "Owner", "displayName": "Owner"},
                {"id": "identity-viewer", "username": "Viewer", "displayName": "Viewer"},
            ],
            history=[],
        )

        with patch("notifier_app.notifier.PlexServer", return_value=FakePlexServer()):
            users = _get_users(settings, tracearr_snapshot=snapshot)

        self.assertEqual(
            users,
            [
                {
                    "user_id": "identity-owner",
                    "username": "Owner",
                    "email": "owner@example.com",
                },
                {
                    "user_id": "identity-viewer",
                    "username": "Viewer",
                    "email": "viewer@example.com",
                },
            ],
        )

    def test_cross_platform_history_drives_episode_checks(self):
        settings = TracearrSettings()
        snapshot = TracearrHistorySnapshot(
            users=[],
            history=[
                {
                    "user": {"id": "identity-viewer", "username": "Viewer"},
                    "mediaType": "episode",
                    "showTitle": "Star Trek: Strange New Worlds",
                    "seasonNumber": 2,
                    "episodeNumber": 4,
                    "year": 2022,
                    "watched": True,
                }
            ],
        )

        watched, status = _user_has_watched_show(
            settings,
            "identity-viewer",
            "unused-plex-key",
            show_title="Star Trek Strange New Worlds",
            show_year=2022,
            tracearr_snapshot=snapshot,
        )
        self.assertTrue(watched)
        self.assertEqual(status, "available")
        self.assertTrue(
            _user_has_history(
                settings,
                "identity-viewer",
                "unused-episode-key",
                show_title="Star Trek Strange New Worlds",
                show_year=2022,
                season=2,
                episode=4,
                tracearr_snapshot=snapshot,
            )
        )
        self.assertTrue(
            _user_has_watched_newer_episode(
                settings,
                "identity-viewer",
                "unused-show-key",
                2,
                3,
                show_title="Star Trek Strange New Worlds",
                show_year=2022,
                tracearr_snapshot=snapshot,
            )
        )

    def test_complete_notification_ledger_blocks_old_duplicate(self):
        app = Flask(__name__)
        app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(app)

        episode = SimpleNamespace(
            grandparentRatingKey="show-1",
            grandparentGuid="plex://show-1",
            grandparentTitle="Example Show",
            grandparentYear=2020,
            parentIndex=1,
            index=2,
            ratingKey="episode-12",
        )

        with app.app_context():
            db.create_all()
            db.session.add(
                Notification(
                    email="viewer@example.com",
                    show_title="Example Show",
                    show_key="show-1",
                    show_guid="plex://show-1",
                    plex_guid="plex://show-1",
                    season=1,
                    episode=2,
                    episode_key="episode-12",
                )
            )
            db.session.commit()

            self.assertTrue(
                _notification_already_sent(
                    "VIEWER@example.com",
                    episode,
                    show_guid="plex://show-1",
                    guid_candidates=["plex://show-1"],
                )
            )


if __name__ == "__main__":
    unittest.main()
