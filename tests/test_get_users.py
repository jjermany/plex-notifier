import os
import sys
import unittest
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("SECRET_KEY", "testing-secret-key")

from notifier_app.notifier import _get_users


class DummySettings:
    tautulli_url = "http://tautulli.test"
    tautulli_api_key = "secret"
    plex_url = "http://plex.test"
    plex_token = "plex-token"
    from_address = "noreply@example.com"


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
    def __init__(self, url, token):
        self.url = url
        self.token = token

    def myPlexAccount(self):
        shared_user = FakePlexUser("shared@example.com", "SharedUser")
        return FakePlexAccount("owner@example.com", "Owner", [shared_user])


class GetUsersTests(unittest.TestCase):
    def test_filters_users_not_in_plex_whitelist(self):
        settings = DummySettings()

        tautulli_payload = {
            "response": {
                "data": [
                    {"user_id": 1, "username": "SharedUser", "email": "shared@example.com"},
                    {"user_id": 2, "username": "RemovedUser", "email": "removed@example.com"},
                    {"user_id": 3, "username": "NoEmailUser", "email": None},
                ]
            }
        }

        with patch("notifier_app.notifier.PlexServer", return_value=FakePlexServer(settings.plex_url, settings.plex_token)):
            with patch("notifier_app.notifier.requests.get", return_value=DummyResponse(tautulli_payload)):
                users = _get_users(settings)

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]["email"], "shared@example.com")
        self.assertEqual(users[0]["username"], "SharedUser")


if __name__ == "__main__":
    unittest.main()
