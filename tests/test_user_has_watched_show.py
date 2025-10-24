import unittest
from unittest.mock import patch

from notifier_app.notifier import _user_has_watched_show


class DummySettings:
    tautulli_url = "http://tautulli.test"
    tautulli_api_key = "secret"


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class UserHasWatchedShowTests(unittest.TestCase):
    def test_paginates_and_handles_affirmative_status_values(self):
        settings = DummySettings()
        target_key = "123"
        page_length = 1000
        total_records = (page_length * 2) + 1
        captured_params = []

        def make_payload(items):
            return {
                "response": {
                    "data": {
                        "recordsFiltered": total_records,
                        "data": items,
                    }
                }
            }

        unrelated_item = {
            "grandparent_rating_key": "999",
            "watched_status": 0,
        }
        unrelated_page = [unrelated_item] * page_length
        final_page = [
            {
                "grandparent_rating_key": target_key,
                "watched_status": "Played",
            }
        ]

        def fake_get(url, params=None, timeout=None):
            captured_params.append(dict(params or {}))
            self.assertEqual(url, f"{settings.tautulli_url.rstrip('/')}/api/v2")
            self.assertEqual(params.get("grandparent_rating_key"), target_key)
            start = params.get("start", 0)
            if start == 0:
                payload = make_payload(unrelated_page)
            elif start == page_length:
                payload = make_payload(unrelated_page)
            elif start == page_length * 2:
                payload = make_payload(final_page)
            else:
                payload = make_payload([])
            return DummyResponse(payload)

        with patch("notifier_app.notifier.requests.get", side_effect=fake_get):
            self.assertTrue(
                _user_has_watched_show(settings, user_id=42, grandparent_rating_key=target_key)
            )

        # Ensure the function paginated through all expected slices
        starts = [params.get("start") for params in captured_params]
        self.assertIn(0, starts)
        self.assertIn(page_length, starts)
        self.assertIn(page_length * 2, starts)
        for params in captured_params:
            self.assertEqual(params.get("length"), page_length)

    def test_handles_truncated_pages_until_records_filtered_consumed(self):
        settings = DummySettings()
        target_key = "456"
        page_length = 1000
        total_records = 250
        captured_params = []

        def make_payload(items):
            return {
                "response": {
                    "data": {
                        "recordsFiltered": total_records,
                        "data": items,
                    }
                }
            }

        def fake_get(url, params=None, timeout=None):
            captured_params.append(dict(params or {}))
            start = params.get("start", 0)
            if start == 0:
                items = [{"grandparent_rating_key": target_key, "watched_status": 0}] * 100
            elif start == 100:
                items = [{"grandparent_rating_key": target_key, "watched_status": 0}] * 100
            elif start == 200:
                items = [
                    {
                        "grandparent_rating_key": target_key,
                        "watched_status": "watched",
                    }
                ]
            else:
                items = []
            return DummyResponse(make_payload(items))

        with patch("notifier_app.notifier.requests.get", side_effect=fake_get):
            self.assertTrue(
                _user_has_watched_show(settings, user_id=99, grandparent_rating_key=target_key)
            )

        starts = [params.get("start") for params in captured_params]
        self.assertEqual(starts[:3], [0, 100, 200])
        for params in captured_params:
            self.assertEqual(params.get("length"), page_length)


if __name__ == "__main__":
    unittest.main()
