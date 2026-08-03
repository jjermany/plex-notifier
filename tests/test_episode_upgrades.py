import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

os.environ.setdefault("SECRET_KEY", "testing-secret-key")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from notifier_app.config import EpisodeFirstSeen, Settings, db
from notifier_app.notifier import check_new_episodes


class FakeEpisode:
    def __init__(self, *, added_at, updated_at):
        self.ratingKey = "replacement-rating-key"
        self.addedAt = added_at
        self.updatedAt = updated_at


def test_scheduled_check_ignores_upgrade_of_existing_episode():
    app = Flask(__name__)
    app.config.update(
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)

    now = datetime.now(timezone.utc)
    upgraded_episode = FakeEpisode(
        added_at=now - timedelta(days=365),
        updated_at=now,
    )
    section = SimpleNamespace(search=lambda **kwargs: [upgraded_episode])
    plex = SimpleNamespace(
        machineIdentifier="plex-server",
        library=SimpleNamespace(section=lambda name: section),
    )

    with app.app_context():
        db.create_all()
        db.session.add(
            Settings(
                plex_url="http://plex.test",
                plex_token="token",
                notify_interval=30,
            )
        )
        db.session.commit()

        with (
            patch("notifier_app.notifier.Episode", FakeEpisode),
            patch("notifier_app.notifier.PlexServer", return_value=plex),
            patch("notifier_app.notifier._get_users") as get_users,
        ):
            check_new_episodes(app)

        get_users.assert_not_called()
        assert db.session.get(EpisodeFirstSeen, "replacement-rating-key") is not None
