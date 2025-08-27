from __future__ import annotations


def normalize_email(email: str | None) -> str:
    """Return a canonical identifier for the email.

    Only the local-part is kept so that records aren't duplicated when
    domains differ. The result is lowercased; ``None`` becomes an empty string.
    """
    if not email:
        return ""
    return email.split("@")[0].lower()
