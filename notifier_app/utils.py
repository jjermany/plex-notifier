from __future__ import annotations
import hashlib


def normalize_email(email: str | None) -> str:
    """Return a canonical identifier for the email.

    Preserves the full email address to ensure users with same local part
    but different domains are treated as distinct users. The result is
    lowercased; ``None`` becomes an empty string.
    """
    if not email:
        return ""
    return email.lower().strip()


def email_to_filename(email: str) -> str:
    """Convert email address to a safe filename.

    Uses a hash of the email to create a consistent, filesystem-safe identifier.
    """
    if not email:
        return "unknown"
    normalized = normalize_email(email)
    # Use first 16 chars of SHA256 hash for reasonable uniqueness
    email_hash = hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]
    # Keep local part for readability, add hash for uniqueness
    local_part = normalized.split("@")[0] if "@" in normalized else normalized
    # Sanitize local part for filesystem
    safe_local = "".join(c if c.isalnum() or c in "-_" else "_" for c in local_part)
    return f"{safe_local}_{email_hash}"
