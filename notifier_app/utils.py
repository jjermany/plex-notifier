from __future__ import annotations
import hashlib
import re


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


def normalize_show_identity(title: str | None, year: int | None = None) -> str:
    """Create a stable identifier for a show based on title/year."""
    if not title:
        return ""

    normalized_title = title.strip().lower()
    extracted_year = None
    year_match = re.search(r"\((\d{4})\)\s*$", normalized_title)
    if year_match:
        extracted_year = int(year_match.group(1))
        normalized_title = normalized_title[:year_match.start()].strip()

    normalized_title = re.sub(r"[^a-z0-9]+", "-", normalized_title).strip("-")
    show_year = year or extracted_year

    if show_year:
        return f"title:{normalized_title}|year:{show_year}"
    return f"title:{normalized_title}"
