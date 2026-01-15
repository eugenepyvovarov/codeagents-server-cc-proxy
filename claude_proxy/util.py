from __future__ import annotations

import re


_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def sanitize_id(value: str) -> str:
    value = value.strip()
    if not value or not _ID_RE.match(value):
        raise ValueError("Invalid id format.")
    return value


sanitize_session_id = sanitize_id


def parse_int(value: str | None, *, default: int) -> int | None:
    if value is None:
        return default
    value = value.strip()
    if value == "":
        return default
    try:
        parsed = int(value, 10)
    except ValueError:
        return None
    if parsed < 0:
        return None
    return parsed
