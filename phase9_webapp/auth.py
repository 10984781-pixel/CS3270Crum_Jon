"""Authentication helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from functools import wraps

from flask import redirect, session, url_for

HASH_ITERATIONS = 200000
DEFAULT_SALT = "cs3270_demo_salt"
DEFAULT_PASSWORD_HASH = "D/4n/TxU5sUdL5o/NMx4zjxQW6R6SpYaQLLXnxfvJF4="


def _password_hash(password: str, salt: str) -> str:
    password_bytes = password.encode("utf-8")
    salt_bytes = salt.encode("utf-8")
    digest = hashlib.pbkdf2_hmac("sha256", password_bytes, salt_bytes, HASH_ITERATIONS)
    return base64.b64encode(digest).decode("utf-8")


def verify_credentials(username: str, password: str) -> bool:
    expected_username = os.getenv("APP_USERNAME", "student")
    expected_hash = os.getenv("APP_PASSWORD_HASH", DEFAULT_PASSWORD_HASH)
    salt = os.getenv("APP_PASSWORD_SALT", DEFAULT_SALT)

    if username != expected_username:
        return False

    actual_hash = _password_hash(password, salt)
    return hmac.compare_digest(actual_hash, expected_hash)


def login_required(view_function):
    @wraps(view_function)
    def wrapped(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login_page"))
        return view_function(*args, **kwargs)

    return wrapped
