"""
auth.py — JWT authentication for LinguaSQL using stdlib only.

Password hashing : PBKDF2-HMAC-SHA256 (hashlib) — 260,000 iterations, 32-byte salt
JWT              : HS256 signed with QM_JWT_SECRET env var (stdlib hmac + base64)
Token lifetime   : 30 days (configurable via QM_JWT_EXPIRE_DAYS)
"""

import os
import hmac
import hashlib
import base64
import json
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
_JWT_SECRET   = os.environ.get("QM_JWT_SECRET", secrets.token_hex(32))
_EXPIRE_DAYS  = int(os.environ.get("QM_JWT_EXPIRE_DAYS", "30"))
_PBKDF2_ITERS = 260_000
_PBKDF2_LEN   = 32


# ── Password hashing ──────────────────────────────────────────────────────────

def hash_password(plaintext: str) -> str:
    """Return 'pbkdf2:sha256:ITERATIONS:SALT_HEX:HASH_HEX'."""
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256", plaintext.encode(), salt.encode(),
        _PBKDF2_ITERS, dklen=_PBKDF2_LEN
    )
    return f"pbkdf2:sha256:{_PBKDF2_ITERS}:{salt}:{dk.hex()}"


def verify_password(plaintext: str, stored: str) -> bool:
    """Constant-time password verification."""
    try:
        _, algo, iters, salt, expected_hex = stored.split(":", 4)
        dk = hashlib.pbkdf2_hmac(
            algo, plaintext.encode(), salt.encode(),
            int(iters), dklen=_PBKDF2_LEN
        )
        return hmac.compare_digest(dk.hex(), expected_hex)
    except Exception:
        return False


# ── JWT (pure stdlib HS256) ────────────────────────────────────────────────────

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    pad = 4 - len(s) % 4
    return base64.urlsafe_b64decode(s + "=" * (pad % 4))


def create_token(user_id: int, email: str, name: str, role: str = "user") -> str:
    """Create a signed HS256 JWT token."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "email": email,
        "name": name,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=_EXPIRE_DAYS)).timestamp()),
    }
    header  = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body    = _b64url_encode(json.dumps(payload).encode())
    sig_input = f"{header}.{body}".encode()
    sig = hmac.new(_JWT_SECRET.encode(), sig_input, hashlib.sha256).digest()
    return f"{header}.{body}.{_b64url_encode(sig)}"


def decode_token(token: str) -> Optional[Dict]:
    """
    Verify and decode a JWT. Returns the payload dict or None if invalid/expired.
    """
    try:
        parts = token.strip().split(".")
        if len(parts) != 3:
            return None
        header, body, sig = parts
        sig_input = f"{header}.{body}".encode()
        expected  = hmac.new(_JWT_SECRET.encode(), sig_input, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64url_decode(sig)):
            return None
        payload = json.loads(_b64url_decode(body))
        # Check expiry
        if payload.get("exp", 0) < datetime.now(timezone.utc).timestamp():
            return None
        return payload
    except Exception:
        return None


# ── Database helpers ───────────────────────────────────────────────────────────

def init_users_table(db_path: str):
    """Create the users table if it does not exist."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            email        TEXT NOT NULL UNIQUE COLLATE NOCASE,
            password_hash TEXT NOT NULL,
            name         TEXT NOT NULL DEFAULT '',
            role         TEXT NOT NULL DEFAULT 'user',
            created_at   TEXT NOT NULL
        )
    """)
    # Add user_id column to query_history if missing
    try:
        conn.execute("ALTER TABLE query_history ADD COLUMN user_id INTEGER")
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.commit()
    conn.close()


def create_user(db_path: str, email: str, password: str, name: str) -> Tuple[bool, str, Optional[Dict]]:
    """
    Register a new user.
    Returns (success, error_message, user_dict).
    """
    if not email or "@" not in email:
        return False, "Invalid email address", None
    if len(password) < 8:
        return False, "Password must be at least 8 characters", None
    if not name.strip():
        return False, "Name is required", None

    pw_hash = hash_password(password)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.execute(
            "INSERT INTO users (email, password_hash, name, role, created_at) VALUES (?,?,?,?,?)",
            (email.lower().strip(), pw_hash, name.strip(), "user", now_str)
        )
        user_id = cur.lastrowid
        conn.commit()
        conn.close()
        return True, "", {"id": user_id, "email": email.lower().strip(),
                          "name": name.strip(), "role": "user"}
    except sqlite3.IntegrityError:
        return False, "An account with this email already exists", None
    except Exception as e:
        return False, str(e), None


def authenticate_user(db_path: str, email: str, password: str) -> Tuple[bool, str, Optional[Dict]]:
    """
    Verify email + password.
    Returns (success, error_message, user_dict).
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM users WHERE email = ? COLLATE NOCASE",
            (email.strip(),)
        ).fetchone()
        conn.close()
    except Exception as e:
        return False, str(e), None

    if not row:
        return False, "No account found with that email", None
    if not verify_password(password, row["password_hash"]):
        return False, "Incorrect password", None

    user = {"id": row["id"], "email": row["email"],
            "name": row["name"], "role": row["role"]}
    return True, "", user


def get_user_by_id(db_path: str, user_id: int) -> Optional[Dict]:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        conn.close()
        if row:
            return {"id": row["id"], "email": row["email"],
                    "name": row["name"], "role": row["role"]}
    except Exception:
        pass
    return None


def update_user(db_path: str, user_id: int,
                name: Optional[str] = None,
                password: Optional[str] = None) -> Tuple[bool, str]:
    """Update user name and/or password."""
    updates, params = [], []
    if name is not None:
        if not name.strip():
            return False, "Name cannot be empty"
        updates.append("name = ?"); params.append(name.strip())
    if password is not None:
        if len(password) < 8:
            return False, "Password must be at least 8 characters"
        updates.append("password_hash = ?"); params.append(hash_password(password))
    if not updates:
        return False, "Nothing to update"
    params.append(user_id)
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", params)
        conn.commit()
        conn.close()
        return True, ""
    except Exception as e:
        try: conn.close()
        except: pass
        return False, str(e)


# ── FastAPI dependency ─────────────────────────────────────────────────────────

def extract_bearer(authorization: Optional[str]) -> Optional[str]:
    """Pull the raw token string from 'Bearer <token>'."""
    if not authorization:
        return None
    parts = authorization.strip().split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def get_current_user(authorization: Optional[str], db_path: str) -> Optional[Dict]:
    """
    Decode the JWT from the Authorization header and return the user dict.
    Returns None in anonymous mode (no/invalid token).
    """
    token = extract_bearer(authorization)
    if not token:
        return None
    payload = decode_token(token)
    if not payload:
        return None
    try:
        user_id = int(payload["sub"])
    except (KeyError, ValueError):
        return None
    return get_user_by_id(db_path, user_id)
