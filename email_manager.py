"""
email_manager.py
Manages client email addresses and inbound 2FA code retrieval using Mailgun sandbox.
"""

import os
import re
import sqlite3
import time
import random
import string
import warnings
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

MAILGUN_API_KEY    = os.getenv("MAILGUN_API_KEY", "")
MAILGUN_DOMAIN     = os.getenv("MAILGUN_DOMAIN", "")
DB_PATH            = os.getenv("EMAIL_DB_PATH", str(Path(__file__).parent / "client_emails.db"))

if not MAILGUN_API_KEY:
    warnings.warn("MAILGUN_API_KEY is not set — inbound email features will not work.")
if not MAILGUN_DOMAIN:
    warnings.warn("MAILGUN_DOMAIN is not set — client email addresses will be incomplete.")


# ── Database setup ────────────────────────────────────────────────────────────

def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't already exist."""
    with _get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS client_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                aeroplan_number TEXT,
                aeroplan_password TEXT
            );

            CREATE TABLE IF NOT EXISTS inbound_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                to_email TEXT NOT NULL,
                code TEXT NOT NULL,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                used INTEGER DEFAULT 0
            );
        """)


# Initialize tables on import
init_db()


# ── Email address management ──────────────────────────────────────────────────

def _random_suffix(length=4):
    """Generate a short random alphanumeric suffix."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def create_client_email(first_name: str, last_name: str) -> str:
    """
    Generate a unique email address for a client and persist it in SQLite.

    Format: firstname.lastname.XXXX@<MAILGUN_DOMAIN>
    where XXXX is a random 4-character suffix to ensure uniqueness.

    Returns the email address string.
    """
    first = re.sub(r"[^a-z0-9]", "", first_name.lower())
    last  = re.sub(r"[^a-z0-9]", "", last_name.lower())

    # Retry a few times in case of a collision (extremely unlikely)
    for _ in range(10):
        suffix = _random_suffix(4)
        email  = f"{first}.{last}.{suffix}@{MAILGUN_DOMAIN}"

        try:
            with _get_conn() as conn:
                conn.execute(
                    "INSERT INTO client_emails (email, first_name, last_name) VALUES (?, ?, ?)",
                    (email, first_name, last_name),
                )
            print(f"[email_manager] Created client email: {email}")
            return email
        except sqlite3.IntegrityError:
            # Collision — try again with a different suffix
            continue

    raise RuntimeError("Failed to generate a unique email after 10 attempts.")


def get_client_by_email(email: str) -> dict:
    """Return the full client record for an email, or None if not found."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM client_emails WHERE email = ?", (email,)
        ).fetchone()
    return dict(row) if row else None


def update_aeroplan_credentials(email: str, aeroplan_number: str, aeroplan_password: str):
    """Persist Aeroplan credentials against the client email record."""
    with _get_conn() as conn:
        conn.execute(
            "UPDATE client_emails SET aeroplan_number = ?, aeroplan_password = ? WHERE email = ?",
            (aeroplan_number, aeroplan_password, email),
        )


# ── Inbound 2FA code handling ─────────────────────────────────────────────────

def store_inbound_code(to_email: str, code: str):
    """
    Store a 6-digit code received for a given email address.
    Called by the /api/inbound-email webhook handler.
    """
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO inbound_codes (to_email, code) VALUES (?, ?)",
            (to_email.lower().strip(), code),
        )
    print(f"[email_manager] Stored code {code} for {to_email}")


def wait_for_code(email_address: str, timeout: int = 120) -> str:
    """
    Poll the inbound_codes table every 2 seconds until a fresh, unused code
    arrives for the given email address (or timeout is reached).

    Marks the code as used before returning it.
    Returns the 6-digit code string.
    Raises TimeoutError if no code arrives within `timeout` seconds.
    """
    email = email_address.lower().strip()
    deadline = time.time() + timeout
    # Record when we started waiting so we only pick up codes that arrive after this point
    started_at = time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[email_manager] Waiting for 2FA code to {email} (timeout={timeout}s)...")

    while time.time() < deadline:
        with _get_conn() as conn:
            row = conn.execute(
                """SELECT id, code FROM inbound_codes
                   WHERE to_email = ?
                     AND used = 0
                     AND received_at >= ?
                   ORDER BY received_at DESC
                   LIMIT 1""",
                (email, started_at),
            ).fetchone()

            if row:
                conn.execute(
                    "UPDATE inbound_codes SET used = 1 WHERE id = ?", (row["id"],)
                )
                print(f"[email_manager] Got code {row['code']} for {email}")
                return row["code"]

        time.sleep(2)

    raise TimeoutError(f"No 2FA code received for {email} within {timeout} seconds.")
