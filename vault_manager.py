"""
vault_manager.py
Manages a pool of pre-loaded Aeroplan "vault" accounts.

Each vault account holds a pre-purchased miles balance. When a customer
books a flight, we pick the vault with enough miles, log in, and book on
their behalf. Air Canada allows booking tickets for other passengers.

Schema
------
vault_accounts:
    id              INTEGER PK
    email           TEXT UNIQUE
    password        TEXT
    aeroplan_number TEXT
    miles_balance   INTEGER     -- last known balance
    status          TEXT        -- 'active' | 'suspended' | 'low'
    last_used_at    TIMESTAMP
    created_at      TIMESTAMP

bookings:
    id              INTEGER PK
    vault_id        INTEGER FK → vault_accounts.id
    passenger_name  TEXT
    flight_ref      TEXT        -- seats.aero availability id
    aeroplan_ref    TEXT        -- Aeroplan booking reference
    miles_used      INTEGER
    taxes_paid      REAL
    status          TEXT        -- 'pending' | 'confirmed' | 'failed'
    created_at      TIMESTAMP
"""

import sqlite3
import time
from pathlib import Path
from typing import Optional

DB_PATH = str(Path(__file__).parent / "vault.db")

LOW_BALANCE_THRESHOLD = 10_000  # mark 'low' when below this


# ── DB setup ──────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS vault_accounts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                email           TEXT UNIQUE NOT NULL,
                password        TEXT NOT NULL,
                aeroplan_number TEXT,
                miles_balance   INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'active',
                last_used_at    TIMESTAMP,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS bookings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                vault_id        INTEGER NOT NULL REFERENCES vault_accounts(id),
                passenger_name  TEXT,
                flight_ref      TEXT,
                aeroplan_ref    TEXT,
                miles_used      INTEGER DEFAULT 0,
                taxes_paid      REAL DEFAULT 0,
                status          TEXT DEFAULT 'pending',
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)


init_db()


# ── Vault account management ──────────────────────────────────────────────────

def add_vault(email: str, password: str, aeroplan_number: str, miles_balance: int = 0) -> int:
    """Add a new vault account. Returns the new row id."""
    with _conn() as c:
        cur = c.execute(
            """INSERT INTO vault_accounts (email, password, aeroplan_number, miles_balance)
               VALUES (?, ?, ?, ?)""",
            (email, password, aeroplan_number, miles_balance),
        )
        print(f"[vault] Added account {email} ({aeroplan_number}) with {miles_balance:,} miles")
        return cur.lastrowid


def update_balance(vault_id: int, new_balance: int):
    """Update miles balance after a query or booking."""
    status = "low" if new_balance < LOW_BALANCE_THRESHOLD else "active"
    with _conn() as c:
        c.execute(
            "UPDATE vault_accounts SET miles_balance=?, status=? WHERE id=?",
            (new_balance, status, vault_id),
        )
    print(f"[vault] Account {vault_id} balance updated to {new_balance:,} ({status})")


def mark_used(vault_id: int):
    """Record that a vault was just used."""
    with _conn() as c:
        c.execute(
            "UPDATE vault_accounts SET last_used_at=CURRENT_TIMESTAMP WHERE id=?",
            (vault_id,),
        )


def list_vaults() -> list[dict]:
    """Return all vault accounts."""
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM vault_accounts ORDER BY miles_balance DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_vault(vault_id: int) -> Optional[dict]:
    """Get a single vault by id."""
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM vault_accounts WHERE id=?", (vault_id,)
        ).fetchone()
    return dict(row) if row else None


def pick_vault(miles_needed: int) -> Optional[dict]:
    """
    Pick the best vault account for a booking.

    Strategy:
    - Must be 'active' status
    - Must have >= miles_needed
    - Prefer the one with the LOWEST balance that still covers the booking
      (preserves high-balance accounts for larger redemptions)
    - Break ties by least recently used
    """
    with _conn() as c:
        row = c.execute(
            """SELECT * FROM vault_accounts
               WHERE status = 'active'
                 AND miles_balance >= ?
               ORDER BY miles_balance ASC, last_used_at ASC NULLS FIRST
               LIMIT 1""",
            (miles_needed,),
        ).fetchone()
    if row:
        print(f"[vault] Selected account {row['email']} ({row['miles_balance']:,} miles) for {miles_needed:,} mile booking")
    else:
        print(f"[vault] No vault found with {miles_needed:,} miles available")
    return dict(row) if row else None


# ── Booking records ───────────────────────────────────────────────────────────

def create_booking(vault_id: int, passenger_name: str, flight_ref: str, miles_used: int, taxes_paid: float) -> int:
    """Create a pending booking record. Returns booking id."""
    with _conn() as c:
        cur = c.execute(
            """INSERT INTO bookings (vault_id, passenger_name, flight_ref, miles_used, taxes_paid)
               VALUES (?, ?, ?, ?, ?)""",
            (vault_id, passenger_name, flight_ref, miles_used, taxes_paid),
        )
        return cur.lastrowid


def confirm_booking(booking_id: int, aeroplan_ref: str, actual_miles_used: int):
    """Mark a booking confirmed and deduct miles from vault."""
    with _conn() as c:
        row = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
        if not row:
            raise ValueError(f"Booking {booking_id} not found")

        c.execute(
            "UPDATE bookings SET status='confirmed', aeroplan_ref=?, miles_used=? WHERE id=?",
            (aeroplan_ref, actual_miles_used, booking_id),
        )
        # Deduct miles from vault
        c.execute(
            "UPDATE vault_accounts SET miles_balance = miles_balance - ?, last_used_at=CURRENT_TIMESTAMP WHERE id=?",
            (actual_miles_used, row["vault_id"]),
        )
        # Re-check status
        vault = c.execute("SELECT miles_balance FROM vault_accounts WHERE id=?", (row["vault_id"],)).fetchone()
        if vault and vault["miles_balance"] < LOW_BALANCE_THRESHOLD:
            c.execute("UPDATE vault_accounts SET status='low' WHERE id=?", (row["vault_id"],))

    print(f"[vault] Booking {booking_id} confirmed (ref: {aeroplan_ref}), {actual_miles_used:,} miles deducted")


def fail_booking(booking_id: int, reason: str = ""):
    """Mark a booking as failed."""
    with _conn() as c:
        c.execute("UPDATE bookings SET status='failed' WHERE id=?", (booking_id,))
    print(f"[vault] Booking {booking_id} failed: {reason}")


def list_bookings(vault_id: int = None) -> list[dict]:
    """List bookings, optionally filtered by vault."""
    with _conn() as c:
        if vault_id:
            rows = c.execute(
                "SELECT * FROM bookings WHERE vault_id=? ORDER BY created_at DESC", (vault_id,)
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM bookings ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


# ── Summary ───────────────────────────────────────────────────────────────────

def vault_summary() -> dict:
    """Return a summary of the vault pool."""
    with _conn() as c:
        total = c.execute("SELECT COUNT(*), COALESCE(SUM(miles_balance),0) FROM vault_accounts WHERE status='active'").fetchone()
        low = c.execute("SELECT COUNT(*) FROM vault_accounts WHERE status='low'").fetchone()
        pending = c.execute("SELECT COUNT(*) FROM bookings WHERE status='pending'").fetchone()
        confirmed = c.execute("SELECT COUNT(*) FROM bookings WHERE status='confirmed'").fetchone()
    return {
        "active_accounts": total[0],
        "total_miles": total[1],
        "low_balance_accounts": low[0],
        "pending_bookings": pending[0],
        "confirmed_bookings": confirmed[0],
    }
