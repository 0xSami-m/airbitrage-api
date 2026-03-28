"""
stripe_issuing.py — Create and manage per-booking virtual cards via Stripe Issuing.

Each booking gets a unique Visa virtual card with a tight spending limit.
The card is cancelled immediately after the booking completes.
Air Canada sees a normal Visa card with no link to any other booking.
"""

import os
import stripe
from typing import Optional

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

# Stripe Issuing requires a cardholder to be created once per account.
# We create one "business" cardholder and reuse it for all virtual cards.
CARDHOLDER_ID = os.getenv("STRIPE_CARDHOLDER_ID")  # set after first run


def get_or_create_cardholder(name: str = "Travel Agent Services", email: str = None) -> str:
    """
    Returns the Stripe cardholder ID to use for virtual card creation.
    Run this once during setup — store the returned ID in .env as STRIPE_CARDHOLDER_ID.
    """
    if CARDHOLDER_ID:
        return CARDHOLDER_ID

    cardholder = stripe.issuing.Cardholder.create(
        name=name,
        email=email or os.getenv("STRIPE_CARDHOLDER_EMAIL", "billing@yourtravelagency.com"),
        phone_number=os.getenv("STRIPE_CARDHOLDER_PHONE", "+12025551234"),
        status="active",
        type="individual",
        billing={
            "address": {
                "line1": os.getenv("STRIPE_BILLING_LINE1", "123 Main St"),
                "city": os.getenv("STRIPE_BILLING_CITY", "New York"),
                "state": os.getenv("STRIPE_BILLING_STATE", "NY"),
                "postal_code": os.getenv("STRIPE_BILLING_ZIP", "10001"),
                "country": os.getenv("STRIPE_BILLING_COUNTRY", "US"),
            }
        },
    )
    print(f"[stripe] Created cardholder: {cardholder.id}")
    print(f"[stripe] Add to .env: STRIPE_CARDHOLDER_ID={cardholder.id}")
    return cardholder.id


def create_virtual_card(
    spending_limit_cents: int,
    label: str = "booking",
    cardholder_id: Optional[str] = None,
) -> dict:
    """
    Creates a single-use virtual Visa card with a tight spending limit.

    Args:
        spending_limit_cents: Max amount the card can be charged, in cents.
                              Set this to taxes + miles cost + 10% buffer.
        label: Human-readable label for this card (e.g. "PTY-CUR-2026-03-26")
        cardholder_id: Stripe cardholder ID. Falls back to STRIPE_CARDHOLDER_ID env var.

    Returns:
        {
            "card_id": "ic_xxx",
            "number": "4242424242424242",
            "exp_month": 12,
            "exp_year": 2026,
            "cvv": "123",
            "spending_limit_cents": 15000,
        }
    """
    ch_id = cardholder_id or CARDHOLDER_ID
    if not ch_id:
        ch_id = get_or_create_cardholder()

    card = stripe.issuing.Card.create(
        cardholder=ch_id,
        currency="usd",
        type="virtual",
        status="active",
        spending_controls={
            "spending_limits": [
                {
                    "amount": spending_limit_cents,
                    "interval": "per_authorization",
                }
            ],
            "allowed_categories": ["airlines", "travel_agencies_tour_operators"],
        },
        metadata={"label": label},
    )

    # Retrieve card number + CVV (requires stripe.issuing.Card.retrieve with expand)
    card_details = stripe.issuing.Card.retrieve(
        card.id,
        expand=["number", "cvc"],
    )

    print(f"[stripe] Created virtual card {card.id} — limit: ${spending_limit_cents/100:.2f} — label: {label}")

    return {
        "card_id": card.id,
        "number": card_details.number,
        "exp_month": card_details.exp_month,
        "exp_year": card_details.exp_year,
        "cvv": card_details.cvc,
        "spending_limit_cents": spending_limit_cents,
    }


def cancel_virtual_card(card_id: str) -> bool:
    """
    Cancels (permanently deactivates) a virtual card after use.
    Call this once the booking is confirmed.
    """
    try:
        stripe.issuing.Card.modify(card_id, status="canceled")
        print(f"[stripe] Cancelled virtual card {card_id}")
        return True
    except stripe.error.StripeError as e:
        print(f"[stripe] Failed to cancel card {card_id}: {e}")
        return False


def format_card_for_aeroplan(card: dict) -> dict:
    """
    Converts Stripe card dict into the format expected by aeroplan_login / miles_buyer.
    """
    return {
        "card_number": card["number"],
        "expiry": f"{card['exp_month']:02d}/{str(card['exp_year'])[-2:]}",
        "cvv": card["cvv"],
        "cardholder_name": os.getenv("STRIPE_CARDHOLDER_NAME", "Travel Agent Services"),
    }
