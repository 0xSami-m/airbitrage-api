"""
stripe_checkout.py — Create Stripe Checkout sessions for customer payment.

Flow:
1. Frontend calls POST /api/create-checkout with flight + passenger details
2. We calculate total price (miles cost + taxes + service fee)
3. Create a Stripe Checkout session → return URL to frontend
4. Customer pays on Stripe-hosted page
5. Stripe fires webhook to POST /api/stripe-webhook
6. We trigger the full booking flow
"""

import os
import json
import stripe

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
SERVICE_FEE_CENTS = int(os.getenv("SERVICE_FEE_CENTS", "3500"))  # $35 default service fee
SUCCESS_URL = os.getenv("STRIPE_SUCCESS_URL", "http://localhost:5174/booking-success?session_id={CHECKOUT_SESSION_ID}")
CANCEL_URL = os.getenv("STRIPE_CANCEL_URL", "http://localhost:5174/booking-cancelled")


def calculate_total_cents(miles: int, taxes_usd: float, cpp_usd: float = None) -> dict:
    """
    Calculate what to charge the customer.

    Args:
        miles: Number of miles needed for the award
        taxes_usd: Aeroplan taxes in USD
        cpp_usd: Cost per mile in USD (defaults to current promo rate from .env)

    Returns breakdown dict with all amounts in cents.
    """
    cpp = cpp_usd or float(os.getenv("MILES_CPP_USD", "0.0144"))  # 1.44¢/mile default

    miles_cost_cents = int(miles * cpp * 100)
    taxes_cents = int(taxes_usd * 100)
    service_fee_cents = SERVICE_FEE_CENTS

    total_cents = miles_cost_cents + taxes_cents + service_fee_cents

    return {
        "miles_cost_cents": miles_cost_cents,
        "taxes_cents": taxes_cents,
        "service_fee_cents": service_fee_cents,
        "total_cents": total_cents,
        "miles": miles,
        "cpp_usd": cpp,
    }


def create_checkout_session(
    flight: dict,
    client: dict,
    miles: int,
    taxes_usd: float,
    availability_id: str,
) -> dict:
    """
    Creates a Stripe Checkout session.

    Args:
        flight: { origin, destination, date, cabin }
        client: { first_name, last_name, dob, passport_number, ... }
        miles: award miles needed
        taxes_usd: taxes in USD
        availability_id: seats.aero availability ID

    Returns:
        { checkout_url, session_id, breakdown }
    """
    breakdown = calculate_total_cents(miles, taxes_usd)

    origin = flight.get("origin", "")
    destination = flight.get("destination", "")
    date = flight.get("date", "")
    cabin = flight.get("cabin", "economy").title()
    name = f"{client.get('first_name', '')} {client.get('last_name', '')}"

    # Store booking details in Stripe metadata so we can retrieve them in webhook
    metadata = {
        "origin": origin,
        "destination": destination,
        "date": date,
        "cabin": flight.get("cabin", "economy"),
        "availability_id": availability_id,
        "miles": str(miles),
        "taxes_usd": str(taxes_usd),
        "client_json": json.dumps(client),
    }

    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"✈️ {origin} → {destination} | {date} | {cabin}",
                        "description": (
                            f"Passenger: {name} · "
                            f"{miles:,} Aeroplan miles + taxes · "
                            f"Nonstop"
                        ),
                    },
                    "unit_amount": breakdown["miles_cost_cents"] + breakdown["taxes_cents"],
                },
                "quantity": 1,
            },
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": "Service Fee",
                        "description": "Booking and processing fee",
                    },
                    "unit_amount": breakdown["service_fee_cents"],
                },
                "quantity": 1,
            },
        ],
        mode="payment",
        success_url=SUCCESS_URL,
        cancel_url=CANCEL_URL,
        customer_email=client.get("email"),
        metadata=metadata,
    )

    print(f"[stripe] Created checkout session {session.id} — total: ${breakdown['total_cents']/100:.2f}")

    return {
        "checkout_url": session.url,
        "session_id": session.id,
        "breakdown": breakdown,
    }


def verify_webhook(payload: bytes, sig_header: str) -> stripe.Event:
    """
    Verifies a Stripe webhook signature and returns the event.
    Raises stripe.error.SignatureVerificationError if invalid.
    """
    return stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
