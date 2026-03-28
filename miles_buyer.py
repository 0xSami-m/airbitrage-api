"""
miles_buyer.py
Purchases Aeroplan miles using a logged-in Playwright session.

Assumes `page` is already authenticated (returned by aeroplan_login.login()).
"""

import asyncio
import re
from typing import Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

BUY_MILES_URL = "https://www.aircanada.com/aeroplan/member/buy-miles"

# Aeroplan min/max purchase limits
MIN_MILES = 1_000
MAX_MILES = 150_000
MILES_STEP = 1_000  # must be a multiple of 1,000


async def buy_miles(page: Page, miles_needed: int, card: dict) -> dict:
    """
    Purchase Aeroplan miles using the provided payment card.

    Parameters
    ----------
    page         : An authenticated Playwright Page (from aeroplan_login.login()).
    miles_needed : Number of miles to purchase. Will be rounded up to the nearest
                   1,000 and clamped to [MIN_MILES, MAX_MILES].
    card         : dict with keys:
                     number     – card number (no spaces)
                     expiry_mm  – 2-digit month string, e.g. "03"
                     expiry_yy  – 2-digit year  string, e.g. "30"
                     cvv        – 3–4 digit security code
                     name       – cardholder name

    Returns
    -------
    dict: {"miles_bought": int, "cost_cad": float}

    Raises
    ------
    ValueError  – if miles_needed is outside purchasable bounds.
    RuntimeError – if the purchase flow fails.
    """
    # ── Normalize miles amount ────────────────────────────────────────────────
    miles = _round_up_miles(miles_needed)
    if miles < MIN_MILES:
        raise ValueError(f"Minimum purchase is {MIN_MILES:,} miles.")
    if miles > MAX_MILES:
        raise ValueError(f"Maximum purchase is {MAX_MILES:,} miles (requested {miles_needed:,}).")

    print(f"[miles_buyer] Navigating to buy-miles page to purchase {miles:,} miles")
    await page.goto(BUY_MILES_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    # ── Step 1: enter miles quantity ─────────────────────────────────────────
    miles_input_sel = 'input[name*="miles"], input[id*="miles"], input[placeholder*="miles" i]'
    try:
        await page.wait_for_selector(miles_input_sel, timeout=10000)
        await page.fill(miles_input_sel, str(miles))
    except PlaywrightTimeoutError:
        # Some versions use a slider or a quantity selector; fall back to a typed field
        for sel in ['input[type="number"]', 'input[name="quantity"]']:
            try:
                await page.wait_for_selector(sel, timeout=3000)
                await page.fill(sel, str(miles))
                break
            except PlaywrightTimeoutError:
                continue

    await page.wait_for_timeout(1000)

    # ── Step 2: capture estimated cost before proceeding ─────────────────────
    cost_cad: Optional[float] = None
    try:
        # Look for a price display element
        price_text = await page.inner_text(
            '[class*="total"], [class*="price"], [class*="cost"], [data-testid*="total"]'
        )
        amounts = re.findall(r"\$?([\d,]+\.?\d*)", price_text)
        if amounts:
            cost_cad = float(amounts[-1].replace(",", ""))
    except Exception:
        pass

    # ── Step 3: proceed to checkout / payment ────────────────────────────────
    print("[miles_buyer] Clicking Continue / Proceed to payment")
    for sel in [
        'button:has-text("Continue")',
        'button:has-text("Proceed")',
        'button:has-text("Next")',
        'button[type="submit"]',
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                break
        except Exception:
            continue

    await page.wait_for_timeout(3000)

    # ── Step 4: fill card details ─────────────────────────────────────────────
    print("[miles_buyer] Filling payment card details")

    # Card number — may be inside an iframe (hosted payment page)
    card_number_filled = False
    for frame in [page] + list(page.frames):
        for sel in [
            'input[name*="cardNumber"]',
            'input[id*="cardNumber"]',
            'input[autocomplete="cc-number"]',
            'input[placeholder*="card number" i]',
        ]:
            try:
                await frame.wait_for_selector(sel, timeout=3000)
                await frame.fill(sel, card["number"])
                card_number_filled = True
                break
            except Exception:
                continue
        if card_number_filled:
            break

    if not card_number_filled:
        raise RuntimeError("Could not locate the card number input field.")

    # Expiry month
    expiry_mm = card.get("expiry_mm", "")
    expiry_yy = card.get("expiry_yy", "")
    for frame in [page] + list(page.frames):
        for sel in ['select[name*="expMonth"]', 'input[name*="expMonth"]', 'input[autocomplete="cc-exp-month"]']:
            try:
                el = await frame.query_selector(sel)
                if el:
                    tag = await el.evaluate("el => el.tagName")
                    if tag.upper() == "SELECT":
                        await frame.select_option(sel, value=expiry_mm.lstrip("0") or expiry_mm)
                    else:
                        await frame.fill(sel, expiry_mm)
                    break
            except Exception:
                continue

    # Expiry year
    for frame in [page] + list(page.frames):
        for sel in ['select[name*="expYear"]', 'input[name*="expYear"]', 'input[autocomplete="cc-exp-year"]']:
            try:
                el = await frame.query_selector(sel)
                if el:
                    tag = await el.evaluate("el => el.tagName")
                    if tag.upper() == "SELECT":
                        # Try both 2-digit and 4-digit year
                        full_year = f"20{expiry_yy}" if len(expiry_yy) == 2 else expiry_yy
                        try:
                            await frame.select_option(sel, value=full_year)
                        except Exception:
                            await frame.select_option(sel, value=expiry_yy)
                    else:
                        await frame.fill(sel, expiry_yy)
                    break
            except Exception:
                continue

    # CVV — often inside the p-api.aircanada.com iframe
    cvv_filled = False
    for frame in [page] + list(page.frames):
        for sel in [
            'input[name*="cvv"]', 'input[name*="cvc"]', 'input[name*="securityCode"]',
            'input[autocomplete="cc-csc"]', 'input[id*="cvv"]',
        ]:
            try:
                await frame.wait_for_selector(sel, timeout=2000)
                await frame.fill(sel, card["cvv"])
                cvv_filled = True
                break
            except Exception:
                continue
        if cvv_filled:
            break

    # Cardholder name
    for frame in [page] + list(page.frames):
        for sel in [
            'input[name*="nameOnCard"]', 'input[name*="cardHolder"]',
            'input[autocomplete="cc-name"]', 'input[placeholder*="name on card" i]',
        ]:
            try:
                await frame.wait_for_selector(sel, timeout=2000)
                await frame.fill(sel, card["name"])
                break
            except Exception:
                continue

    # ── Step 5: submit purchase ───────────────────────────────────────────────
    print("[miles_buyer] Submitting payment")
    for sel in [
        'button:has-text("Purchase")',
        'button:has-text("Buy Miles")',
        'button:has-text("Confirm")',
        'button:has-text("Pay")',
        'button[type="submit"]',
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                break
        except Exception:
            continue

    await page.wait_for_timeout(5000)

    # ── Step 6: confirm success ──────────────────────────────────────────────
    page_text = await page.inner_text("body")
    success_phrases = [
        "thank you", "purchase complete", "miles have been added",
        "transaction successful", "confirmation",
    ]
    if not any(phrase in page_text.lower() for phrase in success_phrases):
        raise RuntimeError(
            f"Miles purchase may have failed — no confirmation text found. "
            f"Current URL: {page.url}"
        )

    # Try to read the final cost if not captured earlier
    if cost_cad is None:
        amounts = re.findall(r"\$?([\d,]+\.?\d*)", page_text)
        if amounts:
            cost_cad = float(amounts[-1].replace(",", ""))

    print(f"[miles_buyer] Purchase complete: {miles:,} miles, ~${cost_cad} CAD")
    return {"miles_bought": miles, "cost_cad": cost_cad or 0.0}


def _round_up_miles(miles: int) -> int:
    """Round up to the nearest 1,000."""
    return (miles + MILES_STEP - 1) // MILES_STEP * MILES_STEP
