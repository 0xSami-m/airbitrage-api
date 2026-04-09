#!/usr/bin/env python3
"""
book_virgin_atlantic.py — Automated Virgin Atlantic Flying Club award booking via Playwright CDP.

Usage:
    python3 book_virgin_atlantic.py \
        --origin LHR --dest JFK --date 2026-04-17 \
        --first Sami --last Muduroglu --dob 2003-04-23 \
        --cabin economy --card 2002 --booking-id 42

Key design decisions (Alaska-style):
  - Navigate DIRECTLY to search URL with all params baked in (no form filling)
  - Always open a FRESH page (avoids state pollution from prior runs)
  - Handle login wall when redirected to identity.virginatlantic.com
  - Login submit button is #continue (not #next) on VA's Azure B2C
  - Cabin selection on results page uses native Playwright locator clicks
  - All waits use domcontentloaded or selector-based (NOT networkidle — VA never reaches it)
  - page.evaluate() uses arrow functions only (function keyword causes SyntaxError)
  - Passenger DOB/FF# are pre-filled from VA account for returning members
"""

import argparse
import asyncio
import json
import os
import re
import sys

# ── Config ────────────────────────────────────────────────────────────────────
BILLING = {
    "address": "69 Brown St",
    "city":    "Providence",
    "state":   "RI",
    "zip":     "02912",
    "country": "US",
}

CARDS = {
    "2002": {
        "label":        "amex platinum",
        "number":       "",
        "expiry_month": "12",
        "expiry_year":  "2030",
        "cvv":          "7393",
        "name":         "Sami Muduroglu",
    },
}

DRY_RUN     = os.environ.get("DRY_RUN", "false").lower() == "true"
VA_EMAIL    = "samimuduroglu1@gmail.com"
VA_PASSWORD = "Rthj9bdx"
CDP_PORT    = 3012


# ── Notifier ──────────────────────────────────────────────────────────────────
def _notify_appa(text: str):
    hook_url   = os.environ.get("APPA_HOOK_URL",   "https://hooks.airbitrage.io/hooks/wake")
    hook_token = os.environ.get("APPA_HOOK_TOKEN", "flightdash-hook-token-2026")
    try:
        import urllib.request
        req = urllib.request.Request(
            hook_url, data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {hook_token}"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"[notify_appa] failed: {e}")


# ── DB helpers ─────────────────────────────────────────────────────────────────
def _update_booking(booking_id, status, ref=None):
    if not booking_id:
        return
    import sqlite3
    db_path = os.path.join(os.path.dirname(__file__), "vault.db")
    conn = sqlite3.connect(db_path)
    if ref:
        conn.execute("UPDATE bookings SET status=?, airline_ref=? WHERE id=?", (status, ref, booking_id))
    else:
        conn.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))
    conn.commit()
    conn.close()
    print(f"[book_va] Updated booking #{booking_id} → {status}" + (f", ref={ref}" if ref else ""))


# ── Main booking coroutine ─────────────────────────────────────────────────────
async def book_virgin_atlantic(
    origin, dest, date, first, last, dob,
    cabin="economy", card_last4="2002", booking_id=None,
):
    notes = []

    def note(msg):
        print(f"[book_va] {msg}")
        notes.append(msg)

    from playwright.async_api import async_playwright

    dob_parts = dob.split("-")
    dob_year, dob_month, dob_day = dob_parts[0], dob_parts[1], dob_parts[2]

    cabin_lower = cabin.lower()
    cabin_map = {
        "economy":         "Economy",
        "premium":         "Premium",
        "premium economy": "Premium",
        "business":        "Upper Class",
        "upper class":     "Upper Class",
        "first":           "Upper Class",
    }
    cabin_label = cabin_map.get(cabin_lower, "Economy")

    # What text appears in the results-page cabin buttons
    cabin_btn_texts = {
        "Economy":     ["Economy Classic", "Economy Delight", "Economy"],
        "Premium":     ["Premium"],
        "Upper Class": ["Upper Class"],
    }
    target_cabin_texts = cabin_btn_texts.get(cabin_label, ["Economy Classic", "Economy"])

    # Modal Select button index (0=Economy, 1=Premium, 2=Upper Class)
    cabin_select_index = {"Economy": 0, "Premium": 1, "Upper Class": 2}.get(cabin_label, 0)

    card_info = CARDS.get(card_last4, CARDS["2002"])

    async with async_playwright() as pw:
        # ── Connect to OpenClaw browser via CDP ───────────────────────────────
        try:
            browser = await pw.chromium.connect_over_cdp(f"http://localhost:{CDP_PORT}")
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
            note(f"✅ Connected to CDP on port {CDP_PORT}")
        except Exception as e:
            note(f"❌ CDP connection failed: {e}")
            raise

        # Always open a FRESH page to avoid state pollution
        page = await context.new_page()
        note("Opened fresh page")

        # ── Step 1: Navigate directly to search URL (Alaska-style) ────────────
        # No form filling needed — VA accepts all params in the URL
        search_url = (
            f"https://www.virginatlantic.com/en-US/flights/search/slice"
            f"?passengers=a1t0c0i0"
            f"&origin={origin}"
            f"&awardSearch=true"
            f"&destination={dest}"
            f"&departing={date}"
            f"&CTA=AbTest_SP_Flights"
        )
        note(f"Step 1: Navigating directly to search URL")
        note(f"  {search_url}")
        await page.goto(search_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        note(f"  URL after load: {page.url}")

        # ── Step 2: Handle login wall if redirected ───────────────────────────
        if "identity.virginatlantic.com" in page.url:
            note("Step 2: Login wall hit — filling VA Flying Club credentials")
            try:
                await page.fill('#signInName', VA_EMAIL, timeout=8000)
                note("  Filled email")
                await page.fill('#password', VA_PASSWORD, timeout=5000)
                note("  Filled password")
                # Submit button id is #continue on VA's Azure B2C (NOT #next)
                await page.locator('#continue').click(timeout=8000)
                note("  Clicked Continue — waiting for redirect to search results")
                await page.wait_for_url("**/flights/search**", timeout=30000)
                await page.wait_for_timeout(3000)
                note(f"  Post-login URL: {page.url}")
            except Exception as e:
                note(f"  ⚠️ Login error: {e}")
                await page.screenshot(path="/tmp/va_login_error.png")
                raise Exception(f"Login wall failed: {e}")
        else:
            note("Step 2: Already logged in — no redirect")

        # ── Step 3: Wait for flight results ──────────────────────────────────
        note("Step 3: Waiting for flight results to load")
        try:
            await page.wait_for_selector(
                "button:has-text('Economy'), button:has-text('Upper Class'), button:has-text('pts'), button:has-text('miles')",
                timeout=30000
            )
            note("  Results loaded ✅")
        except Exception:
            note("  ⚠️ Timed out waiting for results — taking screenshot")
        await page.wait_for_timeout(2000)
        note(f"  Results URL: {page.url}")
        await page.screenshot(path="/tmp/va_step3_results.png")
        note("  Screenshot: /tmp/va_step3_results.png")

        # ── Step 4: Select cabin on results page ──────────────────────────────
        note(f"Step 4: Selecting cabin '{cabin_label}'")
        selected = False
        for cabin_text in target_cabin_texts:
            try:
                btn_loc = page.locator(f"button:has-text('{cabin_text}')").first
                if await btn_loc.is_visible(timeout=3000):
                    text = await btn_loc.inner_text()
                    await btn_loc.click(timeout=5000)
                    note(f"  ✅ Clicked: '{text.strip()[:60]}'")
                    selected = True
                    break
            except Exception as e:
                note(f"  ⚠️ Cabin '{cabin_text}' error: {e}")

        if not selected:
            await page.screenshot(path="/tmp/va_step4_nocabin.png")
            raise Exception(f"Could not find cabin button for {cabin_label}")

        await page.wait_for_timeout(2000)
        await page.screenshot(path="/tmp/va_step4_modal.png")
        note("  Screenshot: /tmp/va_step4_modal.png")

        # ── Step 5: Click Select in cabin modal ───────────────────────────────
        note(f"Step 5: Clicking Select in cabin modal (index {cabin_select_index})")
        try:
            select_btns = page.locator("button:has-text('Select')")
            count = await select_btns.count()
            note(f"  Found {count} Select buttons")
            if count > cabin_select_index:
                await select_btns.nth(cabin_select_index).click(timeout=5000)
                note("  ✅ Select clicked")
            else:
                await select_btns.first.click(timeout=5000)
                note("  ✅ Select clicked (first fallback)")
        except Exception as e:
            note(f"  ⚠️ Select button error: {e}")

        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        await page.wait_for_timeout(3000)
        note(f"  Post-select URL: {page.url}")
        await page.screenshot(path="/tmp/va_step5_summary.png")
        note("  Screenshot: /tmp/va_step5_summary.png")

        # ── Step 6: Continue from flight summary ──────────────────────────────
        note("Step 6: Flight summary — clicking Continue to passenger details")
        try:
            cont = page.locator("button:has-text('Continue to passenger details')").first
            if await cont.is_visible(timeout=5000):
                await cont.click(timeout=5000)
                note("  ✅ Continue clicked")
            else:
                # fallback
                await page.locator("button:has-text('Continue')").first.click(timeout=5000)
                note("  ✅ Continue clicked (fallback)")
        except Exception as e:
            note(f"  ⚠️ Continue error: {e}")

        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        await page.wait_for_timeout(2000)
        note(f"  URL: {page.url}")
        await page.screenshot(path="/tmp/va_step6_passenger.png")
        note("  Screenshot: /tmp/va_step6_passenger.png")

        # ── Step 7: Fill passenger details ────────────────────────────────────
        # For returning VA members, most fields are pre-populated from the account.
        # We still try to fill in case it's a new session / different passenger.
        note(f"Step 7: Filling passenger — {first} {last}, DOB {dob}")

        # Title
        for sel in ["select[name='title']", "select[id='title']"]:
            try:
                await page.select_option(sel, value="MR", timeout=2000)
                note("  Set title: Mr")
                break
            except Exception:
                pass

        # First name
        for sel in ["input[name='firstName']", "input[id='firstName']", "[placeholder*='First']"]:
            try:
                current = await page.locator(sel).first.input_value(timeout=2000)
                if not current:
                    await page.fill(sel, first, timeout=3000)
                    note(f"  Set first name")
                else:
                    note(f"  First name pre-filled: '{current}'")
                break
            except Exception:
                continue

        # Last name
        for sel in ["input[name='lastName']", "input[id='lastName']", "[placeholder*='Last']"]:
            try:
                current = await page.locator(sel).first.input_value(timeout=2000)
                if not current:
                    await page.fill(sel, last, timeout=3000)
                    note(f"  Set last name")
                else:
                    note(f"  Last name pre-filled: '{current}'")
                break
            except Exception:
                continue

        # DOB — try dropdowns (VA uses day/month/year selects)
        # For returning members these are pre-filled from account
        for day_sel in ["select[name='dobDay']", "select[id='dobDay']"]:
            try:
                current = await page.locator(day_sel).first.input_value(timeout=2000)
                if not current or current == "0":
                    await page.select_option(day_sel, value=str(int(dob_day)), timeout=2000)
                    note(f"  Set DOB day: {dob_day}")
                else:
                    note(f"  DOB day pre-filled: {current}")
                break
            except Exception:
                pass

        for mon_sel in ["select[name='dobMonth']", "select[id='dobMonth']"]:
            try:
                current = await page.locator(mon_sel).first.input_value(timeout=2000)
                if not current or current == "0":
                    await page.select_option(mon_sel, value=str(int(dob_month)), timeout=2000)
                    note(f"  Set DOB month: {dob_month}")
                else:
                    note(f"  DOB month pre-filled: {current}")
                break
            except Exception:
                pass

        for yr_sel in ["select[name='dobYear']", "select[id='dobYear']"]:
            try:
                current = await page.locator(yr_sel).first.input_value(timeout=2000)
                if not current or current == "0":
                    await page.select_option(yr_sel, value=dob_year, timeout=2000)
                    note(f"  Set DOB year: {dob_year}")
                else:
                    note(f"  DOB year pre-filled: {current}")
                break
            except Exception:
                pass

        # Email
        for sel in ["input[name='email']", "input[type='email']"]:
            try:
                current = await page.locator(sel).first.input_value(timeout=2000)
                if not current:
                    await page.fill(sel, VA_EMAIL, timeout=3000)
                    note("  Set email")
                else:
                    note(f"  Email pre-filled: '{current}'")
                break
            except Exception:
                continue

        await page.wait_for_timeout(500)
        await page.screenshot(path="/tmp/va_step7_passenger_filled.png")
        note("  Screenshot: /tmp/va_step7_passenger_filled.png")

        # ── DRY RUN STOP ──────────────────────────────────────────────────────
        if DRY_RUN:
            note("🧪 DRY RUN — stopping before payment. Screenshots in /tmp/va_step*.png")
            _notify_appa(
                f"🧪 VA DRY RUN complete\n"
                f"Route: {origin} → {dest} | {date} | {cabin_label}\n"
                f"Passenger: {first} {last}\n"
                f"Reached passenger page ✅"
            )
            _update_booking(booking_id, "dry_run")
            print("\n=== DRY RUN NOTES ===")
            for n in notes:
                print(f"  {n}")
            return "DRY_RUN"

        # ── Step 8: Continue to next step ─────────────────────────────────────
        note("Step 8: Continuing past passenger details")
        try:
            cont = page.locator("button:has-text('Continue')").first
            await cont.click(timeout=5000)
            note("  ✅ Continue clicked")
        except Exception as e:
            note(f"  ⚠️ Continue error: {e}")

        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        await page.wait_for_timeout(2000)

        # ── Step 9: Seat selection (skip) ─────────────────────────────────────
        note("Step 9: Skipping seat selection")
        for skip_text in ["Skip", "No thanks", "Continue without"]:
            try:
                btn = page.locator(f"button:has-text('{skip_text}')").first
                if await btn.is_visible(timeout=3000):
                    await btn.click(timeout=3000)
                    note(f"  Skipped seats: '{skip_text}'")
                    break
            except Exception:
                pass

        await page.wait_for_load_state("domcontentloaded", timeout=10000)
        await page.wait_for_timeout(1500)

        # ── Step 10: Extras (skip) ────────────────────────────────────────────
        note("Step 10: Skipping extras")
        for skip_text in ["Skip", "No thanks", "Continue"]:
            try:
                btn = page.locator(f"button:has-text('{skip_text}')").first
                if await btn.is_visible(timeout=3000):
                    await btn.click(timeout=3000)
                    note(f"  Skipped extras: '{skip_text}'")
                    break
            except Exception:
                pass

        await page.wait_for_load_state("domcontentloaded", timeout=10000)
        await page.wait_for_timeout(1500)
        note(f"  Pre-payment URL: {page.url}")
        await page.screenshot(path="/tmp/va_step10_payment.png")
        note("  Screenshot: /tmp/va_step10_payment.png")

        # ── Step 11: Payment ──────────────────────────────────────────────────
        note("Step 11: Payment — filling billing details")

        for sel in ["input[name='addressLine1']", "input[name='address1']", "[placeholder*='Address']"]:
            try:
                await page.fill(sel, BILLING["address"], timeout=3000)
                note(f"  Billing address set")
                break
            except Exception:
                continue

        for sel in ["input[name='city']", "[placeholder*='City']"]:
            try:
                await page.fill(sel, BILLING["city"], timeout=3000)
                note("  City set")
                break
            except Exception:
                continue

        for sel in ["input[name='postcode']", "input[name='zipCode']", "input[name='zip']", "[placeholder*='Zip']", "[placeholder*='Post']"]:
            try:
                await page.fill(sel, BILLING["zip"], timeout=3000)
                note("  ZIP set")
                break
            except Exception:
                continue

        for sel in ["select[name='country']", "select[name='billingCountry']"]:
            try:
                await page.select_option(sel, label="United States", timeout=3000)
                note("  Country set to US")
                break
            except Exception:
                try:
                    await page.select_option(sel, value="US", timeout=2000)
                    note("  Country set to US (value)")
                    break
                except Exception:
                    pass

        # Try selecting saved card
        for sel in [f"button:has-text('{card_info['label']}')", f"label:has-text('{card_last4}')"]:
            try:
                await page.click(sel, timeout=3000)
                note(f"  Selected saved card")
                break
            except Exception:
                pass

        # CVV
        for sel in ["input[name='cvv']", "input[name='cvc']", "input[name='securityCode']", "[placeholder*='CVV']"]:
            try:
                await page.fill(sel, card_info["cvv"], timeout=3000)
                note("  CVV set")
                break
            except Exception:
                continue

        # T&Cs
        for sel in ["input[type='checkbox'][name*='term']", "input[type='checkbox'][id*='term']"]:
            try:
                cb = await page.query_selector(sel)
                if cb and not await cb.is_checked():
                    await cb.click()
                    note("  Accepted T&Cs")
            except Exception:
                pass

        await page.screenshot(path="/tmp/va_step11_payment_filled.png")
        note("  Screenshot: /tmp/va_step11_payment_filled.png")

        # ── Step 12: Pay ──────────────────────────────────────────────────────
        note("Step 12: Clicking Pay now")
        booked = False
        for pay_text in ["Pay now", "Confirm and pay", "Book now", "Complete booking", "Pay"]:
            try:
                btn = page.locator(f"button:has-text('{pay_text}')").first
                if await btn.is_visible(timeout=2000) and not await btn.is_disabled(timeout=2000):
                    await btn.click(timeout=8000)
                    note(f"  ✅ Clicked: '{pay_text}'")
                    booked = True
                    break
            except Exception:
                continue

        if not booked:
            raise Exception("Could not click Pay/Book now button")

        # ── Step 13: Confirmation ─────────────────────────────────────────────
        note("Step 13: Waiting for booking confirmation")
        try:
            await page.wait_for_url("**/confirmation**", timeout=90000)
        except Exception:
            try:
                await page.wait_for_selector("text=Booking reference", timeout=30000)
            except Exception:
                pass

        await page.wait_for_timeout(2000)
        content = await page.content()
        codes = re.findall(r'\b([A-Z0-9]{6})\b', content)
        false_pos = {"VIRGIN", "ATLANT", "FLYING", "BUSINE", "SELECT", "POINTS", "SEARCH", "REWARD"}
        codes = [c for c in codes if c not in false_pos and not c.isdigit()]
        confirmation = codes[0] if codes else "UNKNOWN"
        note(f"✅ BOOKED! Ref: {confirmation}")

        await page.screenshot(path=f"/tmp/va_confirmation_{confirmation}.png")
        _update_booking(booking_id, "confirmed", confirmation)
        _notify_appa(
            f"✅ VA booking confirmed!\n"
            f"Route: {origin}→{dest} | {date} | {cabin_label}\n"
            f"Passenger: {first} {last}\nRef: {confirmation}"
        )
        return confirmation


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin",     required=True)
    parser.add_argument("--dest",       required=True)
    parser.add_argument("--date",       required=True)
    parser.add_argument("--first",      required=True)
    parser.add_argument("--last",       required=True)
    parser.add_argument("--dob",        required=True)
    parser.add_argument("--cabin",      default="economy")
    parser.add_argument("--card",       default="2002")
    parser.add_argument("--booking-id", default=None, type=int)
    parser.add_argument("--dry-run",    action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        os.environ["DRY_RUN"] = "true"

    try:
        result = asyncio.run(book_virgin_atlantic(
            origin=args.origin.upper(), dest=args.dest.upper(),
            date=args.date, first=args.first, last=args.last, dob=args.dob,
            cabin=args.cabin, card_last4=args.card, booking_id=args.booking_id,
        ))
        print(json.dumps({"confirmation": result, "status": "confirmed" if result != "DRY_RUN" else "dry_run"}))
    except Exception as e:
        print(f"[book_va] ❌ FAILED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
