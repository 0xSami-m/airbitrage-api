#!/usr/bin/env python3
"""
book_alaska.py — Automated Alaska Mileage Plan award booking via Playwright CDP.

Usage:
    python3 book_alaska.py \
        --origin JFK --dest LAX --date 2026-04-07 \
        --first Imran --last Trehan --dob 1998-03-06 \
        --cabin business --card 2002

All shadow DOM / web-component workarounds are encoded here so we never have
to rediscover them mid-booking.
"""

import argparse
import asyncio
import json
import sys
import time

# ── Config ────────────────────────────────────────────────────────────────────
BILLING = {
    "address": "69 Brown St",
    "city":    "Providence",
    "state":   "RI",
    "zip":     "02912",
}

# Map last-4 → internal label shown on Alaska checkout
CARDS = {
    "1004": "Sami's card",
    "2002": "amex platinum",
}

CDP_PORT  = 9222          # OpenClaw browser CDP port
HEADLESS  = False

# ── Shadow DOM helpers (injected as JS) ───────────────────────────────────────
FIND_IN_SHADOW_JS = """
function findInShadow(root, sel) {
    const el = root.querySelector(sel);
    if (el) return el;
    for (const child of root.querySelectorAll('*')) {
        if (child.shadowRoot) {
            const found = findInShadow(child.shadowRoot, sel);
            if (found) return found;
        }
    }
    return null;
}
function findAllInShadow(root, sel, results) {
    root.querySelectorAll(sel).forEach(e => results.push(e));
    for (const child of root.querySelectorAll('*')) {
        if (child.shadowRoot) findAllInShadow(child.shadowRoot, sel, results);
    }
}
"""

SET_INPUT_JS = FIND_IN_SHADOW_JS + """
function setInput(el, val) {
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    setter.call(el, val);
    el.dispatchEvent(new InputEvent('input', {bubbles:true, inputType:'insertText', data:val}));
    el.dispatchEvent(new Event('change', {bubbles:true}));
}
function setInputByName(name, val) {
    const all = [];
    findAllInShadow(document, 'input', all);
    const el = all.find(i => i.name === name);
    if (!el) return 'nf:' + name;
    setInput(el, val);
    return 'ok:' + el.value;
}
function setSelectByName(name, val) {
    const all = [];
    findAllInShadow(document, 'select', all);
    const el = all.find(i => i.name === name);
    if (!el) return 'nf:' + name;
    el.value = val;
    el.dispatchEvent(new Event('change', {bubbles:true}));
    return 'ok:' + el.value;
}
"""


async def book_alaska(
    origin: str,
    dest: str,
    date: str,           # YYYY-MM-DD
    first: str,
    last: str,
    dob: str,            # YYYY-MM-DD
    cabin: str = "business",
    card_last4: str = "2002",
    no_insurance: bool = True,
    booking_id: int = None,
):
    """
    Full Alaska award booking flow. Returns confirmation code string or raises.
    """
    from playwright.async_api import async_playwright

    # Parse DOB
    dob_parts = dob.split("-")
    dob_month = str(int(dob_parts[1]))   # "3" not "03"
    dob_day   = str(int(dob_parts[2]))
    dob_year  = dob_parts[0]

    # Format date for Alaska URL: MM/DD/YYYY
    date_parts = date.split("-")
    date_formatted = f"{date_parts[1]}/{date_parts[2]}/{date_parts[0]}"

    # Alaska cabin → seat class param
    cabin_map = {"economy": "Y", "premium": "W", "business": "F", "first": "F"}
    seat_class = cabin_map.get(cabin.lower(), "F")

    async with async_playwright() as pw:
        # Connect to OpenClaw's managed browser via CDP
        try:
            browser = await pw.chromium.connect_over_cdp(f"http://localhost:{CDP_PORT}")
            print(f"[book_alaska] Connected to CDP on port {CDP_PORT}")
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
        except Exception:
            print(f"[book_alaska] CDP connection failed, launching new browser")
            context = await pw.chromium.launch_persistent_context(
                user_data_dir="/Users/samimuduroglu/.openclaw/browser-profiles/alaska-booking",
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            browser = None  # persistent context has no separate browser object

        page = await context.new_page()

        # ── Step 1: Go to search page ─────────────────────────────────────────
        search_url = (
            f"https://www.alaskaair.com/search/"
            f"?O={origin}&D={dest}&DT={date}&TT=OW&A=1&SC={seat_class}&PTS=true"
        )
        print(f"[book_alaska] Navigating to: {search_url}")
        await page.goto(search_url, wait_until="networkidle")

        # Dismiss cookie banner if present
        try:
            await page.click("button:has-text('Dismiss')", timeout=3000)
        except Exception:
            pass

        # ── Step 2: Check if logged in, skip MFA if prompted ──────────────────
        if "login" in page.url or "auth0" in page.url:
            print("[book_alaska] Redirected to login — checking MFA skip")

        try:
            await page.click("button:has-text('Skip for now')", timeout=4000)
            print("[book_alaska] Skipped MFA prompt")
            await page.goto(search_url, wait_until="networkidle")
        except Exception:
            pass

        # ── Step 3: Fill date via shadow DOM ──────────────────────────────────
        print(f"[book_alaska] Setting date: {date_formatted}")
        await page.evaluate(f"""
            {FIND_IN_SHADOW_JS}
            const inp = findInShadow(document, 'input[placeholder="MM/DD/YYYY"]');
            if (inp) {{
                inp.click(); inp.focus();
                const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                setter.call(inp, '{date_formatted}');
                inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                inp.dispatchEvent(new Event('change', {{bubbles:true}}));
            }}
        """)

        # ── Step 4: Check "Use points" ────────────────────────────────────────
        await page.evaluate(f"""
            {FIND_IN_SHADOW_JS}
            const all = [];
            findAllInShadow(document, 'input[type="checkbox"]', all);
            const pts = all.find(c => c.name === 'points-input');
            if (pts && !pts.checked) pts.click();
        """)

        # ── Step 5: Select one-way ─────────────────────────────────────────────
        await page.evaluate(f"""
            {FIND_IN_SHADOW_JS}
            const all = [];
            findAllInShadow(document, 'input[type="radio"]', all);
            const ow = all.find(r => r.value === 'OW' || r.name === 'trip-type-OW');
            if (ow && !ow.checked) ow.click();
        """)

        # ── Step 6: Search ────────────────────────────────────────────────────
        print("[book_alaska] Clicking Search flights")
        await page.evaluate(f"""
            {FIND_IN_SHADOW_JS}
            const all = [];
            findAllInShadow(document, 'button', all);
            const btn = all.find(b => b.textContent.trim() === 'Search flights');
            if (btn) btn.click();
        """)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # ── Step 7: Pick best nonstop business seat ────────────────────────────
        print("[book_alaska] Selecting best nonstop business seat")
        # Look for nonstop first, then any
        selected = False
        for selector in [
            f"button:has-text('{cabin.capitalize()} '):not([disabled])",
            f"button:has-text('Business '):not([disabled])",
            f"button:has-text('First '):not([disabled])",
        ]:
            try:
                # Get all matching buttons, prefer nonstop
                buttons = await page.query_selector_all(selector)
                if buttons:
                    # Click the first one (results sorted by stops then price)
                    await buttons[0].click()
                    selected = True
                    print(f"[book_alaska] Selected fare: {await buttons[0].text_content()}")
                    break
            except Exception:
                continue

        if not selected:
            raise Exception("No business award seats found for this route/date")

        await page.wait_for_load_state("networkidle")

        # ── Step 8: Add to cart ───────────────────────────────────────────────
        print("[book_alaska] Adding to cart")
        try:
            await page.click("button:has-text('Add to cart')", timeout=8000)
        except Exception:
            # May have already proceeded to cart page
            pass
        await page.wait_for_load_state("networkidle")

        # ── Step 9: Continue to checkout ──────────────────────────────────────
        print("[book_alaska] Continuing to checkout")
        try:
            await page.click("button:has-text('Continue to checkout')", timeout=8000)
        except Exception:
            pass
        await page.wait_for_load_state("networkidle")

        # ── Step 10: Fill passenger info ──────────────────────────────────────
        print(f"[book_alaska] Filling passenger: {first} {last}, DOB {dob}")
        await page.evaluate(f"""
            {SET_INPUT_JS}
            setInputByName('travelerInfo[0].firstName', '{first}');
            setInputByName('travelerInfo[0].middleName', '');
            setInputByName('travelerInfo[0].lastName', '{last}');
            setSelectByName('travelerInfo[0].dateOfBirthMonth', '{dob_month}');
            setInputByName('travelerInfo[0].dateOfBirthDay', '{dob_day}');
            setInputByName('travelerInfo[0].dateOfBirthYear', '{dob_year}');
            // Clear loyalty number to avoid name mismatch
            setSelectByName('travelerInfo[0].loyaltyProgram', '');
            setInputByName('travelerInfo[0].loyaltyNumber', '');
        """)

        await page.wait_for_timeout(500)

        # Click Continue
        print("[book_alaska] Submitting passenger info")
        await page.evaluate(f"""
            {FIND_IN_SHADOW_JS}
            const all = [];
            findAllInShadow(document, 'button', all);
            const btn = all.find(b => b.textContent.trim() === 'Continue');
            if (btn) btn.click();
        """)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1000)

        # Check for errors
        error_el = await page.query_selector("text=doesn't match")
        if error_el:
            # Re-clear loyalty and retry
            print("[book_alaska] Loyalty mismatch — clearing and retrying")
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setSelectByName('travelerInfo[0].loyaltyProgram', '');
                setInputByName('travelerInfo[0].loyaltyNumber', '');
            """)
            await page.evaluate(f"""
                {FIND_IN_SHADOW_JS}
                const all = [];
                findAllInShadow(document, 'button', all);
                const btn = all.find(b => b.textContent.trim() === 'Continue');
                if (btn) btn.click();
            """)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(1000)

        # ── Step 11: Review & Pay — select insurance No ───────────────────────
        print("[book_alaska] Selecting no insurance")
        await page.evaluate(f"""
            {FIND_IN_SHADOW_JS}
            const all = [];
            findAllInShadow(document, 'input[type="radio"]', all);
            const ins = all.filter(r => r.name === 'TripInsuranceAWP');
            // Second radio = No
            if (ins.length >= 2) ins[ins.length - 1].click();
        """)

        # ── Step 12: Select payment card ──────────────────────────────────────
        print(f"[book_alaska] Selecting card ending {card_last4}")
        card_label = CARDS.get(card_last4, card_last4)
        try:
            # Cards are regular buttons on this page
            await page.click(f"button:has-text('{card_label}')", timeout=5000)
        except Exception:
            print(f"[book_alaska] Could not find card '{card_label}', trying by last 4")
            await page.click(f"button:has-text('{card_last4}')", timeout=5000)

        await page.wait_for_timeout(500)

        # ── Step 13: Fill billing address using aria refs ─────────────────────
        print("[book_alaska] Filling billing address")
        # Use type action (keyboard events) which sticks reliably
        addr_input = page.get_by_label("Address line 1")
        try:
            await addr_input.click(timeout=3000)
            await addr_input.type(BILLING["address"])
        except Exception:
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setInputByName('addressLineOne', '{BILLING["address"]}');
            """)

        city_input = page.get_by_label("City")
        try:
            await city_input.click(timeout=3000)
            await city_input.type(BILLING["city"])
        except Exception:
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setInputByName('city', '{BILLING["city"]}');
            """)

        # State dropdown
        await page.evaluate(f"""
            {SET_INPUT_JS}
            setSelectByName('state', '{BILLING["state"]}');
        """)

        zip_input = page.get_by_label("Zip code")
        try:
            await zip_input.click(timeout=3000)
            await zip_input.type(BILLING["zip"])
        except Exception:
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setInputByName('zipCode', '{BILLING["zip"]}');
            """)

        await page.wait_for_timeout(500)

        # ── Step 14: Click Book now ───────────────────────────────────────────
        print("[book_alaska] Clicking Book now")
        # Must use interactive snapshot to find the button in web component
        # Try direct Playwright first, then fall back to evaluate
        booked = False
        for attempt in range(3):
            try:
                btn = page.get_by_role("button", name="Book now")
                is_disabled = await btn.is_disabled(timeout=3000)
                if is_disabled:
                    print(f"[book_alaska] Book now disabled (attempt {attempt+1}), waiting...")
                    await page.wait_for_timeout(1500)
                    continue
                await btn.click(timeout=5000)
                booked = True
                break
            except Exception as e:
                print(f"[book_alaska] Book now attempt {attempt+1} failed: {e}")
                await page.wait_for_timeout(1000)

        if not booked:
            raise Exception("Could not click Book now button after 3 attempts")

        # ── Step 15: Wait for confirmation ────────────────────────────────────
        print("[book_alaska] Waiting for confirmation...")
        await page.wait_for_url("**/confirmation**", timeout=60000)
        await page.wait_for_load_state("networkidle")

        # ── Step 16: Extract confirmation code ────────────────────────────────
        content = await page.content()
        # Alaska confirmation is typically 6 uppercase alphanumeric chars
        import re
        codes = re.findall(r'\b([A-Z0-9]{6})\b', content)
        # Filter common false positives
        false_positives = {"ALASKA", "AMERIC", "POINTS", "BUSINE", "SELECT"}
        codes = [c for c in codes if c not in false_positives]

        confirmation = codes[0] if codes else "UNKNOWN"
        print(f"[book_alaska] ✅ BOOKED! Confirmation: {confirmation}")
        print(f"[book_alaska] All codes found: {codes[:5]}")

        await page.screenshot(path=f"/tmp/booking_confirmation_{confirmation}.png")

        # Write confirmation back to vault.db
        if booking_id:
            import sqlite3, os
            db_path = os.path.join(os.path.dirname(__file__), "vault.db")
            conn = sqlite3.connect(db_path)
            conn.execute(
                "UPDATE bookings SET status=?, airline_ref=? WHERE id=?",
                ("confirmed", confirmation, booking_id)
            )
            conn.commit()
            conn.close()
            print(f"[book_alaska] Updated booking_id={booking_id} → confirmed, ref={confirmation}")

        return confirmation


def main():
    parser = argparse.ArgumentParser(description="Book Alaska award flight")
    parser.add_argument("--origin",  required=True)
    parser.add_argument("--dest",    required=True)
    parser.add_argument("--date",    required=True, help="YYYY-MM-DD")
    parser.add_argument("--first",   required=True)
    parser.add_argument("--last",    required=True)
    parser.add_argument("--dob",     required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin",      default="business")
    parser.add_argument("--card",       default="2002", help="Last 4 of card")
    parser.add_argument("--booking-id", default=None,   type=int)
    args = parser.parse_args()

    confirmation = asyncio.run(book_alaska(
        origin     = args.origin.upper(),
        dest       = args.dest.upper(),
        date       = args.date,
        first      = args.first,
        last       = args.last,
        dob        = args.dob,
        cabin      = args.cabin,
        card_last4 = args.card,
        booking_id = args.booking_id,
    ))

    print(json.dumps({"confirmation": confirmation, "status": "confirmed"}))


if __name__ == "__main__":
    main()
