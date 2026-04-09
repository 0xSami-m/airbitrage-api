#!/usr/bin/env python3
"""
book_virgin_atlantic.py — Automated Virgin Atlantic Flying Club award booking via Playwright CDP.

Usage:
    python3 book_virgin_atlantic.py \
        --origin JFK --dest LHR --date 2026-05-15 \
        --first Sami --last Muduroglu --dob 1990-01-01 \
        --cabin business --card 2002 --booking-id 42

All shadow DOM / web-component workarounds are encoded here so we never have
to rediscover them mid-booking.
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

# Map last-4 → card details for entry on VA checkout
CARDS = {
    "2002": {
        "label":  "amex platinum",
        "number": "",        # card is saved on VA — no need to re-enter
        "expiry_month": "12",
        "expiry_year":  "2030",
        "cvv":    "7393",   # Amex Platinum CVV
        "name":   "Sami Muduroglu",
    },
}

# Set to True to stop just before clicking Pay (for test runs)
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

VA_EMAIL    = "samimuduroglu1@gmail.com"
VA_PASSWORD = "Rthj9bdx"
CDP_PORT    = 9222

# ── Shadow DOM helpers (injected as JS) ───────────────────────────────────────
# Copied verbatim from book_alaska.py — VA uses React web components too.
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
function setInputById(id, val) {
    const all = [];
    findAllInShadow(document, 'input', all);
    const el = all.find(i => i.id === id);
    if (!el) return 'nf:' + id;
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
function setSelectById(id, val) {
    const all = [];
    findAllInShadow(document, 'select', all);
    const el = all.find(i => i.id === id);
    if (!el) return 'nf:' + id;
    el.value = val;
    el.dispatchEvent(new Event('change', {bubbles:true}));
    return 'ok:' + el.value;
}
"""


# ── Appa notifier (mirrors server.py pattern) ──────────────────────────────────
def _notify_appa(text: str):
    """POST a wake event to Appa's hook so it can relay alerts to Telegram."""
    hook_url   = os.environ.get("APPA_HOOK_URL", "https://hooks.airbitrage.io/hooks/wake")
    hook_token = os.environ.get("APPA_HOOK_TOKEN", "flightdash-hook-token-2026")
    payload = json.dumps({"text": text}).encode()
    try:
        import urllib.request
        req = urllib.request.Request(
            hook_url,
            data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {hook_token}"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"[notify_appa] failed: {e}")


# ── DB helpers ─────────────────────────────────────────────────────────────────
def _update_booking(booking_id: int, status: str, ref: str = None):
    """Update vault.db booking row with final status and optional reference."""
    if not booking_id:
        return
    import sqlite3
    db_path = os.path.join(os.path.dirname(__file__), "vault.db")
    conn = sqlite3.connect(db_path)
    if ref:
        conn.execute(
            "UPDATE bookings SET status=?, airline_ref=? WHERE id=?",
            (status, ref, booking_id),
        )
    else:
        conn.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))
    conn.commit()
    conn.close()
    print(f"[book_va] Updated booking_id={booking_id} → {status}" + (f", ref={ref}" if ref else ""))


# ── Main booking coroutine ─────────────────────────────────────────────────────
async def book_virgin_atlantic(
    origin: str,
    dest: str,
    date: str,           # YYYY-MM-DD
    first: str,
    last: str,
    dob: str,            # YYYY-MM-DD
    cabin: str = "business",
    card_last4: str = "2002",
    booking_id: int = None,
):
    """
    Full Virgin Atlantic Flying Club award booking flow.
    Returns confirmation code string or raises.
    """
    from playwright.async_api import async_playwright

    # Parse DOB
    dob_parts   = dob.split("-")
    dob_year    = dob_parts[0]
    dob_month   = dob_parts[1]   # "03" zero-padded
    dob_day     = dob_parts[2]   # "06" zero-padded
    dob_month_i = str(int(dob_month))  # "3" not "03"

    # Format date components
    date_parts = date.split("-")
    date_year  = date_parts[0]
    date_month = date_parts[1]
    date_day   = date_parts[2]
    # VA date picker format: DD/MM/YYYY  (UK-style)
    date_uk    = f"{date_day}/{date_month}/{date_year}"

    # VA cabin → search param
    cabin_lower = cabin.lower()
    cabin_map = {
        "economy":         "Economy",
        "premium":         "Premium",
        "premium economy": "Premium",
        "business":        "Upper Class",
        "upper class":     "Upper Class",
        "first":           "Upper Class",
    }
    cabin_label = cabin_map.get(cabin_lower, "Upper Class")

    # Card details
    card_info = CARDS.get(card_last4, CARDS.get("2002"))

    async with async_playwright() as pw:
        # ── Connect to OpenClaw's managed browser via CDP ──────────────────────
        try:
            browser = await pw.chromium.connect_over_cdp(f"http://localhost:{CDP_PORT}")
            print(f"[book_va] Connected to CDP on port {CDP_PORT}")
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
        except Exception:
            print("[book_va] CDP connection failed, launching persistent context")
            context = await pw.chromium.launch_persistent_context(
                user_data_dir="/Users/samimuduroglu/.openclaw/browser-profiles/va-booking",
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            )
            browser = None

        page = await context.new_page()

        # ── Step 1: Go to Flying Club homepage ────────────────────────────────
        print("[book_va] Navigating to Virgin Atlantic Flying Club")
        await page.goto(
            "https://www.virginatlantic.com/us/en/flying-club.html",
            wait_until="domcontentloaded",
        )
        await page.wait_for_timeout(2000)

        # Dismiss cookie consent if present
        for cookie_sel in [
            "button:has-text('Accept')",
            "button:has-text('Accept all')",
            "button:has-text('Accept All Cookies')",
            "#onetrust-accept-btn-handler",
        ]:
            try:
                await page.click(cookie_sel, timeout=3000)
                print("[book_va] Dismissed cookie banner")
                break
            except Exception:
                pass

        # ── Step 2: Log in if not already ─────────────────────────────────────
        print("[book_va] Checking login state")
        already_logged_in = False
        try:
            # Look for account/member indicators
            await page.wait_for_selector(
                "text=My account, text=Log out, text=Sign out, [data-testid='account-menu'], .fc-member",
                timeout=4000,
            )
            already_logged_in = True
            print("[book_va] Already logged in")
        except Exception:
            pass

        if not already_logged_in:
            print("[book_va] Logging in to Flying Club")
            # Click Sign in / Log in button
            for login_sel in [
                "a:has-text('Log in')",
                "button:has-text('Log in')",
                "a:has-text('Sign in')",
                "button:has-text('Sign in')",
                "[data-testid='login-button']",
                "[aria-label='Log in']",
            ]:
                try:
                    await page.click(login_sel, timeout=4000)
                    print(f"[book_va] Clicked login: {login_sel}")
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)

            # Fill email
            for email_sel in ["input[type='email']", "input[name='email']", "#email", "input[autocomplete='email']"]:
                try:
                    await page.fill(email_sel, VA_EMAIL, timeout=5000)
                    print(f"[book_va] Filled email via {email_sel}")
                    break
                except Exception:
                    continue

            await page.wait_for_timeout(500)

            # Click Continue / Next if email-first flow
            for next_sel in [
                "button:has-text('Continue')",
                "button:has-text('Next')",
                "button[type='submit']:has-text('Continue')",
            ]:
                try:
                    await page.click(next_sel, timeout=3000)
                    await page.wait_for_timeout(1500)
                    break
                except Exception:
                    pass

            # Fill password
            for pwd_sel in ["input[type='password']", "input[name='password']", "#password"]:
                try:
                    await page.fill(pwd_sel, VA_PASSWORD, timeout=5000)
                    print(f"[book_va] Filled password via {pwd_sel}")
                    break
                except Exception:
                    continue

            await page.wait_for_timeout(300)

            # Submit
            for submit_sel in [
                "button[type='submit']:has-text('Log in')",
                "button[type='submit']:has-text('Sign in')",
                "button:has-text('Log in')",
                "button[type='submit']",
            ]:
                try:
                    await page.click(submit_sel, timeout=4000)
                    print("[book_va] Submitted login form")
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)
            print(f"[book_va] Post-login URL: {page.url}")

        # ── Step 3: Navigate to award search ──────────────────────────────────
        print("[book_va] Navigating to award flight search")
        # Try direct URL to the miles booking search page
        await page.goto(
            "https://www.virginatlantic.com/us/en/book-a-flight.html",
            wait_until="domcontentloaded",
        )
        await page.wait_for_timeout(2000)

        # Select "Use miles" / "Reward" / "Miles" toggle if present
        for miles_sel in [
            "label:has-text('Use miles')",
            "button:has-text('Use miles')",
            "input[value='miles']",
            "label:has-text('Miles')",
            "button:has-text('Reward')",
            "[data-testid='miles-toggle']",
            "label:has-text('Redeem miles')",
        ]:
            try:
                await page.click(miles_sel, timeout=4000)
                print(f"[book_va] Clicked miles toggle: {miles_sel}")
                await page.wait_for_timeout(500)
                break
            except Exception:
                pass

        # Select one-way
        for ow_sel in [
            "label:has-text('One way')",
            "input[value='one-way']",
            "input[value='OW']",
            "button:has-text('One way')",
            "[data-testid='one-way']",
        ]:
            try:
                await page.click(ow_sel, timeout=3000)
                print("[book_va] Selected one-way")
                await page.wait_for_timeout(300)
                break
            except Exception:
                pass

        # ── Step 4: Fill origin ────────────────────────────────────────────────
        print(f"[book_va] Setting origin: {origin}")
        for from_sel in [
            "[placeholder*='From']",
            "[placeholder*='Origin']",
            "[placeholder*='Departure']",
            "input[name='origin']",
            "[data-testid='origin-input']",
            "#from",
        ]:
            try:
                await page.click(from_sel, timeout=3000)
                await page.fill(from_sel, origin, timeout=3000)
                await page.wait_for_timeout(1000)
                # Select from dropdown
                for dropdown_sel in [
                    f"li:has-text('{origin}')",
                    f"[role='option']:has-text('{origin}')",
                    f"button:has-text('{origin}')",
                ]:
                    try:
                        await page.click(dropdown_sel, timeout=3000)
                        break
                    except Exception:
                        pass
                print(f"[book_va] Origin set")
                break
            except Exception:
                continue

        # ── Step 5: Fill destination ───────────────────────────────────────────
        print(f"[book_va] Setting destination: {dest}")
        for to_sel in [
            "[placeholder*='To']",
            "[placeholder*='Destination']",
            "[placeholder*='Arrival']",
            "input[name='destination']",
            "[data-testid='destination-input']",
            "#to",
        ]:
            try:
                await page.click(to_sel, timeout=3000)
                await page.fill(to_sel, dest, timeout=3000)
                await page.wait_for_timeout(1000)
                # Select from dropdown
                for dropdown_sel in [
                    f"li:has-text('{dest}')",
                    f"[role='option']:has-text('{dest}')",
                    f"button:has-text('{dest}')",
                ]:
                    try:
                        await page.click(dropdown_sel, timeout=3000)
                        break
                    except Exception:
                        pass
                print(f"[book_va] Destination set")
                break
            except Exception:
                continue

        # ── Step 6: Fill departure date ────────────────────────────────────────
        print(f"[book_va] Setting date: {date_uk}")
        for date_sel in [
            "[placeholder*='DD/MM/YYYY']",
            "[placeholder*='Date']",
            "input[name='departureDate']",
            "input[name='outbound']",
            "[data-testid='departure-date']",
            "#departure-date",
        ]:
            try:
                await page.click(date_sel, timeout=3000)
                await page.fill(date_sel, date_uk, timeout=3000)
                await page.keyboard.press("Tab")
                print("[book_va] Date set via fill")
                break
            except Exception:
                continue
        else:
            # Fallback: shadow DOM approach
            await page.evaluate(f"""
                {FIND_IN_SHADOW_JS}
                const inp = findInShadow(document, 'input[placeholder*="DD/MM"]') ||
                            findInShadow(document, 'input[type="date"]');
                if (inp) {{
                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    setter.call(inp, '{date_uk}');
                    inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                    inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                }}
            """)

        await page.wait_for_timeout(500)

        # Close date picker if open
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

        # ── Step 7: Set cabin class ────────────────────────────────────────────
        print(f"[book_va] Setting cabin: {cabin_label}")
        for cabin_sel in [
            "select[name='cabin']",
            "select[name='cabinClass']",
            "[data-testid='cabin-select']",
            "#cabin",
        ]:
            try:
                await page.select_option(cabin_sel, label=cabin_label, timeout=3000)
                print(f"[book_va] Cabin set via select")
                break
            except Exception:
                continue
        else:
            # Try clicking dropdown then option
            for cabin_btn_sel in [
                f"button:has-text('Economy')",
                f"[data-testid='cabin-class']",
                "select[name='class']",
            ]:
                try:
                    await page.click(cabin_btn_sel, timeout=3000)
                    await page.wait_for_timeout(500)
                    await page.click(f"[role='option']:has-text('{cabin_label}')", timeout=3000)
                    print(f"[book_va] Cabin set via dropdown")
                    break
                except Exception:
                    continue

        # ── Step 8: Search ─────────────────────────────────────────────────────
        print("[book_va] Clicking Search")
        for search_sel in [
            "button:has-text('Search')",
            "button[type='submit']:has-text('Search')",
            "[data-testid='search-button']",
            "button:has-text('Find flights')",
            "button:has-text('Search flights')",
        ]:
            try:
                await page.click(search_sel, timeout=5000)
                print(f"[book_va] Clicked search: {search_sel}")
                break
            except Exception:
                continue

        print("[book_va] Waiting for results...")
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(3000)

        # ── Step 9: Select first available result ──────────────────────────────
        print("[book_va] Selecting first available flight")
        selected = False

        # VA shows "Select" or "Book" buttons next to flights
        for select_sel in [
            "button:has-text('Select'):not([disabled])",
            "button:has-text('Book'):not([disabled])",
            "[data-testid='select-flight']:not([disabled])",
            ".flight-result button:not([disabled])",
            "button:has-text('miles'):not([disabled])",
        ]:
            try:
                buttons = await page.query_selector_all(select_sel)
                if buttons:
                    await buttons[0].scroll_into_view_if_needed()
                    await buttons[0].click()
                    selected = True
                    print(f"[book_va] Selected first flight result")
                    break
            except Exception:
                continue

        if not selected:
            # Try shadow DOM
            clicked = await page.evaluate(f"""
                {FIND_IN_SHADOW_JS}
                const all = [];
                findAllInShadow(document, 'button', all);
                const btn = all.find(b => {{
                    const t = b.textContent.trim().toLowerCase();
                    return (t.includes('select') || t.includes('book') || t.includes('miles'))
                        && !b.disabled;
                }});
                if (btn) {{ btn.click(); return true; }}
                return false;
            """)
            if clicked:
                selected = True
                print("[book_va] Selected flight via shadow DOM")

        if not selected:
            raise Exception("No available award seats found for this route/date")

        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # ── Step 10: Handle cabin upsell or confirm cabin selection ───────────
        # VA sometimes shows a cabin confirmation step
        for confirm_sel in [
            f"button:has-text('{cabin_label}')",
            "button:has-text('Continue')",
            "button:has-text('Confirm')",
        ]:
            try:
                await page.click(confirm_sel, timeout=3000)
                await page.wait_for_timeout(1000)
                break
            except Exception:
                pass

        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1000)

        # ── Step 11: Fill passenger details ───────────────────────────────────
        print(f"[book_va] Filling passenger: {first} {last}, DOB {dob}")

        # Title (required by VA — default to Mr)
        for title_sel in [
            "select[name='title']",
            "select[id='title']",
            "[data-testid='passenger-title']",
        ]:
            try:
                await page.select_option(title_sel, value="MR", timeout=3000)
                print("[book_va] Set title to Mr")
                break
            except Exception:
                continue

        # First name
        for fn_sel in [
            "input[name='firstName']",
            "input[name='first_name']",
            "input[id='firstName']",
            "[placeholder*='First name']",
            "[data-testid='first-name']",
        ]:
            try:
                await page.fill(fn_sel, first, timeout=3000)
                print("[book_va] Set first name")
                break
            except Exception:
                continue
        else:
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setInputByName('firstName', '{first}') ||
                setInputByName('first_name', '{first}') ||
                setInputById('firstName', '{first}');
            """)

        # Last name
        for ln_sel in [
            "input[name='lastName']",
            "input[name='last_name']",
            "input[id='lastName']",
            "[placeholder*='Last name']",
            "[data-testid='last-name']",
        ]:
            try:
                await page.fill(ln_sel, last, timeout=3000)
                print("[book_va] Set last name")
                break
            except Exception:
                continue
        else:
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setInputByName('lastName', '{last}') ||
                setInputByName('last_name', '{last}') ||
                setInputById('lastName', '{last}');
            """)

        # DOB — VA uses separate day/month/year dropdowns or a single field
        # Try dropdowns first
        dob_set = False
        for day_sel in ["select[name='dobDay']", "select[id='dobDay']", "select[name='dob-day']"]:
            try:
                await page.select_option(day_sel, value=dob_day, timeout=2000)
                dob_set = True
                break
            except Exception:
                pass

        if dob_set:
            for mon_sel in ["select[name='dobMonth']", "select[id='dobMonth']", "select[name='dob-month']"]:
                try:
                    await page.select_option(mon_sel, value=dob_month, timeout=2000)
                    break
                except Exception:
                    pass
            for yr_sel in ["select[name='dobYear']", "select[id='dobYear']", "select[name='dob-year']"]:
                try:
                    await page.select_option(yr_sel, value=dob_year, timeout=2000)
                    break
                except Exception:
                    pass
            print("[book_va] Set DOB via dropdowns")
        else:
            # Try text field (DD/MM/YYYY or YYYY-MM-DD)
            for dob_sel in [
                "input[name='dateOfBirth']",
                "input[name='dob']",
                "input[id='dateOfBirth']",
                "[placeholder*='Date of birth']",
                "[placeholder*='DD/MM/YYYY']",
            ]:
                try:
                    await page.fill(dob_sel, f"{dob_day}/{dob_month}/{dob_year}", timeout=3000)
                    print("[book_va] Set DOB via text field")
                    break
                except Exception:
                    continue

        await page.wait_for_timeout(500)

        # Continue to next step
        for cont_sel in [
            "button:has-text('Continue')",
            "button:has-text('Next')",
            "button[type='submit']",
        ]:
            try:
                await page.click(cont_sel, timeout=5000)
                print("[book_va] Continued past passenger details")
                break
            except Exception:
                continue

        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # ── Step 12: Seat selection (skip / continue) ──────────────────────────
        print("[book_va] Skipping seat selection if prompted")
        for skip_sel in [
            "button:has-text('Skip')",
            "button:has-text('No thanks')",
            "button:has-text('Continue without selecting')",
            "button:has-text('Continue')",
        ]:
            try:
                await page.click(skip_sel, timeout=4000)
                print(f"[book_va] Skipped seat selection")
                await page.wait_for_timeout(1000)
                break
            except Exception:
                pass

        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1500)

        # ── Step 13: Extras / add-ons (skip) ──────────────────────────────────
        print("[book_va] Skipping extras")
        for skip_sel in [
            "button:has-text('Continue')",
            "button:has-text('No thanks')",
            "button:has-text('Skip')",
        ]:
            try:
                await page.click(skip_sel, timeout=4000)
                print("[book_va] Skipped extras")
                await page.wait_for_timeout(1000)
                break
            except Exception:
                pass

        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1500)

        # ── Step 14: Payment page ──────────────────────────────────────────────
        print(f"[book_va] Filling billing address and card ending {card_last4}")

        # Billing address
        for addr_sel in [
            "input[name='addressLine1']",
            "input[name='address1']",
            "input[name='billingAddress']",
            "[placeholder*='Address']",
            "[data-testid='billing-address']",
        ]:
            try:
                await page.fill(addr_sel, BILLING["address"], timeout=3000)
                print("[book_va] Set billing address")
                break
            except Exception:
                continue
        else:
            await page.evaluate(f"""
                {SET_INPUT_JS}
                setInputByName('addressLine1', '{BILLING["address"]}') ||
                setInputByName('address1', '{BILLING["address"]}') ||
                setInputByName('billingAddress', '{BILLING["address"]}');
            """)

        for city_sel in [
            "input[name='city']",
            "input[name='billingCity']",
            "[placeholder*='City']",
            "[data-testid='city']",
        ]:
            try:
                await page.fill(city_sel, BILLING["city"], timeout=3000)
                print("[book_va] Set city")
                break
            except Exception:
                continue

        # State/Province
        for state_sel in ["select[name='state']", "select[name='billingState']", "input[name='state']"]:
            try:
                await page.select_option(state_sel, label="Rhode Island", timeout=2000)
                print("[book_va] Set state")
                break
            except Exception:
                try:
                    await page.fill(state_sel, BILLING["state"], timeout=2000)
                    break
                except Exception:
                    pass

        for zip_sel in [
            "input[name='postcode']",
            "input[name='zipCode']",
            "input[name='zip']",
            "input[name='postalCode']",
            "[placeholder*='Postcode']",
            "[placeholder*='Zip']",
        ]:
            try:
                await page.fill(zip_sel, BILLING["zip"], timeout=3000)
                print("[book_va] Set zip")
                break
            except Exception:
                continue

        # Country — VA is UK-based, billing country = US
        for country_sel in [
            "select[name='country']",
            "select[name='billingCountry']",
            "[data-testid='country-select']",
        ]:
            try:
                await page.select_option(country_sel, label="United States", timeout=3000)
                print("[book_va] Set country to US")
                break
            except Exception:
                try:
                    await page.select_option(country_sel, value="US", timeout=3000)
                    break
                except Exception:
                    pass

        await page.wait_for_timeout(500)

        # ── Card details ───────────────────────────────────────────────────────
        # Try selecting a saved card first
        saved_card_clicked = False
        for saved_sel in [
            f"button:has-text('{card_info['label']}')",
            f"button:has-text('{card_last4}')",
            f"label:has-text('{card_last4}')",
            f"[data-testid='saved-card-{card_last4}']",
        ]:
            try:
                await page.click(saved_sel, timeout=3000)
                saved_card_clicked = True
                print(f"[book_va] Selected saved card '{card_info['label']}'")
                break
            except Exception:
                pass

        if not saved_card_clicked:
            # Enter card number manually
            for cn_sel in [
                "input[name='cardNumber']",
                "input[name='card_number']",
                "input[autocomplete='cc-number']",
                "[placeholder*='Card number']",
                "[data-testid='card-number']",
            ]:
                try:
                    await page.fill(cn_sel, card_info["number"], timeout=3000)
                    print("[book_va] Set card number")
                    break
                except Exception:
                    continue

            # Expiry month
            for exp_m_sel in [
                "select[name='expiryMonth']",
                "input[name='expiryMonth']",
                "select[autocomplete='cc-exp-month']",
                "[data-testid='expiry-month']",
            ]:
                try:
                    await page.select_option(exp_m_sel, value=card_info["expiry_month"], timeout=3000)
                    break
                except Exception:
                    try:
                        await page.fill(exp_m_sel, card_info["expiry_month"], timeout=3000)
                        break
                    except Exception:
                        pass

            # Expiry year
            for exp_y_sel in [
                "select[name='expiryYear']",
                "input[name='expiryYear']",
                "select[autocomplete='cc-exp-year']",
                "[data-testid='expiry-year']",
            ]:
                try:
                    await page.select_option(exp_y_sel, value=card_info["expiry_year"], timeout=3000)
                    break
                except Exception:
                    try:
                        await page.fill(exp_y_sel, card_info["expiry_year"], timeout=3000)
                        break
                    except Exception:
                        pass

            # CVV / Security code
            for cvv_sel in [
                "input[name='cvv']",
                "input[name='cvc']",
                "input[name='securityCode']",
                "input[autocomplete='cc-csc']",
                "[placeholder*='CVV']",
                "[placeholder*='Security']",
                "[data-testid='cvv']",
            ]:
                try:
                    await page.fill(cvv_sel, card_info["cvv"], timeout=3000)
                    print("[book_va] Set CVV")
                    break
                except Exception:
                    continue

            # Cardholder name
            for name_sel in [
                "input[name='cardholderName']",
                "input[name='nameOnCard']",
                "input[autocomplete='cc-name']",
                "[placeholder*='Name on card']",
            ]:
                try:
                    await page.fill(name_sel, card_info["name"], timeout=3000)
                    print("[book_va] Set cardholder name")
                    break
                except Exception:
                    continue

        await page.wait_for_timeout(500)

        # ── Step 15: Accept T&Cs / tick checkboxes ────────────────────────────
        print("[book_va] Accepting terms if required")
        for tc_sel in [
            "input[type='checkbox'][name*='terms']",
            "input[type='checkbox'][id*='terms']",
            "input[type='checkbox'][name*='agree']",
            "[data-testid='terms-checkbox']",
        ]:
            try:
                cb = await page.query_selector(tc_sel)
                if cb and not await cb.is_checked():
                    await cb.click()
                    print("[book_va] Accepted T&Cs")
            except Exception:
                pass

        await page.wait_for_timeout(500)

        # ── Step 16: Click Pay / Confirm / Book now ────────────────────────────
        if DRY_RUN:
            print("[book_va] DRY RUN — stopping before Pay button. Screenshot saved.")
            await page.screenshot(path="/tmp/va_dry_run_payment_page.png")
            _notify_appa(
                f"\U0001f9ea DRY RUN complete for booking #{booking_id}\n"
                f"Route: {origin} \u2192 {dest} on {date}\n"
                f"Got to payment page — stopped before Pay. Screenshot at /tmp/va_dry_run_payment_page.png"
            )
            _update_booking(booking_id, "dry_run")
            return "DRY_RUN"

        print("[book_va] Clicking Pay/Book now")
        booked = False
        for attempt in range(3):
            for pay_sel in [
                "button:has-text('Pay now')",
                "button:has-text('Confirm and pay')",
                "button:has-text('Book now')",
                "button:has-text('Complete booking')",
                "button:has-text('Pay')",
                "[data-testid='pay-button']",
                "[data-testid='book-button']",
            ]:
                try:
                    btn = page.locator(pay_sel).first
                    is_disabled = await btn.is_disabled(timeout=2000)
                    if is_disabled:
                        print(f"[book_va] Pay button disabled (attempt {attempt+1}), waiting...")
                        await page.wait_for_timeout(1500)
                        continue
                    await btn.click(timeout=8000)
                    booked = True
                    print(f"[book_va] Clicked pay button: {pay_sel}")
                    break
                except Exception:
                    continue
            if booked:
                break
            await page.wait_for_timeout(1500)

        if not booked:
            # Shadow DOM fallback
            booked = await page.evaluate(f"""
                {FIND_IN_SHADOW_JS}
                const all = [];
                findAllInShadow(document, 'button', all);
                const btn = all.find(b => {{
                    const t = b.textContent.trim().toLowerCase();
                    return (t.includes('pay') || t.includes('book') || t.includes('confirm'))
                        && !b.disabled;
                }});
                if (btn) {{ btn.click(); return true; }}
                return false;
            """)

        if not booked:
            raise Exception("Could not click Pay/Book now button after 3 attempts")

        # ── Step 17: Wait for confirmation ────────────────────────────────────
        print("[book_va] Waiting for booking confirmation...")
        try:
            await page.wait_for_url("**/confirmation**", timeout=90000)
        except Exception:
            # VA might not change URL — wait for confirmation text instead
            try:
                await page.wait_for_selector(
                    "text=Booking reference, text=Confirmation, text=Reference number, text=Booking number",
                    timeout=90000,
                )
            except Exception:
                pass

        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # ── Step 18: Extract booking reference ────────────────────────────────
        content = await page.content()

        # VA booking reference: 6 alphanumeric uppercase chars (e.g. "ABC123")
        # Also look for PNR patterns
        codes = re.findall(r'\b([A-Z0-9]{6})\b', content)
        false_positives = {
            "VIRGIN", "ATLANT", "FLYING", "BUSINE", "SELECT", "POINTS",
            "SEARCH", "REWARD", "FLIGHT", "TICKET", "TRAVEL", "PLEASE",
        }
        codes = [c for c in codes if c not in false_positives and not c.isdigit()]

        # Also try longer reference patterns (some VA refs are longer)
        long_refs = re.findall(r'\b([A-Z]{2}\d{6,})\b', content)

        confirmation = codes[0] if codes else (long_refs[0] if long_refs else "UNKNOWN")
        print(f"[book_va] ✅ BOOKED! Confirmation: {confirmation}")
        print(f"[book_va] All codes found: {codes[:5]}")

        await page.screenshot(path=f"/tmp/va_booking_confirmation_{confirmation}.png")
        print(f"[book_va] Screenshot saved to /tmp/va_booking_confirmation_{confirmation}.png")

        # ── Step 19: Update vault.db ───────────────────────────────────────────
        _update_booking(booking_id, "confirmed", confirmation)

        # ── Step 20: Notify Appa ───────────────────────────────────────────────
        _notify_appa(
            f"✅ Virgin Atlantic booking confirmed!\n"
            f"  Route: {origin} → {dest} on {date}\n"
            f"  Passenger: {first} {last}\n"
            f"  Cabin: {cabin_label}\n"
            f"  Booking reference: {confirmation}"
        )

        return confirmation


def main():
    parser = argparse.ArgumentParser(description="Book Virgin Atlantic award flight")
    parser.add_argument("--origin",     required=True)
    parser.add_argument("--dest",       required=True)
    parser.add_argument("--date",       required=True, help="YYYY-MM-DD")
    parser.add_argument("--first",      required=True)
    parser.add_argument("--last",       required=True)
    parser.add_argument("--dob",        required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin",      default="business")
    parser.add_argument("--card",       default="2002", help="Last 4 of card")
    parser.add_argument("--booking-id", default=None, type=int)
    parser.add_argument("--dry-run",    action="store_true", help="Stop before Pay button")
    args = parser.parse_args()

    if args.dry_run:
        os.environ["DRY_RUN"] = "true"

    booking_id = args.booking_id

    try:
        confirmation = asyncio.run(book_virgin_atlantic(
            origin     = args.origin.upper(),
            dest       = args.dest.upper(),
            date       = args.date,
            first      = args.first,
            last       = args.last,
            dob        = args.dob,
            cabin      = args.cabin,
            card_last4 = args.card,
            booking_id = booking_id,
        ))
        print(json.dumps({"confirmation": confirmation, "status": "confirmed"}))

    except Exception as e:
        err_msg = str(e)
        print(f"[book_va] ❌ FAILED: {err_msg}", file=sys.stderr)

        # Mark booking failed in DB
        _update_booking(booking_id, "failed")

        # Notify Appa of failure
        _notify_appa(
            f"❌ Virgin Atlantic booking FAILED!\n"
            f"  Booking ID: {booking_id}\n"
            f"  Error: {err_msg}"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
