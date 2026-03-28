"""
aeroplan_registrar.py
Automates Aeroplan account signup at aircanada.com.

The signup form is a 3-step wizard:
  Step 1: Email + password + T&Cs
  Step 2: Personal info (name, DOB, gender)
  Step 3: Contact info (address, phone)

If a CAPTCHA is detected, CaptchaRequiredError is raised.
"""

import asyncio
import os
import re
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/tmp/pw-browsers")

ENROLMENT_URL = "https://www.aircanada.com/aeroplan/member/enrolment"


class CaptchaRequiredError(Exception):
    """Raised when a CAPTCHA challenge is detected during account registration."""


async def register_account(client: dict) -> dict:
    """
    Automate Aeroplan account signup.

    Parameters
    ----------
    client : dict with keys:
        first_name  (str)
        last_name   (str)
        dob         (str)  – DD/MM/YYYY
        email       (str)
        password    (str)
        address     (str)  – free-form, e.g. "123 Main St, Toronto, ON M5V 1A1"
        phone       (str)  – E.164 or local format, e.g. "+14165551234"

    Returns
    -------
    dict:  {"aeroplan_number": str, "email": str}

    Raises
    ------
    CaptchaRequiredError  – if a CAPTCHA is detected on the form.
    RuntimeError          – if registration fails for any other reason.
    """
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )
    context = await browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()

    # Stealth: mask automation indicators
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except ImportError:
        pass

    # Remove webdriver flag
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
    """)

    try:
        print(f"[registrar] Navigating to enrolment page", flush=True)
        await page.goto(ENROLMENT_URL, wait_until="load", timeout=30000)

        # Wait for the form to render
        await page.wait_for_selector('input[name="emailAddress"]', timeout=15000)
        print("[registrar] Step 1: email + password", flush=True)

        # CAPTCHA check
        if await _captcha_present(page):
            raise CaptchaRequiredError("CAPTCHA detected before form fill.")

        # ── Step 1: Email + password ─────────────────────────────────────────
        # Use type() not fill() — Angular needs real keyboard events
        await page.click('input[name="emailAddress"]')
        await page.type('input[name="emailAddress"]', client["email"], delay=40)
        await page.wait_for_timeout(200)
        await page.click('input[name="password"]')
        await page.type('input[name="password"]', client["password"], delay=40)
        await page.keyboard.press('Tab')
        await page.wait_for_timeout(400)

        # Dismiss OneTrust cookie banner if present (it blocks clicks)
        try:
            accept_btn = await page.query_selector('#onetrust-accept-btn-handler, button:has-text("Accept all")')
            if accept_btn:
                await accept_btn.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass

        # Accept T&Cs — use JS to check the MDC checkbox and fire Angular's change event
        await page.evaluate("""
            const input = document.querySelector('input#checkBox-input');
            if (input && !input.checked) {
                input.checked = true;
                ['click', 'change', 'input'].forEach(evt =>
                    input.dispatchEvent(new Event(evt, {bubbles: true}))
                );
            }
        """)

        # Click Continue
        await page.click('button:has-text("Continue")')
        print("[registrar] Step 1 submitted", flush=True)

        # ── Step 2: Personal information ─────────────────────────────────────
        # Wait for first name field
        await page.wait_for_selector('input[name="firstName"], input[id*="firstName"]', timeout=15000)
        print("[registrar] Step 2: personal info", flush=True)

        if await _captcha_present(page):
            raise CaptchaRequiredError("CAPTCHA detected on step 2.")

        # Parse DOB: DD/MM/YYYY
        dob_parts = client["dob"].split("/")
        if len(dob_parts) != 3:
            raise ValueError(f"dob must be DD/MM/YYYY, got: {client['dob']!r}")
        dob_day, dob_month, dob_year = dob_parts

        # Fill name fields
        for sel in ['input[name="firstName"]', 'input[id*="firstName"]']:
            try:
                await page.fill(sel, client["first_name"])
                break
            except Exception:
                continue

        for sel in ['input[name="lastName"]', 'input[id*="lastName"]']:
            try:
                await page.fill(sel, client["last_name"])
                break
            except Exception:
                continue

        # DOB — try combined field first, then separate selects
        dob_filled = False
        for sel in ['input[name="dateOfBirth"]', 'input[id*="dob"]', 'input[id*="dateOfBirth"]']:
            try:
                el = await page.query_selector(sel)
                if el:
                    await page.fill(sel, client["dob"])
                    dob_filled = True
                    break
            except Exception:
                continue

        if not dob_filled:
            # Try separate day/month/year selects or inputs
            try:
                await page.select_option('select[name*="day"], select[id*="day"]', value=dob_day.lstrip("0") or "1")
                await page.select_option('select[name*="month"], select[id*="month"]', value=dob_month.lstrip("0") or "1")
                await page.select_option('select[name*="year"], select[id*="year"]', value=dob_year)
            except Exception:
                pass

        # Gender (pick first option if present)
        try:
            gender_sel = await page.query_selector('select[name*="gender"], select[id*="gender"]')
            if gender_sel:
                options = await gender_sel.query_selector_all('option')
                if len(options) > 1:
                    val = await options[1].get_attribute("value")
                    await page.select_option('select[name*="gender"], select[id*="gender"]', value=val)
        except Exception:
            pass

        # Continue to step 3
        await page.click('button:has-text("Continue")')
        print("[registrar] Step 2 submitted", flush=True)

        # ── Step 3: Contact information ──────────────────────────────────────
        await page.wait_for_timeout(5000)
        print("[registrar] Step 3: contact info", flush=True)

        if await _captcha_present(page):
            raise CaptchaRequiredError("CAPTCHA detected on step 3.")

        # Address
        address_parts = [p.strip() for p in client.get("address", "").split(",")]
        if address_parts:
            for sel in ['input[name="address"]', 'input[name="addressLine1"]', 'input[id*="address"]']:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await page.fill(sel, address_parts[0])
                        break
                except Exception:
                    continue

        if len(address_parts) > 1:
            for sel in ['input[name="city"]', 'input[id*="city"]']:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await page.fill(sel, address_parts[1])
                        break
                except Exception:
                    continue

        # Phone
        for sel in ['input[name="phone"]', 'input[id*="phone"]', 'input[type="tel"]']:
            try:
                el = await page.query_selector(sel)
                if el:
                    await page.fill(sel, client.get("phone", ""))
                    break
            except Exception:
                continue

        # Final submit
        for sel in [
            'button[type="submit"]',
            'button:has-text("Join")',
            'button:has-text("Enroll")',
            'button:has-text("Register")',
            'button:has-text("Complete")',
            'button:has-text("Submit")',
            'button:has-text("Continue")',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    break
            except Exception:
                continue

        print("[registrar] Final submit clicked, waiting for confirmation...", flush=True)
        await page.wait_for_timeout(6000)

        if await _captcha_present(page):
            raise CaptchaRequiredError("CAPTCHA detected after final submit.")

        # ── Extract Aeroplan number ──────────────────────────────────────────
        page_text = await page.inner_text("body")
        matches = re.findall(r"\b(\d{9})\b", page_text)
        aeroplan_number = matches[0] if matches else ""

        if not aeroplan_number:
            for sel in [
                '[data-testid*="aeroplan"]',
                '[class*="memberNumber"]',
                '[id*="memberNumber"]',
                '[class*="member-number"]',
            ]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        text = await el.inner_text()
                        nums = re.findall(r"\d{9}", text)
                        if nums:
                            aeroplan_number = nums[0]
                            break
                except Exception:
                    continue

        print(f"[registrar] Final page URL: {page.url}", flush=True)
        print(f"[registrar] Page snippet: {page_text[:300]}", flush=True)

        if not aeroplan_number:
            raise RuntimeError(
                f"Registration may have succeeded but could not extract Aeroplan number. "
                f"Page URL: {page.url}\nPage text: {page_text[:500]}"
            )

        print(f"[registrar] ✅ Registered! Aeroplan number: {aeroplan_number}", flush=True)
        return {"aeroplan_number": aeroplan_number, "email": client["email"]}

    finally:
        await browser.close()
        await playwright.stop()


async def _captcha_present(page) -> bool:
    """Heuristic check for common CAPTCHA indicators."""
    indicators = [
        "iframe[src*='recaptcha']",
        "iframe[src*='hcaptcha']",
        ".g-recaptcha",
        "#captcha",
        "[data-sitekey]",
        "iframe[title*='captcha' i]",
    ]
    for sel in indicators:
        try:
            el = await page.query_selector(sel)
            if el:
                return True
        except Exception:
            continue

    try:
        text = await page.inner_text("body")
        if any(phrase in text.lower() for phrase in ["verify you are human", "i'm not a robot", "captcha"]):
            return True
    except Exception:
        pass

    return False


def register_account_sync(client: dict) -> dict:
    """Synchronous wrapper around register_account()."""
    return asyncio.run(register_account(client))
