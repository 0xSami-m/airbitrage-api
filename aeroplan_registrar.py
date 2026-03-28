"""
aeroplan_registrar.py
Automates Aeroplan account signup at aircanada.com.

The signup form may present a CAPTCHA. If detected, a CaptchaRequiredError
is raised so callers can handle it gracefully (e.g. fall back to manual
registration or a solving service).
"""

import asyncio
import re
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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
        headless=False,
        executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
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

    try:
        print(f"[registrar] Navigating to {ENROLMENT_URL}")
        await page.goto(ENROLMENT_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        # ── CAPTCHA detection ────────────────────────────────────────────────
        if await _captcha_present(page):
            raise CaptchaRequiredError(
                "CAPTCHA detected on the Aeroplan enrolment page. "
                "Manual intervention or a CAPTCHA-solving service is required."
            )

        # ── Parse DOB ────────────────────────────────────────────────────────
        dob_parts = client["dob"].split("/")
        if len(dob_parts) != 3:
            raise ValueError(f"dob must be DD/MM/YYYY, got: {client['dob']!r}")
        dob_day, dob_month, dob_year = dob_parts

        # ── Fill personal information ─────────────────────────────────────────
        print("[registrar] Filling personal information")

        # First / last name — field selectors vary by form version; try several
        for sel in ['input[name="firstName"]', 'input[id*="firstName"]', '#firstName']:
            try:
                await page.wait_for_selector(sel, timeout=3000)
                await page.fill(sel, client["first_name"])
                break
            except PlaywrightTimeoutError:
                continue

        for sel in ['input[name="lastName"]', 'input[id*="lastName"]', '#lastName']:
            try:
                await page.fill(sel, client["last_name"])
                break
            except Exception:
                continue

        # Date of birth
        for sel in ['input[name="dateOfBirth"]', 'input[id*="dob"]', '#dateOfBirth']:
            try:
                await page.fill(sel, client["dob"])
                break
            except Exception:
                continue

        # Some forms use separate day/month/year selects
        try:
            await page.select_option('select[name*="day"], select[id*="day"]',   value=dob_day.lstrip("0") or "1")
            await page.select_option('select[name*="month"], select[id*="month"]', value=dob_month.lstrip("0") or "1")
            await page.select_option('select[name*="year"], select[id*="year"]',  value=dob_year)
        except Exception:
            pass  # DOB might already be handled by the combined field above

        # Phone
        for sel in ['input[name="phone"]', 'input[id*="phone"]', 'input[type="tel"]']:
            try:
                await page.fill(sel, client.get("phone", ""))
                break
            except Exception:
                continue

        # Address — simplified to street + city fields
        address_parts = [p.strip() for p in client.get("address", "").split(",")]
        if address_parts:
            for sel in ['input[name="address"]', 'input[name="addressLine1"]', 'input[id*="address"]']:
                try:
                    await page.fill(sel, address_parts[0])
                    break
                except Exception:
                    continue
        if len(address_parts) > 1:
            for sel in ['input[name="city"]', 'input[id*="city"]']:
                try:
                    await page.fill(sel, address_parts[1])
                    break
                except Exception:
                    continue

        # ── Email & password ─────────────────────────────────────────────────
        print("[registrar] Filling email and password")
        for sel in ['input[name="email"]', 'input[type="email"]', 'input[id*="email"]']:
            try:
                await page.fill(sel, client["email"])
                break
            except Exception:
                continue

        for sel in ['input[name="password"]', 'input[type="password"]', 'input[id*="password"]']:
            try:
                await page.fill(sel, client["password"])
                break
            except Exception:
                continue

        # Confirm password field (if present)
        for sel in ['input[name="confirmPassword"]', 'input[id*="confirmPassword"]']:
            try:
                await page.fill(sel, client["password"])
                break
            except Exception:
                continue

        # ── Accept T&Cs ──────────────────────────────────────────────────────
        for sel in [
            'input[type="checkbox"][name*="terms"]',
            'input[type="checkbox"][id*="terms"]',
            'input[type="checkbox"][name*="agree"]',
        ]:
            try:
                checkbox = await page.query_selector(sel)
                if checkbox and not await checkbox.is_checked():
                    await checkbox.check()
                break
            except Exception:
                continue

        # ── Submit ───────────────────────────────────────────────────────────
        print("[registrar] Submitting enrolment form")
        for sel in [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Join")',
            'button:has-text("Enroll")',
            'button:has-text("Register")',
        ]:
            try:
                submit_btn = await page.query_selector(sel)
                if submit_btn:
                    await submit_btn.click()
                    break
            except Exception:
                continue

        await page.wait_for_timeout(5000)

        # ── Check for CAPTCHA after submit ───────────────────────────────────
        if await _captcha_present(page):
            raise CaptchaRequiredError(
                "CAPTCHA appeared after form submission. "
                "Manual intervention or a CAPTCHA-solving service is required."
            )

        # ── Extract Aeroplan number ──────────────────────────────────────────
        print("[registrar] Looking for Aeroplan number in confirmation")
        page_text = await page.inner_text("body")
        # Aeroplan numbers are 9-digit numeric strings
        matches = re.findall(r"\b(\d{9})\b", page_text)
        aeroplan_number = matches[0] if matches else ""

        if not aeroplan_number:
            # Try common confirmation selectors
            for sel in [
                '[data-testid*="aeroplan"]',
                '[class*="memberNumber"]',
                '[id*="memberNumber"]',
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

        if not aeroplan_number:
            raise RuntimeError(
                "Registration appeared to succeed but could not extract Aeroplan number. "
                f"Page URL: {page.url}"
            )

        print(f"[registrar] Registered! Aeroplan number: {aeroplan_number}")
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

    # Check page text for CAPTCHA-related phrases
    try:
        text = await page.inner_text("body")
        if any(phrase in text.lower() for phrase in ["verify you are human", "i'm not a robot", "captcha"]):
            return True
    except Exception:
        pass

    return False


# ── Sync wrapper ──────────────────────────────────────────────────────────────

def register_account_sync(client: dict) -> dict:
    """Synchronous wrapper around register_account()."""
    return asyncio.run(register_account(client))
