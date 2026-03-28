"""
aeroplan_login.py
Playwright-based Aeroplan / Air Canada login helper.

Reverse-engineered Gigya login flow:
  1. Open aircanada.com, click the "Sign in" link.
  2. Type credentials into the Gigya-hosted form.
  3. After submission a 2FA screen appears.
  4. Click "Send Code" via absolute mouse co-ordinates (CSS pixels: 731, 370).
  5. Enter the 6-digit code into emailCode_0 input inside the Gigya CVV iframe.
"""

import asyncio
from typing import Callable

from playwright.async_api import async_playwright, Browser, Page


# Hard-coded co-ordinates taken from CSS pixel layout at 1280×900 viewport.
# If the page ever changes these may need updating.
SEND_CODE_X = 731
SEND_CODE_Y = 370

AC_HOME_URL = "https://www.aircanada.com/home/us/en/aco/flights"


async def login(
    email: str,
    password: str,
    get_code_fn: Callable[[], str],
) -> tuple[Browser, Page]:
    """
    Log into Air Canada / Aeroplan using Playwright.

    Parameters
    ----------
    email       : Aeroplan account email address.
    password    : Aeroplan account password.
    get_code_fn : Zero-argument callable that returns the 6-digit 2FA code string.
                  Will be called after the "Send Code" button is clicked.

    Returns
    -------
    (browser, page) with an authenticated session.
    The *caller* is responsible for closing the browser when done:
        browser.close()
    """
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()

    print(f"[aeroplan_login] Navigating to {AC_HOME_URL}")
    await page.goto(AC_HOME_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)

    # ── Step 1: click the "Sign in" link ─────────────────────────────────────
    print("[aeroplan_login] Clicking Sign in link")
    await page.click("a[data-testid='signin-link'], a[href*='signin'], button:has-text('Sign in')")
    await page.wait_for_timeout(2500)

    # ── Step 2: fill in credentials ──────────────────────────────────────────
    # The active Gigya form uses a screenset role selector
    username_sel = '[data-screenset-roles="instance"] input[name="username"]'
    password_sel = '[data-screenset-roles="instance"] input[name="password"]'

    print("[aeroplan_login] Waiting for username field")
    await page.wait_for_selector(username_sel, timeout=15000)
    await page.fill(username_sel, email)
    await page.fill(password_sel, password)

    print("[aeroplan_login] Submitting credentials")
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(3000)

    # ── Step 3: trigger email 2FA ─────────────────────────────────────────────
    # "Send Code" button is at a fixed CSS pixel position in the Gigya screen.
    print(f"[aeroplan_login] Clicking Send Code at ({SEND_CODE_X}, {SEND_CODE_Y})")
    await page.mouse.click(SEND_CODE_X, SEND_CODE_Y)
    await page.wait_for_timeout(2000)

    # ── Step 4: get the 2FA code ──────────────────────────────────────────────
    print("[aeroplan_login] Waiting for 2FA code via get_code_fn()")
    code = get_code_fn()
    print(f"[aeroplan_login] Got 2FA code: {code}")

    # ── Step 5: enter the code in the emailCode_0 field ──────────────────────
    # The field lives inside an iframe served from p-api.aircanada.com
    code_input_sel = 'input[name="emailCode_0"]'

    # Try direct page first (in case not in iframe), then search frames
    try:
        await page.wait_for_selector(code_input_sel, timeout=5000)
        await page.click(code_input_sel)
        await page.keyboard.type(code)
    except Exception:
        # Locate the correct frame (hosted at p-api.aircanada.com)
        target_frame = None
        for frame in page.frames:
            if "p-api.aircanada.com" in frame.url:
                target_frame = frame
                break

        if target_frame is None:
            # Fall back to any frame that contains the input
            for frame in page.frames:
                try:
                    await frame.wait_for_selector(code_input_sel, timeout=2000)
                    target_frame = frame
                    break
                except Exception:
                    continue

        if target_frame is None:
            raise RuntimeError("Could not locate the 2FA code input field in any frame.")

        await target_frame.click(code_input_sel)
        await page.keyboard.type(code)

    # Submit the 2FA form
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(3000)

    print("[aeroplan_login] Login complete")
    return browser, page


# ── Convenience sync wrapper ──────────────────────────────────────────────────

def login_sync(
    email: str,
    password: str,
    get_code_fn: Callable[[], str],
) -> tuple[Browser, Page]:
    """Synchronous wrapper around the async login() coroutine."""
    return asyncio.run(login(email, password, get_code_fn))
