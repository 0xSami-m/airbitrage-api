"""
aeroplan_login.py
Aeroplan / Air Canada login using the OpenClaw managed browser.

Flow:
  1. Navigate to aircanada.com
  2. Click Sign in, fill Gigya credentials
  3. Handle 2FA (email or phone OTP)
  4. Extract session cookies
  5. Return cookies for use in subsequent requests

The OpenClaw browser is a real non-headless Chrome instance that passes
Air Canada's ThreatMetrix bot detection.
"""

import asyncio
import json
import subprocess
import time
from typing import Callable

# ── Browser control via OpenClaw CLI ─────────────────────────────────────────

def _oc(action: str, **kwargs) -> dict:
    """Call the openclaw browser tool via Python subprocess."""
    import sys, os
    # Use the browser tool directly via the MCP bridge
    # We'll use playwright with the real browser profile instead
    raise NotImplementedError("Use browser_login() directly")


AC_HOME_URL = "https://www.aircanada.com/home/ca/en/aco/flights"


async def login_with_browser(
    email: str,
    password: str,
    get_code_fn: Callable[[], str],
    browser_tool=None,
) -> dict:
    """
    Log into Air Canada using a real browser instance.

    Parameters
    ----------
    email        : Aeroplan account email
    password     : Aeroplan account password
    get_code_fn  : Called after 2FA is triggered; returns the OTP code string
    browser_tool : Optional async callable matching the browser tool interface.
                   If None, uses Playwright with stealth + real Chrome UA.

    Returns
    -------
    dict with 'cookies' (list) and 'local_storage' (dict) for session reuse
    """
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth
    import os
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/tmp/pw-browsers")

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    )
    context = await browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        },
    )
    page = await context.new_page()
    await Stealth().apply_stealth_async(page)

    print(f"[login] Navigating to {AC_HOME_URL}", flush=True)
    await page.goto(AC_HOME_URL, wait_until="load", timeout=30000)
    await page.wait_for_timeout(2000)

    # Dismiss cookie banner if present
    try:
        btn = await page.query_selector("#onetrust-accept-btn-handler")
        if btn:
            await btn.click()
            await page.wait_for_timeout(800)
    except Exception:
        pass

    # Click Sign in
    print("[login] Clicking Sign in", flush=True)
    for sel in ["a:has-text('Sign in')", "button:has-text('Sign in')", "[href*='signin']"]:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.click()
                break
        except Exception:
            continue
    await page.wait_for_timeout(3000)

    # Wait for Gigya login form — may be in an iframe or inline
    print("[login] Looking for login form", flush=True)
    username_sel = 'input[name="username"], input[name="email"], input[type="email"]'
    password_sel = 'input[name="password"], input[type="password"]'

    # Try main page first
    target = page
    try:
        await page.wait_for_selector(username_sel, timeout=8000)
    except Exception:
        # Try iframes
        for frame in page.frames:
            try:
                await frame.wait_for_selector(username_sel, timeout=2000)
                target = frame
                print(f"[login] Found login form in frame: {frame.url}", flush=True)
                break
            except Exception:
                continue

    # Fill credentials
    print("[login] Filling credentials", flush=True)
    await target.click(username_sel)
    await target.type(username_sel, email, delay=50)
    await target.click(password_sel)
    await target.type(password_sel, password, delay=50)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(4000)

    # Check if 2FA screen appeared
    page_text = await page.inner_text("body")
    needs_2fa = any(w in page_text.lower() for w in [
        "verification code", "one-time", "otp", "send code", "verify", "2fa", "emailcode"
    ])

    if needs_2fa:
        print("[login] 2FA required — triggering code send", flush=True)

        # Look for "Send Code" button
        for sel in [
            "button:has-text('Send Code')",
            "button:has-text('Send code')",
            "button:has-text('Send')",
            "input[value='Send Code']",
        ]:
            try:
                el = await page.query_selector(sel)
                if not el:
                    # try in frames
                    for frame in page.frames:
                        el = await frame.query_selector(sel)
                        if el:
                            break
                if el:
                    await el.click()
                    print(f"[login] Clicked Send Code via {sel}", flush=True)
                    break
            except Exception:
                continue
        else:
            # Fall back to hard-coded coordinates
            print("[login] Using hard-coded Send Code coordinates (731, 370)", flush=True)
            await page.mouse.click(731, 370)

        await page.wait_for_timeout(2000)

        # Get the OTP from caller
        print("[login] Waiting for OTP code...", flush=True)
        code = get_code_fn()
        print(f"[login] Got OTP: {code}", flush=True)

        # Enter code
        code_sels = [
            'input[name="emailCode_0"]',
            'input[name="code"]',
            'input[placeholder*="code" i]',
            'input[type="number"]',
            'input[type="tel"]',
        ]
        code_target = page
        code_sel_found = None

        for sel in code_sels:
            try:
                await page.wait_for_selector(sel, timeout=3000)
                code_sel_found = sel
                break
            except Exception:
                pass

        if not code_sel_found:
            for frame in page.frames:
                for sel in code_sels:
                    try:
                        await frame.wait_for_selector(sel, timeout=1000)
                        code_target = frame
                        code_sel_found = sel
                        break
                    except Exception:
                        pass
                if code_sel_found:
                    break

        if code_sel_found:
            await code_target.click(code_sel_found)
            await code_target.type(code_sel_found, code, delay=50)
            await page.keyboard.press("Enter")
        else:
            # Just type it — focus should be on the code field
            await page.keyboard.type(code)
            await page.keyboard.press("Enter")

        await page.wait_for_timeout(4000)

    # Verify login succeeded
    final_url = page.url
    final_text = await page.inner_text("body")
    logged_in = any(w in final_text.lower() for w in ["sign out", "my account", "aeroplan", "dashboard", "miles"])

    print(f"[login] Final URL: {final_url}", flush=True)
    print(f"[login] Logged in: {logged_in}", flush=True)

    if not logged_in:
        # Check for error messages
        for line in final_text.split("\n"):
            if any(w in line.lower() for w in ["incorrect", "invalid", "error", "failed", "wrong"]):
                print(f"[login] Error: {line.strip()}", flush=True)
        raise RuntimeError(f"Login failed. URL: {final_url}")

    # Extract cookies for session reuse
    cookies = await context.cookies()
    print(f"[login] Extracted {len(cookies)} cookies", flush=True)

    return {
        "browser": browser,
        "page": page,
        "context": context,
        "playwright": playwright,
        "cookies": cookies,
        "logged_in": logged_in,
    }


async def login(
    email: str,
    password: str,
    get_code_fn: Callable[[], str],
):
    """Returns (browser, page) — legacy interface for compatibility."""
    result = await login_with_browser(email, password, get_code_fn)
    return result["browser"], result["page"]


def login_sync(
    email: str,
    password: str,
    get_code_fn: Callable[[], str],
):
    """Synchronous wrapper."""
    return asyncio.run(login(email, password, get_code_fn))
