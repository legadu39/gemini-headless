# utils/consent_detector.py — Async NASA++ compat
from __future__ import annotations
from typing import Optional
from playwright.async_api import Page, TimeoutError as PwTimeoutError

# --- Sélecteurs & heuristiques ---
CONSENT_SELECTORS = [
    'form[action*="consent"] button[aria-label*="J\'accepte"]',
    'form[action*="consent"] button:has-text("J\'accepte")',
    'form[action*="consent"] button:has-text("Accept all")',
    'button:has-text("J\'accepte tout")',
    'button:has-text("Tout accepter")',
    'button:has-text("I agree")',
    'button[aria-label*="Accept"]',
]
BANNER_HINTS = ['consent.google.com', 'privacy', 'consent', 'cookie']

async def _maybe_on_consent(page: Page) -> bool:
    url = page.url or ""
    if any(h in url for h in BANNER_HINTS):
        return True
    try:
        return bool(await page.locator('form[action*="consent"]').first.count())
    except Exception:
        return False

async def _handle_once(page: Page, timeout_ms: int) -> bool:
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    except PwTimeoutError:
        pass

    on_banner = await _maybe_on_consent(page)
    if not on_banner:
        # scanner les sélecteurs usuels même si l’URL ne matche pas
        found = False
        for sel in CONSENT_SELECTORS:
            try:
                if await page.locator(sel).first.count():
                    found = True
                    break
            except Exception:
                continue
        if not found:
            return False

    for sel in CONSENT_SELECTORS:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                await loc.click(timeout=2000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=4000)
                except PwTimeoutError:
                    pass
                return True
        except PwTimeoutError:
            continue
        except Exception:
            continue

    try:
        await page.keyboard.press("Enter")
        try:
            await page.wait_for_load_state("networkidle", timeout=3000)
        except PwTimeoutError:
            pass
        if not await _maybe_on_consent(page):
            return True
    except Exception:
        pass
    return False

class ConsentDetector:
    @staticmethod
    async def handle_if_present(page: Page, timeout_ms: int = 3000, retries: int = 2) -> bool:
        tries = max(1, int(retries) + 1)
        for _ in range(tries):
            try:
                ok = await _handle_once(page, timeout_ms=timeout_ms)
                if ok:
                    return True
            except Exception:
                pass
            try:
                await page.wait_for_timeout(500)
            except Exception:
                pass
        return False
