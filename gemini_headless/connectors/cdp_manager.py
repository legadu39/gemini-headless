# -- coding: utf-8 --
from __future__ import annotations

"""
Gemini Headless — CDP Manager (Critical Mode · NASA++)
Endpoint-agnostic attach with atomic phases, deterministic logs, and a hard
stance on transport failures (first 'NoneType.send' == transport death).

Public API (backward-compatible):
async def attach_or_spawn(cfg, *, logger=None) -> tuple[browser, context]

Design goals / invariants implemented here:
Accept BOTH HTTP root endpoints (e.g. http://127.0.0.1:9222) and WS endpoints
(e.g. ws://127.0.0.1:9222/devtools/browser/…).
If HTTP root is available: create a new page via /json/new?{url} (about:blank + /app),
then attach and wait until at least one page is visible to Playwright.
If WS-only: create & attach a target using pure CDP (Target.createTarget),
then wait for at least one page to materialize in a context.
Emit canonical logs (JSON, additive only; never print to STDOUT):
{"evt":"cdp_connect","ok":true,"url":...}
{"evt":"cdp_create_target","ok":true,"url":...}
{"evt":"cdp_wait_page","pages":N}
{"evt":"cdp_attach","ok":true,"attach_ms":int}   ← KPI exposed here
Atomic attach_or_spawn: on any step failure, perform a clean rollback and
retry once from a stable state. Treat first 'NoneType.send' as hard transport
death → close/reconnect, recreate page/context, restart pipeline.
Provide a post-attach CDP probe (Runtime.evaluate 1+1) against a real page to
detect dead transports early, before higher layers attempt new_cdp_session,
expose_function, add_init_script, or locator.click.

Notes:
We keep the surface minimal here; page preparation (hook ordering, awaiters,
consent/repair) is handled by page_manager / input_and_session elsewhere.
We set browser._pw_handle = playwright_instance for later graceful teardown.
Changes in this version:
Removed event-loop blocking by offloading HTTP /json calls to a thread via
asyncio.to_thread, keeping dependencies minimal.
Exposed KPI attach_ms (milliseconds from attempt start to cdp_attach ok:true
or ok:false) to satisfy performance monitoring (<800 ms local target).
"""
import asyncio
import json
import os
import sys
import time
from typing import Any, Optional, Tuple, List
from urllib.parse import urlparse, urlencode, quote_plus
from playwright.async_api import async_playwright


# ─────────────────────────────────────────────────────────────────────────────
# Logging (JSON lines to logger or STDERR; never STDOUT)
# ─────────────────────────────────────────────────────────────────────────────
def _jlog(logger, evt: str, **payload) -> None:
    payload.setdefault("ts", time.time())
    try:
        line = json.dumps({"evt": evt, **payload}, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        line = json.dumps({"evt": evt, "unserializable": True}, ensure_ascii=False)
    try:
        if logger and hasattr(logger, "info"):
            logger.info(line)
        else:
            sys.stderr.write(line + "\n")
    except Exception:
        # last-ditch
        try:
            sys.stderr.write(line + "\n")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# URL helpers
# ─────────────────────────────────────────────────────────────────────────────
def _is_ws_endpoint(u: str) -> bool:
    try:
        return u.strip().lower().startswith(("ws://", "wss://"))
    except Exception:
        return False


def _is_http_root(u: str) -> bool:
    try:
        pr = urlparse(u)
        if pr.scheme.lower() not in ("http", "https"):
            return False
        # Root remote-debugging endpoint usually exposes /json, /json/version, /json/new
        return True
    except Exception:
        return False


def _http_root_base(u: str) -> str:
    pr = urlparse(u)
    base = f"{pr.scheme}://{pr.netloc}"
    return base.rstrip("/")


async def _playwright_connect(pw, url: str):
    # playwright accepts both HTTP root and WS URLs for connect_over_cdp
    return await pw.chromium.connect_over_cdp(url)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers (for /json/* on root endpoint) — non-blocking via to_thread
# ─────────────────────────────────────────────────────────────────────────────
def _sync_http_get_json(url: str) -> Optional[Any]:
    try:
        import urllib.request
        with urllib.request.urlopen(url, timeout=2.5) as resp:
            if resp.status != 200:
                return None
            raw = resp.read()
            return json.loads(raw.decode("utf-8", "ignore"))
    except Exception:
        return None


async def _http_get_json(url: str) -> Optional[Any]:
    # Offload blocking I/O to a thread to avoid freezing the event loop.
    return await asyncio.to_thread(_sync_http_get_json, url)


async def _http_create_target(http_root: str, target_url: str, logger=None) -> bool:
    try:
        # /json/new?{url} ; ensure URL is quoted safely (Chrome accepts raw too, but be strict)
        quoted = quote_plus(target_url)
        q = f"{http_root.rstrip('/')}/json/new?{quoted}"
        data = await _http_get_json(q)
        ok = bool(data and isinstance(data, dict) and data.get("id"))
        _jlog(logger, "cdp_create_target", ok=ok, url=target_url)
        return ok
    except Exception as e:
        _jlog(logger, "cdp_warning", msg="json_new_failed", error=str(e))
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Page/context discovery & probing
# ─────────────────────────────────────────────────────────────────────────────
def _pick_context_with_pages(browser) -> Optional[Any]:
    try:
        for ctx in list(browser.contexts):
            try:
                if list(getattr(ctx, "pages", [])):
                    return ctx
            except Exception:
                continue
    except Exception:
        pass
    return None


def _count_all_pages(browser) -> int:
    try:
        n = 0
        for ctx in list(browser.contexts):
            try:
                n += len(list(getattr(ctx, "pages", [])))
            except Exception:
                continue
        return n
    except Exception:
        return 0


async def _wait_pages(browser, *, deadline_s: float, logger=None) -> Optional[Any]:
    # Wait until any context has ≥1 page; log cdp_wait_page (pages=N)
    end = time.monotonic() + float(deadline_s)
    last = -1
    while time.monotonic() < end:
        await asyncio.sleep(0.20)
        n = _count_all_pages(browser)
        if n != last:
            _jlog(logger, "cdp_wait_page", pages=n)
            last = n
        if n >= 1:
            ctx = _pick_context_with_pages(browser)
            if ctx is not None:
                return ctx
    # Final report
    _jlog(logger, "cdp_wait_page", pages=_count_all_pages(browser))
    return None


async def _probe_cdp_session(context, logger=None, *, t0: Optional[float] = None) -> bool:
    """
    Hard gate against dead transports:
    - Create a CDP session on a real page and send a trivial command.
    - If anything looks like NoneType.send / closed pipe, return False.
    - On success, detach immediately (we only test viability).
    Emits cdp_attach with attach_ms if t0 provided.
    """
    try:
        pages = list(getattr(context, "pages", []))
        if not pages:
            # Consider as failed probe; still log with attach_ms if any.
            ms = int((time.monotonic() - t0) * 1000) if t0 is not None else None
            if ms is not None:
                _jlog(logger, "cdp_attach", ok=False, error="no_pages", hard=False, attach_ms=ms)
            else:
                _jlog(logger, "cdp_attach", ok=False, error="no_pages", hard=False)
            return False
        page = pages[-1]
        cdp = await context.new_cdp_session(page)
        try:
            await cdp.send("Runtime.enable")
            await cdp.send("Runtime.evaluate", {"expression": "1+1"})
        finally:
            try:
                await cdp.detach()
            except Exception:
                pass
        ms = int((time.monotonic() - t0) * 1000) if t0 is not None else None
        if ms is not None:
            _jlog(logger, "cdp_attach", ok=True, attach_ms=ms)
        else:
            _jlog(logger, "cdp_attach", ok=True)
        return True
    except Exception as e:
        msg = str(e) or ""
        hard = ("NoneType" in msg and ".send" in msg) or "detached" in msg.lower() or "closed" in msg.lower()
        ms = int((time.monotonic() - t0) * 1000) if t0 is not None else None
        if ms is not None:
            _jlog(logger, "cdp_attach", ok=False, error=msg, hard=hard, attach_ms=ms)
        else:
            _jlog(logger, "cdp_attach", ok=False, error=msg, hard=hard)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# WS-only target creation via pure CDP
# ─────────────────────────────────────────────────────────────────────────────
async def _ws_create_target_via_cdp(browser, *, url: str, logger=None) -> bool:
    """
    Use Browser-level CDP to create a target when only WS is provided.
    Target.createTarget will spawn a new tab; Playwright should reflect it.
    """
    try:
        # new_browser_cdp_session() → Browser CDP transport
        bcdp = await browser.new_browser_cdp_session()
        try:
            # Some Chromium builds require a non-empty URL; use about:blank then navigate later.
            out = await bcdp.send("Target.createTarget", {"url": url or "about:blank"})
            ok = bool(out and out.get("targetId"))
            _jlog(logger, "cdp_create_target", ok=ok, url=url or "about:blank")
            return ok
        finally:
            try:
                await bcdp.detach()
            except Exception:
                pass
    except Exception as e:
        _jlog(logger, "cdp_warning", msg="ws_create_target_failed", error=str(e))
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Atomic attach_or_spawn
# ─────────────────────────────────────────────────────────────────────────────
async def attach_or_spawn(cfg: Any, *, logger: Any | None = None) -> Tuple[Any, Any]:
    """
    Attach to an existing Chrome via CDP (preferred) or spawn Chromium when allowed.
    Returns (browser, context) with the invariant that context.pages is non-empty
    and CDP is live (probe passed). Atomic: on failure, cleans up and retries once.
    Environment:
      ALLOW_SPAWN = "0" → *forbid* local spawn; require cfg.cdp_url.
    """
    allow_spawn = os.getenv("ALLOW_SPAWN", "1") != "0"
    cdp_url: Optional[str] = getattr(cfg, "cdp_url", None)
    headless: bool = bool(getattr(cfg, "headless", False))
    # Decide mode
    use_cdp = bool(cdp_url) or not allow_spawn
    if not use_cdp and not allow_spawn:
        raise RuntimeError("ALLOW_SPAWN=0 and no cdp_url provided")

    async def _cleanup_pw(pw, browser):
        # Best-effort cleanup without raising
        try:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

    async def _do_connect_once() -> Tuple[Any, Any, Any]:
        """
        Returns (pw, browser, context). Raises on failure.
        Ensures context has ≥1 page and CDP probe succeeds.
        """
        t0 = time.monotonic()
        pw = await async_playwright().start()
        browser = None
        try:
            if use_cdp:
                if not cdp_url:
                    raise RuntimeError("cdp_url required for CDP attach")
                # Connect (works for HTTP root and WS endpoints)
                browser = await _playwright_connect(pw, cdp_url)
                setattr(browser, "_pw_handle", pw)  # for external graceful teardown
                _jlog(logger, "cdp_connect", ok=True, url=cdp_url)

                ctx = _pick_context_with_pages(browser)

                # If no pages, try to create one according to endpoint type
                if ctx is None:
                    if _is_http_root(cdp_url):
                        root = _http_root_base(cdp_url)
                        # Try about:blank first, then Gemini app
                        await _http_create_target(root, "about:blank", logger=logger)
                        await _http_create_target(root, "https://gemini.google.com/app", logger=logger)
                    else:
                        # WS-only: create target via pure CDP
                        await _ws_create_target_via_cdp(browser, url="about:blank", logger=logger)
                        await _ws_create_target_via_cdp(browser, url="https://gemini.google.com/app", logger=logger)

                    # Wait for pages to appear
                    ctx = await _wait_pages(browser, deadline_s=5.0, logger=logger)
                    if ctx is None:
                        # Final HTTP /json probe for diagnostics (HTTP root only)
                        if _is_http_root(cdp_url):
                            listing = await _http_get_json(_http_root_base(cdp_url) + "/json")
                            _jlog(logger, "cdp_debug_targets", count=len(listing or []))
                        raise RuntimeError("No context with pages after target creation")

                # Probe CDP viability against a real page (emits cdp_attach with attach_ms)
                if not await _probe_cdp_session(ctx, logger=logger, t0=t0):
                    raise RuntimeError("cdp_probe_failed")

                return pw, browser, ctx

            # Spawn path (allowed AND no cdp_url)
            browser = await pw.chromium.launch(headless=headless)
            setattr(browser, "_pw_handle", pw)
            _jlog(logger, "spawn_browser", ok=True, headless=headless)
            context = await browser.new_context()
            # Ensure at least one page
            try:
                page = await context.new_page()
                _ = page  # unused; page existence matters
            except Exception:
                pass
            # Log pages
            _jlog(logger, "cdp_wait_page", pages=len(list(getattr(context, "pages", []))))
            # Probe via a temporary page if possible (emits cdp_attach with attach_ms)
            if not await _probe_cdp_session(context, logger=logger, t0=t0):
                raise RuntimeError("cdp_probe_failed_spawn")
            return pw, browser, context

        except Exception:
            # Cleanup on failure
            await _cleanup_pw(pw, browser)
            raise

    # Atomic with ONE retry on transport death or attach failure
    attempt = 0
    last_err: Optional[str] = None
    while attempt < 2:
        try:
            pw, browser, context = await _do_connect_once()
            return browser, context
        except Exception as e:
            msg = str(e) or repr(e)
            last_err = msg
            hard = ("NoneType" in msg and ".send" in msg) or "cdp_probe_failed" in msg or "closed" in msg.lower()
            _jlog(logger, "cdp_attach_error", attempt=attempt, error=msg, hard=hard)
            attempt += 1
            if attempt >= 2:
                break
            # brief backoff before retry
            try:
                await asyncio.sleep(0.35)
            except Exception:
                pass

    # If we reach here, both attempts failed
    raise RuntimeError(f"attach_or_spawn_failed: {last_err or 'unknown'}")


__all__ = ["attach_or_spawn"]