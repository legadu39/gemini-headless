# gemini_headless/connectors/input_and_session.py
# CORRIGÉ V11.6 (Correctifs V11.5 + améliorations mineures)
# - FIX: @dataclass déclaration (SyntaxError corrigé)
# - FIX: JS booleans (True -> true) & retrait des sélecteurs :visible (CSS natif)
# - FIX: Suppression de wait_for(state="enabled"), remplacé par polling is_enabled()
# - ADD: Raccourci ARIA (clic Playwright) avant l’évaluation JS
# - KEEP: Orchestration retries/budgets, logs jlog, cache sélecteur

from __future__ import annotations

import asyncio
import json
import os
import time
import unicodedata
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any
from pathlib import Path
import re

from playwright.async_api import (
    Page,
    ElementHandle,
    Locator,
    TimeoutError as PWTimeout,
    Error as PlaywrightError,
)

# --- Logging (utilise jlog si disponible) ---
try:
    try:
        from ..collect.utils.logs import jlog
    except ImportError:
        from ..utils.logs import jlog  # type: ignore
except ImportError:
    try:
        from utils.logs import jlog  # type: ignore
    except ImportError:
        import sys

        def jlog(evt: str, **payload):
            try:
                payload.setdefault("ts", time.time())
                print(json.dumps({"evt": evt, **payload}), file=sys.stderr)
                sys.stderr.flush()
            except Exception:
                pass

# --- Configuration via ENV ---

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "").strip().lower())
    return v in {"1", "true", "yes", "y", "on"} if v else default


GH_INPUT_MAX_ATTEMPTS = _env_int("GH_INPUT_MAX_ATTEMPTS", 2)
GH_LOCATE_FILL_TIMEOUT_MS = _env_int("GH_LOCATE_FILL_TIMEOUT_MS", 4000)
GH_SUBMIT_BUTTON_CLICK_TIMEOUT_MS = _env_int(
    "GH_SUBMIT_BUTTON_CLICK_TIMEOUT_MS", 20000
)  # Timeout global pour _submit_by_button_click
GH_RETRY_DELAY_MS = _env_int("GH_RETRY_DELAY_MS", 300)
GH_POST_UPLOAD_STABILIZE_MS = _env_int("GH_POST_UPLOAD_STABILIZE_MS", 7000)
GH_SUBMIT_SELECTOR_OVERRIDE = os.getenv("GH_SUBMIT_SELECTOR_OVERRIDE", "").strip()
GH_INPUT_CACHE = _env_bool("GH_INPUT_CACHE", True)
GH_INPUT_CACHE_TTL = _env_int("GH_INPUT_CACHE_TTL", 7 * 24 * 3600)
GH_INPUT_CACHE_DIR = os.getenv("GH_INPUT_CACHE_DIR", str(Path.home() / ".gh_cache"))
CACHE_PATH = str(Path(GH_INPUT_CACHE_DIR) / "input_locator_cache_v2.json")

# --- Sélecteurs CSS préférés (input zone) ---
_SELECTORS_PREF_V3 = [
    'div[role="textbox"][aria-label="Demander à Gemini"]',
    'div[role="textbox"][contenteditable="true"]',
    'textarea[aria-label*="prompt" i]',
    '[data-testid*="chat-input"]',
    '[contenteditable="true"][aria-label]',
    'textarea[aria-label]',
    'main div[role="textbox"]',
    'main textarea',
    'textarea',
    '[contenteditable="true"]',
]

# Fallback Playwright pour clic natif
SUBMIT_BUTTON_SELECTOR_FALLBACK_PLAYWRIGHT = (
    "button[aria-label='Envoyer le message'], button[aria-label='Send message']"
)

# --- Cache sélecteurs ---

@dataclass
class CacheEntry:
    selector: str
    ts: float


_cache_data: Optional[Dict[str, Any]] = None


def _load_cache() -> dict:
    global _cache_data
    if not GH_INPUT_CACHE:
        return {}
    if _cache_data is not None:
        return _cache_data
    try:
        cache_file = Path(CACHE_PATH)
    except Exception:
        _cache_data = {}
        return _cache_data
    try:
        if cache_file.exists():
            with cache_file.open("r", encoding="utf-8") as f:
                _cache_data = json.load(f)
                jlog("cache_loaded", path=CACHE_PATH, entries=len(_cache_data))
                return _cache_data
        else:
            _cache_data = {}
            return _cache_data
    except Exception as e:
        jlog("cache_load_error", path=CACHE_PATH, error=str(e), level="WARN")
        _cache_data = {}
        return _cache_data


def _save_cache(d: dict) -> None:
    global _cache_data
    _cache_data = d
    if not GH_INPUT_CACHE:
        return
    try:
        cache_file = Path(CACHE_PATH)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception as p_err:
        jlog("cache_save_path_error", path=CACHE_PATH, error=str(p_err), level="WARN")
        return
    try:
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
    except Exception as e:
        jlog("cache_save_error", path=CACHE_PATH, error=str(e), level="WARN")


def _cache_key(url: str) -> str:
    try:
        from urllib.parse import urlparse

        u = urlparse(url)
        return f"{u.hostname or 'nohost'}|{u.path or '/'}"
    except Exception:
        return "default_key"


def _get_cached_selector(page: Page) -> Optional[str]:
    if not GH_INPUT_CACHE:
        return None
    key = _cache_key(page.url)
    data = _load_cache()
    try:
        entry_data = data.get(key)
    except Exception:
        return None
    if not isinstance(entry_data, dict):
        return None
    try:
        entry = CacheEntry(**entry_data)
    except Exception:
        return None
    if (time.time() - entry.ts) > GH_INPUT_CACHE_TTL:
        jlog("cache_ttl_expired", key=key)
        return None
    return entry.selector if isinstance(entry.selector, str) and entry.selector else None


def _put_cached_selector(page: Page, selector: str) -> None:
    if not GH_INPUT_CACHE or not selector:
        return
    key = _cache_key(page.url)
    data = _load_cache()
    try:
        entry = CacheEntry(selector=selector, ts=time.time())
        if not isinstance(data, dict):
            data = {}
        data[key] = entry.__dict__
        _save_cache(data)
        jlog("cache_put", key=key, selector=selector)
    except Exception as e:
        jlog("cache_put_error", key=key, error=str(e), level="WARN")


# --- Interaction helpers ---

async def _submit_by_enter(page: Page) -> Tuple[bool, str]:
    """(Fallback Python ultime) Tente de soumettre en appuyant sur Entrée."""
    start_time = time.perf_counter()
    reason = "submit_by_enter_init"
    try:
        focused_element: Optional[ElementHandle] = await page.evaluate_handle(
            "document.activeElement"
        )
        is_input_area = False
        tag_name = "none"
        if focused_element:
            tag_name = await focused_element.evaluate("el => el.tagName.toLowerCase()")
            is_input_area = tag_name in ["textarea", "div"] and await focused_element.evaluate(
                "el => el.getAttribute('role') === 'textbox' || el.isContentEditable"
            )
        if not is_input_area:
            jlog("submit_by_enter_refocusing", tag=tag_name)
            try:
                input_locator = page.locator(
                    ", ".join(_SELECTORS_PREF_V3)
                ).first
                await input_locator.focus(timeout=500)
                await asyncio.sleep(0.05)
            except Exception as focus_err:
                reason = f"refocus_failed: {str(focus_err).splitlines()[0]}"
                jlog("submit_by_enter_refocus_failed", error=reason, level="WARN")
        await page.keyboard.press("Enter")
        reason = "success_python_enter_key"
        jlog(
            "submit_by_enter_success",
            ms=int((time.perf_counter() - start_time) * 1000),
        )
        return True, reason
    except PlaywrightError as e:
        reason = f"playwright_error: {str(e).splitlines()[0]}"
        jlog(
            "submit_by_enter_failed",
            error=reason,
            error_type=type(e).__name__,
            level="WARN",
        )
        return False, reason
    except Exception as e:
        reason = f"unexpected_error: {str(e).splitlines()[0]}"
        jlog(
            "submit_by_enter_failed",
            error=reason,
            error_type=type(e).__name__,
            level="ERROR",
        )
        return False, reason


# --- SCRIPT JS V11.6 (correctifs V11.5) ---
_JS_CLICK_SUBMIT_BUTTON_V11_6 = r"""
async () => {
    // Helpers
    const isVisible = (el) => {
        if (!el || !el.isConnected) return false;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return false;
        const style = window.getComputedStyle(el);
        if (style.visibility === 'hidden' || style.display === 'none' || style.opacity === '0') return false;
        let parent = el.parentElement;
        while (parent) {
            const ps = window.getComputedStyle(parent);
            if (ps.display === 'none' || ps.visibility === 'hidden') return false;
            parent = parent.parentElement;
        }
        return true;
    };
    const getText = (el) => (el.textContent || el.innerText || el.getAttribute('aria-label') || '').trim();
    const isEffectivelyDisabled = (el) => {
        if (!el) return true;
        if (el.disabled || el.getAttribute('aria-disabled') === 'true' || el.hasAttribute('disabled')) return true;
        if (el.classList && (el.classList.contains('disabled') || el.classList.contains('mdc-button--disabled') || el.classList.contains('mat-mdc-button-disabled') || el.classList.contains('is-loading') || el.classList.contains('is-disabled'))) return true;
        return false;
    };

    // Selectors (sans :visible)
    const selectors = [
        "button[aria-label='Envoyer le message']",
        "button[aria-label='Send message']",
        "button[data-testid='send-button']",
        "button:has(svg path[d*='M3 13'])"
    ];
    const targetIconPathD = "M3 13";
    let candidates = [];

    // 1) Collect candidates by selectors
    for (const selector of selectors) {
        try {
            document.querySelectorAll(selector).forEach(el => {
                if (el && !candidates.includes(el)) candidates.push(el);
            });
        } catch (e) {}
    }
    // 2) Extra: find by SVG
    try {
        document.querySelectorAll('button svg path').forEach(path => {
            const d = path.getAttribute('d') || '';
            if (d.includes(targetIconPathD)) {
                const btn = path.closest('button');
                if (btn && !candidates.includes(btn)) candidates.push(btn);
            }
        });
    } catch (e) {}

    // 3) Score visible candidates
    let bestCandidate = null; let maxScore = -Infinity;
    for (const el of candidates) {
        if (!isVisible(el)) continue;
        let score = 0;
        const label = getText(el).toLowerCase();
        const disabled = isEffectivelyDisabled(el);
        if (label.includes('envoyer') || label.includes('send')) score += 50;
        if (el.matches("button[data-testid='send-button']")) score += 40;
        if (el.querySelector(`svg path[d*='${targetIconPathD}']`)) score += 30;
        if (disabled) score -= 1000;
        if (score > maxScore) { maxScore = score; bestCandidate = el; }
    }

    // 4) Poll if disabled
    const pollTimeoutMs = 15000;
    const pollIntervalMs = 250;
    const startTime = Date.now();
    let candidateBecameEnabledDuringPoll = false;
    if (bestCandidate && maxScore < 0) {
        while (Date.now() - startTime < pollTimeoutMs) {
            await new Promise(r => setTimeout(r, pollIntervalMs));
            if (!bestCandidate.isConnected) break;
            if (isVisible(bestCandidate) && !isEffectivelyDisabled(bestCandidate)) {
                maxScore = 100;
                candidateBecameEnabledDuringPoll = true;
                break;
            }
        }
    }

    // 5) Try normal click
    if (bestCandidate && maxScore >= 0) {
        try {
            bestCandidate.focus();
            bestCandidate.click();
            return { clicked: true, success: true, selector_info: `JS Heuristic V11.6 (score ${maxScore})`, element_outerHTML: bestCandidate.outerHTML.substring(0, 200) };
        } catch (e) {
            try {
                const clickEvent = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
                bestCandidate.dispatchEvent(clickEvent);
                return { clicked: true, success: true, selector_info: `JS Heuristic V11.6 (dispatchEvent)`, element_outerHTML: bestCandidate.outerHTML.substring(0, 200) };
            } catch (e2) {}
        }
    }

    // 6) Forced click (uncertain)
    if (bestCandidate && !candidateBecameEnabledDuringPoll) {
        try {
            bestCandidate.focus();
            bestCandidate.click();
            return { clicked: true, success: false, error: 'Used FORCED click on potentially disabled button (success unknown)', selector_info: `JS Forced Click V11.6 (score ${maxScore})`, element_outerHTML: bestCandidate.outerHTML.substring(0, 200) };
        } catch (eForced) {}
    }

    // 7) Enter fallback (JS)
    let fallbackAttempted = false;
    let fallbackSucceeded = false;
    let fallbackError = 'Enter fallback not reached or failed internally';
    try {
        const inputSelectors = [
          'div[role="textbox"][contenteditable="true"][aria-label*="prompt" i]',
          'div[role="textbox"][contenteditable="true"]',
          'textarea[aria-label*="prompt" i]'
        ];
        let inputBox = null;
        for (const sel of inputSelectors) { inputBox = document.querySelector(sel); if (inputBox) break; }
        let targetElement = document.activeElement;
        if (inputBox && typeof inputBox.focus === 'function') { inputBox.focus(); await new Promise(r => setTimeout(r, 150)); targetElement = inputBox; }
        else { if (!targetElement || targetElement === document.body || targetElement === document.documentElement) { targetElement = document.body; } }
        const enterDown = new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true, cancelable: true });
        targetElement.dispatchEvent(enterDown); fallbackAttempted = true;
        await new Promise(r => setTimeout(r, 60));
        const enterUp = new KeyboardEvent('keyup', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true, cancelable: true });
        targetElement.dispatchEvent(enterUp); fallbackSucceeded = true;
        fallbackError = 'Used Keyboard Enter fallback V11.6 (success unknown)';
    } catch (eEnter) {
        fallbackError = `Keyboard Enter fallback V11.6 CRASHED: ${eEnter}`;
        fallbackSucceeded = false;
    }
    return { clicked: true, success: false, error: fallbackError, selector_info: `JS Enter Fallback V11.6 (attempted: ${fallbackAttempted}, dispatched_ok: ${fallbackSucceeded})` };
}
"""


# --- Submit par clic bouton (JS + fallbacks Python) ---
async def _submit_by_button_click(page: Page, timeout_ms: int) -> Tuple[bool, str]:
    """
    (V11.6) Tente d'abord un clic ARIA Playwright court, sinon évalue le JS V11.6.
    Retourne (succès, raison/info).
    """
    start_time_global = time.perf_counter()

    # 0) Shortcut ARIA (souvent très robuste et "humain")
    try:
        aria_btn = page.get_by_role("button", name=re.compile(r"Envoyer|Send", re.I))
        await aria_btn.first.click(timeout=1000)
        jlog("submit_by_button_shortcut_aria_click_ok")
        return True, "ARIA button click (shortcut)"
    except Exception:
        pass

    # 1) JS evaluate
    js_budget_ms = max(17000, int(timeout_ms * 0.9))
    external_timeout_ms = js_budget_ms + 2000
    jlog(
        "submit_by_button_attempt_start_v11_js_polling",
        timeout_ms=timeout_ms,
        js_budget_ms=js_budget_ms,
        external_timeout_ms=external_timeout_ms,
    )

    js_result: Dict[str, Any] = {}
    last_fail_reason = "js_init"
    js_execution_successful = False
    t_js_start = time.perf_counter()
    jlog("submit_by_button_js_evaluate_start", budget_ms=js_budget_ms)

    try:
        js_result = await asyncio.wait_for(
            page.evaluate(_JS_CLICK_SUBMIT_BUTTON_V11_6),
            timeout=external_timeout_ms / 1000.0,
        )
        js_execution_successful = True
    except asyncio.TimeoutError:
        last_fail_reason = f"js_evaluate_asyncio_timeout_{external_timeout_ms}ms"
        jlog(
            "submit_by_button_js_evaluate_asyncio_timeout",
            budget_ms=external_timeout_ms,
            level="ERROR",
        )
    except PlaywrightError as e_js_pw_error:
        last_fail_reason = (
            f"js_evaluate_playwright_error: {str(e_js_pw_error).splitlines()[0]}"
        )
        jlog(
            "submit_by_button_js_evaluate_playwright_error",
            error=last_fail_reason,
            level="ERROR",
        )
    except Exception as e_js_unexp:
        last_fail_reason = f"js_evaluate_unexpected_error: {str(e_js_unexp).splitlines()[0]}"
        jlog(
            "submit_by_button_js_evaluate_unexpected_error",
            error=last_fail_reason,
            error_type=type(e_js_unexp).__name__,
            level="ERROR",
        )
    finally:
        js_duration_ms = int((time.perf_counter() - t_js_start) * 1000)
        jlog(
            "submit_by_button_js_evaluate_end",
            duration_ms=js_duration_ms,
            success=js_execution_successful,
            result_summary=str(js_result)[:100],
            last_reason_if_fail=last_fail_reason if not js_execution_successful else "N/A",
        )

    if not js_execution_successful:
        return False, last_fail_reason

    if js_result.get("clicked") and js_result.get("success"):
        reason_success = js_result.get("selector_info", "javascript_click_with_poll")
        jlog(
            "submit_by_button_success_v11",
            method=reason_success,
            selector_info=reason_success,
            ms=int((time.perf_counter() - start_time_global) * 1000),
        )
        return True, reason_success

    elif (
        js_result.get("clicked")
        and not js_result.get("success")
        and "Forced Click" in js_result.get("selector_info", "")
    ):
        last_fail_reason = js_result.get(
            "error",
            "Used FORCED click on potentially disabled button (success unknown)",
        )
        jlog(
            "submit_by_button_js_used_forced_click",
            reason=last_fail_reason,
            selector_info=js_result.get("selector_info"),
            level="WARN",
        )
        return False, last_fail_reason

    elif (
        js_result.get("clicked")
        and not js_result.get("success")
        and "Enter Fallback" in js_result.get("selector_info", "")
    ):
        last_fail_reason = js_result.get(
            "error", "Used Keyboard Enter fallback (success unknown)"
        )
        jlog(
            "submit_by_button_js_used_enter_fallback",
            reason=last_fail_reason,
            selector_info=js_result.get("selector_info"),
            level="WARN",
        )
        return False, last_fail_reason

    else:
        last_fail_reason = js_result.get("error", "js_unknown_failure_state")
        jlog("submit_by_button_js_reported_failure", reason=last_fail_reason, level="ERROR")
        return False, last_fail_reason


# --- Helpers de valeur (sûrs) ---
async def get_value_locator(locator: Locator) -> str:
    val = ""
    timeout_val = 150
    try:
        val = await locator.input_value(timeout=timeout_val)
        return val if val is not None else ""
    except Exception:
        pass
    try:
        val = await locator.text_content(timeout=timeout_val)
        return val if val is not None else ""
    except Exception:
        pass
    try:
        val = await locator.evaluate("el => el.value || el.textContent", timeout=timeout_val)
        return val if val is not None else ""
    except Exception:
        pass
    return ""


def _normalize_str(s: str) -> str:
    if not isinstance(s, str):
        return ""
    try:
        s_nfd = unicodedata.normalize("NFD", s)
        s_no_diacritics = "".join(ch for ch in s_nfd if unicodedata.category(ch) != "Mn")
        return s_no_diacritics.lower().replace("\u00a0", " ")
    except Exception as e:
        jlog("normalize_str_failed", error=str(e), level="WARN", snippet=s[:50])
        return s.lower().replace("\u00a0", " ")


# --- API principale ---
async def fast_send_prompt(page: Page, prompt: str, *, is_post_upload: bool = False) -> bool:
    """
    (V11.6) Orchestre localisation+injection et soumission (JS V11.6 + Fallbacks Python), avec retries.
    """
    t_start_total = time.perf_counter()

    locate_focus_budget_ms = GH_LOCATE_FILL_TIMEOUT_MS
    submit_action_budget_ms = GH_SUBMIT_BUTTON_CLICK_TIMEOUT_MS
    post_upload_wait_ms = GH_POST_UPLOAD_STABILIZE_MS if is_post_upload else 0
    buffer_ms = 1000
    typing_estimate_ms = max(5000, len(prompt) * 15)
    single_attempt_budget_ms = (
        locate_focus_budget_ms
        + typing_estimate_ms
        + post_upload_wait_ms
        + submit_action_budget_ms
        + buffer_ms
    )
    overall_budget_ms = (
        single_attempt_budget_ms * GH_INPUT_MAX_ATTEMPTS
        + ((GH_INPUT_MAX_ATTEMPTS - 1) * GH_RETRY_DELAY_MS)
    )
    overall_deadline = t_start_total + overall_budget_ms / 1000.0

    jlog(
        "fast_send_prompt_budget_v11_js_polling",
        total_budget_ms=int(overall_budget_ms),
        attempts=GH_INPUT_MAX_ATTEMPTS,
        locate_focus_ms=locate_focus_budget_ms,
        typing_estimate_ms=typing_estimate_ms,
        post_upload_wait_ms=post_upload_wait_ms,
        submit_action_ms=submit_action_budget_ms,
        retry_delay_ms=GH_RETRY_DELAY_MS,
        is_post_upload=is_post_upload,
    )

    time_left_ms = lambda: max(0, int((overall_deadline - time.perf_counter()) * 1000))

    final_success = False
    last_error_reason = "unknown_init"
    used_input_selector = "none"
    input_locator: Optional[Locator] = None
    cache_invalidated_this_run = False
    locate_focus_failed_previous_attempt = False

    for attempt in range(1, GH_INPUT_MAX_ATTEMPTS + 1):
        t_start_attempt = time.perf_counter()
        jlog(
            "fast_send_attempt_start",
            attempt=attempt,
            max_attempts=GH_INPUT_MAX_ATTEMPTS,
            is_post_upload=is_post_upload,
            time_left_ms=time_left_ms(),
        )
        stage = "init"
        submit_ok = False
        type_ok = False
        locate_focus_ok = False
        locate_focus_failed_this_attempt = False
        cache_hit_this_attempt = False
        submit_reason = "not_attempted"

        min_required_time = 1000 + typing_estimate_ms + post_upload_wait_ms + submit_action_budget_ms
        if time_left_ms() < min_required_time:
            if last_error_reason in {"unknown_init", "not_attempted"}:
                last_error_reason = f"timeout_before_attempt_{attempt}"
            jlog(
                "fast_send_abort_timeout",
                stage=stage,
                attempt=attempt,
                time_left_ms=time_left_ms(),
                min_required_ms=min_required_time,
            )
            break

        try:
            # 1) Choix sélecteur input
            stage = "select_selector"
            target_selector = None
            cached_selector = _get_cached_selector(page)
            should_invalidate_cache = (
                attempt > 1
                and locate_focus_failed_previous_attempt
                and cached_selector == used_input_selector
                and not cache_invalidated_this_run
            )
            if should_invalidate_cache:
                jlog(
                    "invalidating_cached_selector_on_locate_fail",
                    selector=cached_selector,
                    attempt=attempt,
                )
                data = _load_cache()
                data.pop(_cache_key(page.url), None)
                _save_cache(data)
                cached_selector = None
                cache_invalidated_this_run = True

            if attempt == 1:
                if cached_selector:
                    target_selector = cached_selector
                    cache_hit_this_attempt = True
                    jlog("using_selector", selector=target_selector, attempt=attempt, source="cache")
                else:
                    target_selector = _SELECTORS_PREF_V3[0]
                    jlog(
                        "using_selector",
                        selector=target_selector,
                        attempt=attempt,
                        source="first_preferred",
                    )
            else:
                if used_input_selector and not should_invalidate_cache:
                    target_selector = used_input_selector
                    jlog(
                        "using_selector",
                        selector=target_selector,
                        attempt=attempt,
                        source="retry_last_successful_input",
                    )
                elif cached_selector:
                    target_selector = cached_selector
                    cache_hit_this_attempt = True
                    jlog(
                        "using_selector",
                        selector=target_selector,
                        attempt=attempt,
                        source="cache_after_invalidate",
                    )
                else:
                    target_selector = _SELECTORS_PREF_V3[0]
                    jlog(
                        "using_selector",
                        selector=target_selector,
                        attempt=attempt,
                        source="retry_first_preferred",
                    )

            if not target_selector:
                last_error_reason = "no_target_selector_logic_error"
                jlog("no_target_selector_found_logic_error", attempt=attempt, level="ERROR")
                break

            input_locator = page.locator(target_selector).first

            # 2) Localisation + focus (enabled via polling)
            stage = "locate_focus"
            locate_budget_ms = min(
                locate_focus_budget_ms,
                time_left_ms() - (typing_estimate_ms + post_upload_wait_ms + submit_action_budget_ms + 200),
            )
            if locate_budget_ms < 500:
                last_error_reason = "timeout_before_locate"
                jlog("fast_send_abort_timeout", stage=stage, attempt=attempt, budget=locate_budget_ms)
                break

            sub_stage = "wait_visible"
            try:
                visible_timeout = max(100, int(locate_budget_ms * 0.4))
                await input_locator.wait_for(state="visible", timeout=visible_timeout)
                jlog(
                    "locate_focus_sub_ok",
                    sub_stage=sub_stage,
                    attempt=attempt,
                    selector=target_selector,
                    ms=int((time.monotonic() - t_start_attempt) * 1000),
                )
                sub_stage = "check_enabled"
                enabled_timeout = max(100, int(locate_budget_ms * 0.3))
                enabled_deadline = time.monotonic() + enabled_timeout / 1000.0
                is_currently_enabled = False
                while time.monotonic() < enabled_deadline:
                    try:
                        if await input_locator.is_enabled(timeout=50):
                            is_currently_enabled = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.05)
                if not is_currently_enabled:
                    raise PlaywrightError(
                        f"Element [{target_selector}] did not become enabled within {enabled_timeout}ms"
                    )
                jlog(
                    "locate_focus_sub_ok",
                    sub_stage=sub_stage,
                    attempt=attempt,
                    selector=target_selector,
                    ms=int((time.monotonic() - t_start_attempt) * 1000),
                )
                sub_stage = "set_focus"
                focus_timeout = max(100, int(locate_budget_ms * 0.2))
                await input_locator.click(timeout=focus_timeout)
                await input_locator.focus(timeout=focus_timeout)
                jlog(
                    "locate_focus_sub_ok",
                    sub_stage=sub_stage,
                    attempt=attempt,
                    selector=target_selector,
                    ms=int((time.monotonic() - t_start_attempt) * 1000),
                )
                locate_focus_ok = True
            except Exception as locate_err:
                locate_focus_failed_this_attempt = True
                last_error_reason = f"locate_focus_failed_{sub_stage}: {str(locate_err).splitlines()[0]}"
                if not cache_hit_this_attempt and attempt == 1:
                    current_index = (
                        _SELECTORS_PREF_V3.index(target_selector)
                        if target_selector in _SELECTORS_PREF_V3
                        else -1
                    )
                    next_index = current_index + 1
                    if next_index < len(_SELECTORS_PREF_V3):
                        next_selector = _SELECTORS_PREF_V3[next_index]
                        jlog(
                            "locate_focus_failed_trying_next_selector",
                            attempt=attempt,
                            failed_selector=target_selector,
                            next_selector=next_selector,
                            error=str(locate_err).splitlines()[0],
                        )
                        target_selector = next_selector
                        input_locator = page.locator(target_selector).first
                        try:
                            await input_locator.wait_for(
                                state="visible", timeout=visible_timeout
                            )
                            # enabled via polling
                            enabled_timeout = max(100, int(locate_budget_ms * 0.3))
                            enabled_deadline = time.monotonic() + enabled_timeout / 1000.0
                            is_currently_enabled = False
                            while time.monotonic() < enabled_deadline:
                                try:
                                    if await input_locator.is_enabled(timeout=50):
                                        is_currently_enabled = True
                                        break
                                except Exception:
                                    pass
                                await asyncio.sleep(0.05)
                            if not is_currently_enabled:
                                raise PlaywrightError(
                                    f"Element [{target_selector}] did not become enabled within {enabled_timeout}ms"
                                )
                            focus_timeout = max(100, int(locate_budget_ms * 0.2))
                            await input_locator.click(timeout=focus_timeout)
                            await input_locator.focus(timeout=focus_timeout)
                            jlog(
                                "locate_focus_sub_ok_on_retry_selector",
                                sub_stage="all",
                                attempt=attempt,
                                selector=target_selector,
                            )
                            locate_focus_ok = True
                            locate_focus_failed_this_attempt = False
                            last_error_reason = ""
                        except Exception as locate_retry_err:
                            last_error_reason = (
                                f"locate_focus_retry_failed: {str(locate_retry_err).splitlines()[0]}"
                            )
                            jlog(
                                "locate_focus_retry_selector_failed",
                                attempt=attempt,
                                selector=target_selector,
                                error=last_error_reason,
                            )
                            raise PlaywrightError(
                                f"Failed locate/focus with primary and fallback selector [{target_selector}]: {last_error_reason}"
                            )
                    else:
                        raise PlaywrightError(
                            f"Failed at sub_stage '{sub_stage}' for selector [{target_selector}] and no more fallbacks: {last_error_reason}"
                        )
                else:
                    raise PlaywrightError(
                        f"Failed at sub_stage '{sub_stage}' for selector [{target_selector}]: {last_error_reason}"
                    )

            # 3) Injection clavier
            stage = "type_prompt"
            await page.keyboard.type(prompt, delay=10)
            jlog(
                "type_prompt_success",
                attempt=attempt,
                len=len(prompt),
                ms=int((time.perf_counter() - t_start_attempt) * 1000),
            )
            type_ok = True

            # 4) Vérification post-injection (skippée)
            stage = "verify_typed_skipped"
            jlog("verify_typed_skipped", attempt=attempt, reason="Robustness v5.8")

            # Update cache
            used_input_selector = target_selector
            if not cache_hit_this_attempt and not should_invalidate_cache:
                _put_cached_selector(page, target_selector)

            # 5) Attente de stabilisation post-upload
            if is_post_upload and post_upload_wait_ms > 0:
                stage = "post_upload_wait"
                wait_budget_ms = min(
                    post_upload_wait_ms, time_left_ms() - (submit_action_budget_ms + 100)
                )
                jlog(
                    "post_upload_stabilization_wait_start",
                    configured_ms=GH_POST_UPLOAD_STABILIZE_MS,
                    actual_wait_ms=wait_budget_ms,
                    attempt=attempt,
                )
                if wait_budget_ms > 100:
                    await asyncio.sleep(wait_budget_ms / 1000.0)
                    jlog(
                        "post_upload_stabilization_wait_end",
                        duration_ms=wait_budget_ms,
                        attempt=attempt,
                    )
                else:
                    jlog(
                        "post_upload_stabilization_wait_skipped_low_budget",
                        budget=wait_budget_ms,
                        attempt=attempt,
                    )

            # 6) Submit (JS + fallbacks Python)
            stage = "submit"
            submit_budget_ms = min(submit_action_budget_ms, time_left_ms() - 100)
            if submit_budget_ms < 1000:
                last_error_reason = "timeout_before_submit"
                jlog(
                    "fast_send_abort_timeout",
                    stage=stage,
                    attempt=attempt,
                    budget=submit_budget_ms,
                    required=1000,
                )
                break

            submit_ok, submit_reason = await _submit_by_button_click(page, submit_budget_ms)
            submit_method_used = submit_reason
            if not submit_ok:
                last_error_reason = f"submit_failed_js_attempt: {submit_reason}"

            # Fallback Enter Playwright
            if not submit_ok:
                jlog(
                    "fast_send_attempt_trying_python_enter_fallback",
                    attempt=attempt,
                    stage=stage,
                    previous_reason=submit_reason,
                    level="WARN",
                )
                pw_enter_ok, pw_enter_reason = await _submit_by_enter(page)
                if pw_enter_ok:
                    submit_ok = True
                    submit_method_used = f"Python Enter Fallback ({pw_enter_reason})"
                    last_error_reason = ""
                    jlog(
                        "fast_send_attempt_python_enter_fallback_dispatched",
                        attempt=attempt,
                        stage=stage,
                        reason=submit_method_used,
                        level="WARN",
                    )
                else:
                    last_error_reason = (
                        f"submit_failed_js_and_py_enter: JS({submit_reason}) AND Py({pw_enter_reason})"
                    )
                    jlog(
                        "fast_send_attempt_python_enter_fallback_failed",
                        attempt=attempt,
                        stage=stage,
                        reason=last_error_reason,
                        level="ERROR",
                    )

            # Fallback Playwright native click (override ou défaut)
            if not submit_ok:
                jlog(
                    "fast_send_attempt_trying_python_native_click_fallback",
                    attempt=attempt,
                    stage=stage,
                    previous_reason=last_error_reason,
                    level="WARN",
                )
                native_click_selector = (
                    GH_SUBMIT_SELECTOR_OVERRIDE or SUBMIT_BUTTON_SELECTOR_FALLBACK_PLAYWRIGHT
                )
                try:
                    submit_locator = page.locator(native_click_selector).first
                    await submit_locator.wait_for(state="visible", timeout=1000)
                    await submit_locator.click(timeout=2000, force=True)
                    submit_ok = True
                    submit_method_used = f"Python Native Click Fallback ({native_click_selector})"
                    last_error_reason = ""
                    jlog(
                        "fast_send_attempt_python_native_click_fallback_dispatched",
                        attempt=attempt,
                        stage=stage,
                        reason=submit_method_used,
                        selector=native_click_selector,
                        level="WARN",
                    )
                except PWTimeout:
                    last_error_reason = (
                        f"submit_failed_native_click_fallback: Button not visible ({native_click_selector})"
                    )
                    jlog(
                        "fast_send_attempt_python_native_click_fallback_failed_timeout",
                        attempt=attempt,
                        stage=stage,
                        reason=last_error_reason,
                        selector=native_click_selector,
                        level="ERROR",
                    )
                except Exception as native_click_err:
                    last_error_reason = (
                        f"submit_failed_native_click_fallback: {str(native_click_err).splitlines()[0]} ({native_click_selector})"
                    )
                    jlog(
                        "fast_send_attempt_python_native_click_fallback_failed_exception",
                        attempt=attempt,
                        stage=stage,
                        reason=last_error_reason,
                        selector=native_click_selector,
                        error_type=type(native_click_err).__name__,
                        level="ERROR",
                    )

            if not submit_ok:
                jlog(
                    "fast_send_attempt_failed",
                    attempt=attempt,
                    stage=stage,
                    reason=last_error_reason,
                    is_post_upload=is_post_upload,
                    level="ERROR",
                )
                if attempt < GH_INPUT_MAX_ATTEMPTS:
                    await asyncio.sleep(GH_RETRY_DELAY_MS / 1000.0)
                    locate_focus_failed_previous_attempt = locate_focus_failed_this_attempt
                    continue
                else:
                    break

            # Succès
            final_success = True
            jlog(
                "fast_send_prompt_success_v11_js_polling",
                attempt=attempt,
                locate_method=used_input_selector,
                inject_method="keyboard.type",
                submit_method=submit_method_used,
                is_post_upload=is_post_upload,
                verification_skipped=True,
                attempt_ms=int((time.perf_counter() - t_start_attempt) * 1000),
                total_ms=int((time.perf_counter() - t_start_total) * 1000),
            )
            break

        except PlaywrightError as e:
            locate_focus_failed_this_attempt = stage in ["locate_focus"]
            last_error_reason = f"playwright_error_stage_{stage}: {str(e).splitlines()[0]}"
            jlog(
                "fast_send_attempt_playwright_error",
                attempt=attempt,
                stage=stage,
                error=last_error_reason,
                level="ERROR",
            )
            is_critical_error = (
                "closed" in str(e).lower()
                or "naviga" in str(e).lower()
                or "target was destroyed" in str(e).lower()
            )
            if is_critical_error:
                jlog("fast_send_abort_page_closed_or_navigated", attempt=attempt, stage=stage)
                break
            elif attempt < GH_INPUT_MAX_ATTEMPTS:
                await asyncio.sleep(GH_RETRY_DELAY_MS / 1000.0)
                locate_focus_failed_previous_attempt = locate_focus_failed_this_attempt
                continue
            else:
                break
        except Exception as e:
            locate_focus_failed_this_attempt = stage in ["locate_focus"]
            last_error_reason = f"unexpected_error_stage_{stage}: {str(e).splitlines()[0]}"
            jlog(
                "fast_send_attempt_unexpected_error",
                attempt=attempt,
                stage=stage,
                error=last_error_reason,
                error_type=type(e).__name__,
                level="CRITICAL",
            )
            break

        if not final_success and attempt < GH_INPUT_MAX_ATTEMPTS:
            locate_focus_failed_previous_attempt = locate_focus_failed_this_attempt

    if not final_success:
        jlog(
            "fast_send_prompt_failed_all_attempts",
            last_reason=last_error_reason,
            total_ms=int((time.perf_counter() - t_start_total) * 1000),
            level="ERROR",
        )

    return final_success


# --- Helper get_value (élément) ---
async def get_value(element: ElementHandle) -> str:
    val = ""
    timeout_val = 150
    try:
        val = await element.input_value(timeout=timeout_val)
        return val if val is not None else ""
    except Exception:
        pass
    try:
        val = await element.text_content(timeout=timeout_val)
        return val if val is not None else ""
    except Exception:
        pass
    try:
        val = await element.evaluate("el => el.value || el.textContent", timeout=timeout_val)
        return val if val is not None else ""
    except Exception:
        pass
    return ""
