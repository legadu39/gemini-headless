# bootstrap_profile.py (si c'est bien ce fichier qui est utilisé pour le connecteur)
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Gemini Headless — Connector (Critical Mode++)
(Copie de gemini_connector.py avec modifications pour upload externe)
"""

import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

from playwright.async_api import Page

# Imports internes (potentiellement différents chemins si ce fichier est à la racine)
try:
    from gemini_headless.connectors.page_manager import prepare_page
except ImportError:
    try: from .page_manager import prepare_page # type: ignore
    except Exception:
        async def prepare_page(cfg: Any, *, logger=None, network_debug: bool = False, consent_timeout_s: float = 4.0) -> Dict[str, Any]:
            raise RuntimeError("prepare_page not available")

try:
    from gemini_headless.connectors.network_sniffer import GeminiNetworkTap
except ImportError:
    try: from .network_sniffer import GeminiNetworkTap # type: ignore
    except Exception: GeminiNetworkTap = None

try:
    from gemini_headless.connectors.awaiter_engine import build_awaiter, await_answer
except ImportError:
    try: from .awaiter_engine import build_awaiter, await_answer # type: ignore
    except Exception:
        async def build_awaiter(page, sniffer=None, logger=None, **kwargs):
            raise RuntimeError("build_awaiter not available")
        async def await_answer(awaiter, dom_snaps=None, t0_ms=None, logger=None, **kwargs):
            raise RuntimeError("await_answer not available")

# --- Logging helper (identique) ---
def _jlog(logger, evt: str, **payload) -> None:
    # ... (code _jlog identique à gemini_connector.py) ...
    payload.setdefault("ts", time.time())
    rec = {"evt": evt, **payload}
    try:
        line = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        line = json.dumps({"evt": evt, "unserializable": True}, ensure_ascii=False)
    try:
        if logger and hasattr(logger, "info"):
            logger.info(line)
        else:
            sys.stderr.write(line + "\n")
            sys.stderr.flush()
    except Exception:
        try:
            sys.stderr.write(line + "\n")
            sys.stderr.flush()
        except Exception:
            pass

# --- Sélecteurs et interactions UI (identiques) ---
async def _focus_input(page: Page, *, logger=None, timeout_ms: int = 4000) -> bool:
    # ... (code _focus_input identique à gemini_connector.py) ...
    candidates = [
        'div[role="textbox"][aria-label*="Gemini"]', 'textarea[data-testid="chat-input"]',
        "textarea[aria-label]", "textarea", "[contenteditable='true'][role='textbox']",
        "[contenteditable='true']", "div[role='textbox']", "form textarea", "main textarea",
    ]
    t0 = time.monotonic()
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=max(100, timeout_ms // len(candidates)))
            await loc.click(timeout=1000)
            _jlog(logger, "input_focus", ok=True, selector=sel)
            return True
        except Exception: pass
        if (time.monotonic() - t0) * 1000 > timeout_ms: _jlog(logger, "input_focus_timeout_selectors", timeout_ms=timeout_ms); break
    _jlog(logger, "input_focus_using_fallback_tab")
    try:
        await page.locator("body").click(timeout=1000); await page.keyboard.press("Tab")
        _jlog(logger, "input_focus", ok=True, selector="fallback_tab"); return True
    except Exception as e: _jlog(logger, "input_focus", ok=False, error=str(e), selector="fallback_tab"); return False

async def _type_prompt(page: Page, prompt: str, *, method: str = "type", logger=None) -> bool:
    # ... (code _type_prompt identique à gemini_connector.py) ...
    if not prompt: _jlog(logger, "input_type_skipped_empty_prompt"); return True
    t0 = time.monotonic()
    try:
        if method == "fill":
            target_locator = page.locator(':focus, textarea:visible, [contenteditable="true"]:visible').first
            await target_locator.fill(prompt, timeout=5000)
            _jlog(logger, "input_type", ok=True, method="fill", nchar=len(prompt), ms=int((time.monotonic()-t0)*1000))
        else:
            await page.keyboard.type(prompt, delay=10)
            _jlog(logger, "input_type", ok=True, method="type", nchar=len(prompt), ms=int((time.monotonic()-t0)*1000))
        return True
    except Exception as e: _jlog(logger, "input_type", ok=False, method=method, error=str(e), error_type=type(e).__name__); return False

async def _submit(page: Page, *, via: str = "enter", logger=None) -> bool:
    # ... (code _submit identique à gemini_connector.py) ...
    t0 = time.monotonic()
    primary_method = via == "enter"
    try:
        if primary_method: await page.keyboard.press("Enter")
        else: await page.locator("button[type='submit'], button[aria-label*='send' i], button[data-testid*='send']").first.click(timeout=5000)
        _jlog(logger, "input_submit_attempt", ok=True, via=via, ms=int((time.monotonic()-t0)*1000))
        await page.wait_for_timeout(300); return True
    except Exception as e: _jlog(logger, "input_submit_attempt", ok=False, via=via, error=str(e), error_type=type(e).__name__); return False

# --- GeminiConnector Classe (identique, mais avec ask_with_file modifié) ---
class GeminiConnector:
    # ... (__init__, __aenter__, __aexit__, open, close, ask identiques) ...
    def __init__( self, *, logger: Any | None = None, user_id: Optional[str] = None, profile_root: Optional[str] = None, headless: Optional[bool] = None, network_debug: Optional[bool] = None, login_timeout_s: Optional[int] = None, cdp_url: Optional[str] = None, **kwargs,) -> None:
        self.logger = logger; self.user_id = user_id; self.profile_root = profile_root; self.headless = headless; self.network_debug = bool(network_debug) if network_debug is not None else False; self.login_timeout_s = login_timeout_s; self.cdp_url = cdp_url
        self._cfg: Dict[str, Any] = {"user_id": self.user_id, "profile_root": self.profile_root, "headless": self.headless, "network_debug": self.network_debug, "cdp_url": self.cdp_url, **kwargs,}
        self.browser = None; self.context = None; self.page: Optional[Page] = None; self.epoch: Optional[int] = None; self.hook_queue: Optional[asyncio.Queue] = None; self.sniffer = None; self.awaiter = None; self._opened = False
    async def __aenter__(self) -> "GeminiConnector": await self.open(); return self
    async def __aexit__(self, exc_type, exc, tb) -> None: await self.close()
    async def open(self) -> None:
        prep = await prepare_page(self._cfg, logger=self.logger, network_debug=self.network_debug)
        self.browser = prep.get("browser"); self.context = prep.get("context"); self.page = prep.get("page"); self.epoch = prep.get("epoch"); self.hook_queue = prep.get("hook_queue")
        if GeminiNetworkTap is None: raise RuntimeError("GeminiNetworkTap not available")
        self.sniffer = GeminiNetworkTap(self.page, logger=self.logger); await self.sniffer.start()
        awaiter_kwargs = {"anti_dom_window_ms": int(float(os.getenv("ANTI_DOM_WINDOW_S", "2.0")) * 1000), "hard_timeout_ms": int(os.getenv("ANSWER_HARD_TIMEOUT_MS", "35000")), "be_max_coalesce_bytes": int(os.getenv("BE_MAX_COALESCE_BYTES", "131072")),}
        self.awaiter = await build_awaiter(self.page, sniffer=self.sniffer, logger=self.logger, hook_queue=self.hook_queue, **awaiter_kwargs)
        _jlog(self.logger, "connector_ready", ok=True, headless=bool(self.headless), cdp=True, epoch=self.epoch); self._opened = True
    async def close(self) -> None:
        stop_tasks = []
        if self.awaiter and hasattr(self.awaiter, "stop"): stop_tasks.append(asyncio.create_task(self.awaiter.stop(), name="stop_awaiter"))
        if self.sniffer and hasattr(self.sniffer, "stop"): stop_tasks.append(asyncio.create_task(self.sniffer.stop(), name="stop_sniffer"))
        if stop_tasks:
            try: await asyncio.wait_for(asyncio.gather(*stop_tasks, return_exceptions=True), timeout=5.0); _jlog(self.logger, "connector_closed_components_stopped")
            except asyncio.TimeoutError: _jlog(self.logger, "connector_close_timeout", level="WARN")
            except Exception as e: _jlog(self.logger, "connector_close_error", error=str(e), level="WARN")
        self._opened = False; _jlog(self.logger, "connector_closed")
    async def ask(self, prompt: str) -> Tuple[str, Dict[str, Any]]:
        if not self._opened: _jlog(self.logger, "connector_auto_opening_for_ask"); await self.open()
        if not self.page or self.page.is_closed(): raise RuntimeError("Page is not available or closed.")
        if not self.awaiter: raise RuntimeError("Awaiter not initialized.")
        focus_ok = await _focus_input(self.page, logger=self.logger);
        if not focus_ok: _jlog(self.logger, "ask_focus_failed_continuing", level="WARN")
        type_ok = await _type_prompt(self.page, prompt, method="type", logger=self.logger)
        if not type_ok: _jlog(self.logger, "ask_type_failed_aborting", level="ERROR"); return "", {"src": "error", "error": "typing_failed"}
        submit_ok = await _submit(self.page, via="enter", logger=self.logger)
        if not submit_ok: _jlog(self.logger, "ask_submit_failed_aborting", level="ERROR"); return "", {"src": "error", "error": "submit_failed"}
        t0_ms = int(time.monotonic() * 1000)
        awaiter_kwargs = {"anti_dom_window_ms": int(float(os.getenv("ANTI_DOM_WINDOW_S", "2.0")) * 1000), "hard_timeout_ms": int(os.getenv("ANSWER_HARD_TIMEOUT_MS", "35000")), "be_max_coalesce_bytes": int(os.getenv("BE_MAX_COALESCE_BYTES", "131072")),}
        try: ans = await await_answer(self.awaiter, t0_ms=t0_ms, logger=self.logger, **awaiter_kwargs)
        except Exception as await_err: _jlog(self.logger, "await_answer_exception", error=str(await_err), error_type=type(await_err).__name__, level="ERROR"); return "", {"src": "error", "error": "await_answer_failed", "details": str(await_err)}
        src = ans.get("src", "unknown"); text = ans.get("text") or ""
        meta = {"src": src, "t0_ms": t0_ms, "t1_ms": int(time.monotonic() * 1000), "stats": getattr(self.awaiter, "stats", lambda: {})(), **(ans.get("meta", {}))}
        _jlog(self.logger, "ask_completed", src=src, len=len(text), meta_keys=list(meta.keys())); return text, meta


    async def ask_with_file(self, prompt: str, file_path: str) -> Tuple[str, Dict[str, Any]]:
        """
        (Simplifié) Appelle `ask` en supposant que le fichier a déjà été téléversé
        par un mécanisme externe (comme collect_cli.py).
        Le timeout pour `await_answer` est augmenté dans cette méthode.
        """
        if not self._opened:
            _jlog(self.logger, "connector_auto_opening_for_ask_with_file")
            await self.open()
        if not self.page or self.page.is_closed():
             raise RuntimeError("Page is not available or closed.")
        if not self.awaiter:
             raise RuntimeError("Awaiter not initialized.")

        # --- Supprimer la logique JS d'upload ---
        _jlog(self.logger, "ask_with_file_called", file=file_path, prompt_len=len(prompt), note="Assuming file already uploaded by CLI")

        # --- Étapes de focus, type, et submit (similaires à la méthode ask) ---
        focus_ok = await _focus_input(self.page, logger=self.logger)
        if not focus_ok: _jlog(self.logger, "ask_with_file_focus_failed", level="WARN")

        type_ok = await _type_prompt(self.page, prompt, method="type", logger=self.logger)
        if not type_ok:
            _jlog(self.logger, "ask_with_file_type_failed", level="ERROR")
            return "", {"src": "error", "error": "typing_failed"}

        submit_ok = await _submit(self.page, via="enter", logger=self.logger)
        if not submit_ok:
            _jlog(self.logger, "ask_with_file_submit_failed", level="ERROR")
            return "", {"src": "error", "error": "submit_failed"}

        # --- Attente de la réponse avec timeout augmenté ---
        t0_ms = int(time.monotonic() * 1000)
        awaiter_kwargs_file = {
            "anti_dom_window_ms": int(float(os.getenv("ANTI_DOM_WINDOW_S", "2.0")) * 1000),
            "hard_timeout_ms": 180000, # 3 minutes pour traitement fichier
            "be_max_coalesce_bytes": int(os.getenv("BE_MAX_COALESCE_BYTES", "131072")),
        }
        _jlog(self.logger, "ask_with_file_calling_await_answer", hard_timeout_ms=awaiter_kwargs_file["hard_timeout_ms"])
        try:
            ans = await await_answer(
                self.awaiter,
                t0_ms=t0_ms,
                logger=self.logger,
                **awaiter_kwargs_file
            )
        except Exception as await_err:
             _jlog(self.logger, "await_answer_exception_file", error=str(await_err), error_type=type(await_err).__name__, level="ERROR")
             return "", {"src": "error", "error": "await_answer_failed_file", "details": str(await_err)}

        src = ans.get("src", "unknown")
        text = ans.get("text") or ""
        meta = {
            "src": src,
            "t0_ms": t0_ms,
            "t1_ms": int(time.monotonic() * 1000),
            "stats": getattr(self.awaiter, "stats", lambda: {})(),
             **(ans.get("meta", {}))
        }
        _jlog(self.logger, "ask_with_file_completed", src=src, len=len(text), meta_keys=list(meta.keys()))
        return text, meta


    async def run_once(self, context: Any, *, prompt: str, network_debug: bool = False, t0_ms: Optional[int] = None) -> Dict[str, Any]:
        _ = context; _ = t0_ms; self.network_debug = bool(network_debug)
        txt, meta = await self.ask(prompt); return {"text": txt, "meta": meta}

    async def ask_text(self, prompt: str) -> str:
        txt, _meta = await self.ask(prompt); return txt

__all__ = ["GeminiConnector"]