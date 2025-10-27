# gemini_headless/collect/orchestrator.py
# VERSION 7.11 (Stagnation V1)
# - CORRIGÉ: Importation de `clean_text_and_extract_json` remplacée par `clean_text_with_stats`.
# - AJOUTÉ: Logique d'extraction JSON (avec marqueur <<END>>) après l'appel à `clean_text_with_stats`.
# - AJOUTÉ (STRATÉGIE ROBUSTE): Suivi de `_last_real_progress_ts` (network/DOM output).
# - AJOUTÉ (STRATÉGIE ROBUSTE): Détection de Stagnation dans `_seen_guard_loop` pour forcer un snapshot.
from __future__ import annotations
import asyncio, time, os, inspect, traceback, re, json # Ajout re, json
from typing import Dict, Optional, Tuple, Any, Callable, List, Set
from playwright.async_api import Page, Error as PlaywrightError

_jlog_defined = False
try:
    from .utils.logs import jlog
    _jlog_defined = True
except ImportError:
    # Définition du fallback jlog corrigée
    try:
        from utils.logs import jlog # type: ignore
        _jlog_defined = True
    except ImportError:
        import sys, json, time
        def jlog(*_a, evt="unknown", **_k):
            payload = {"evt": evt, "ts": time.time(), **_k}
            try:
                # Correction: Appel print() valide
                print(json.dumps(payload, ensure_ascii=False, separators=(",",":")), file=sys.stderr)
                sys.stderr.flush()
            except Exception:
                # Correction: Appel print() valide pour l'erreur fallback
                print(json.dumps({"evt": "jlog_fallback_error", "original_evt": evt}), file=sys.stderr)
                sys.stderr.flush()

# --- Placeholder & Imports (MODIFIÉ: Import cleaner corrigé) ---
class BaseProducerPlaceholder:
    def __init__(self, *args, **kwargs): jlog(f"{self.__class__.__name__}_placeholder_used", reason="Import failed"); self.seen = False; self.done = False
    async def start(self): pass
    async def stop(self): pass
    async def snapshot_now(self) -> str: return ""

_producers_imported = False
try:
    from .producers.sse import SSEProducer
    from .producers.ws import WSProducer
    from .producers.be import BEProducer
    from .producers.dom import DOMProducer, _GET_BEST_TEXT_JS # Garde _GET_BEST_TEXT_JS pour snapshot
    # MODIFIÉ: Importer les fonctions existantes du cleaner
    from .filters.cleaner import clean_text_with_stats, clean_text
    _producers_imported = True
    jlog("producer_imports_successful_v7.11_corrected") # Version log corrigée
except ImportError as import_err:
    err_type = type(import_err).__name__; err_msg = str(import_err); tb_str = traceback.format_exc(); jlog("orchestrator_import_error", error_type=err_type, error_message=err_msg, traceback=tb_str, level="ERROR")
    # Définitions des classes fallback sur des lignes séparées (Corrigé)
    class SSEProducer(BaseProducerPlaceholder): pass
    class WSProducer(BaseProducerPlaceholder): pass
    class BEProducer(BaseProducerPlaceholder): pass
    class DOMProducer(BaseProducerPlaceholder): pass
    _GET_BEST_TEXT_JS = "() => ''"
    # Fallback pour les fonctions cleaner si l'import initial échoue quand même
    def clean_text_with_stats(x: str, *, src: str = "unknown", ui_markup: bool = False) -> Tuple[str, Dict[str, Any]]: jlog("cleaner_fallback_used", level="WARN"); return (x or "").strip(), {}
    def clean_text(x: str, *, src: str = "unknown", ui_markup: bool = False) -> str: return (x or "").strip()


if not _producers_imported:
     jlog("warning_running_with_placeholder_producers_v7.11_corrected", level="ERROR") # Version log corrigée

# --- Constants and Config (inchangés) ---
PRIO = ["sse", "ws", "be", "dom"]
def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default


# --- JavaScript Observer V7.10 (inchangés) ---
_DOM_OBSERVER_SCRIPT_V7_10_PRINCIPLE = r"""
async (pythonCallbackName, stabilityTimeoutMs = 750) => {
  // ... (contenu JS identique à celui fourni précédemment) ...
}
"""

class Orchestrator:
    # --- __init__ (MODIFIÉ: Ajout de _last_real_progress_ts et stagnation_timeout_ms) ---
    def __init__(self, page: Page, *, fast_path_strict: bool = True, max_bytes: int = 262_144,
                 seen_guard_ms: Optional[int] = None, stagnation_timeout_ms: int = 120000, **_: object) -> None: # AJOUT
        self.page = page; self.fast_path_strict = fast_path_strict; self.max_bytes = max_bytes
        if seen_guard_ms is None: seen_guard_ms = _env_int("GH_STRICT_PLUS_MS", 10000)
        self.seen_guard_ms = max(0, int(seen_guard_ms)); self.dom_guard_window_ms = _env_int("GH_DOM_GUARD_WINDOW_MS", 5000)
        
        # NOUVEAU: Timeout pour détecter une stagnation après le début du progrès
        self.stagnation_timeout_ms = max(0, int(stagnation_timeout_ms)) 
        self._last_real_progress_ts: Optional[float] = None # NOUVEAU: Marqueur de progrès réel (output)

        self._allow_dom_after_seen_guard = False; self._first_network_seen_ts: Optional[float] = None; self._seen_guard_task: Optional[asyncio.Task] = None
        self.sse = SSEProducer(page, self._on_progress("sse"), self._on_done); self.ws = WSProducer(page, self._on_progress("ws"), self._on_done); self.be = BEProducer(page, self._on_progress("be"), self._on_done); self.dom = DOMProducer(page, self._on_progress("dom"), self._on_done)
        self._buf: Dict[str, str] = {src: "" for src in PRIO}; self._last_dom_seen: str = ""; self._emit_text: Optional[str] = None; self._emit_meta: Dict[str, object] = {}; self._done_evt = asyncio.Event(); self._final_cleaner_stats: Dict[str, Any] = {}; self._fallback_called = False; self._dom_callback_name = f"__gh_dom_callback_{int(time.time()*1000)}"; self._dom_observer_installed = False


    # --- _on_progress (MODIFIÉ: Ajout de la mise à jour de _last_real_progress_ts) ---
    def _on_progress(self, src: str):
        def _cb(chunk: str, _src: Optional[str] = None) -> None:
            s = src if _src is None else _src
            if not chunk or self._done_evt.is_set(): return

            is_network_src = s in ("sse", "ws", "be")
            
            # NOUVEAU: Mettre à jour le timestamp du dernier progrès réel
            if chunk and (is_network_src or s == "dom"):
                 self._last_real_progress_ts = time.monotonic()
                 if self._first_network_seen_ts is None:
                     jlog("orchestrator_first_real_progress", src=s, ts=self._last_real_progress_ts) # Log de progrès réel initial (événement utilisé pour la phase de supervision)

            if is_network_src and self._first_network_seen_ts is None:
                self._first_network_seen_ts = time.monotonic()
                jlog("network_seen", src=s, ts=self._first_network_seen_ts)
                if self.seen_guard_ms > 0 and self._seen_guard_task is None:
                    try:
                        loop = asyncio.get_running_loop()
                        self._seen_guard_task = loop.create_task(self._seen_guard_loop(), name="seen_guard")
                        jlog("seen_guard_task_started_on_network", guard_ms=self.seen_guard_ms)
                    except RuntimeError: jlog("error_creating_seen_guard_task_no_loop", level="ERROR")
                    except Exception as task_err: jlog("error_creating_seen_guard_task", error=str(task_err), level="ERROR")

            if s in self._buf and s != "dom":
                current_len = len(self._buf.get(s, ""))
                chunk_len = len(chunk)
                if current_len < self.max_bytes:
                    new_len = min(self.max_bytes, current_len + chunk_len)
                    prefix = " " if current_len > 0 and not self._buf.get(s,"").endswith(("\n", " ")) else ""
                    safe_chunk = chunk if isinstance(chunk, str) else str(chunk)
                    self._buf[s] = (self._buf.get(s, "") + prefix + safe_chunk)[:new_len]
                    if new_len == self.max_bytes and current_len < self.max_bytes:
                        jlog("buffer_limit_reached", src=s, max_bytes=self.max_bytes, level="WARN")

            try:
                prod = getattr(self, s, None)
                if prod and hasattr(prod, 'seen'): prod.seen = True
            except Exception: pass
        return _cb


    # --- Callback DOM _on_dom_stable_ready (MODIFIÉ: Ajout de la mise à jour de _last_real_progress_ts) ---
    async def _on_dom_stable_ready(self, payload: Dict[str, Any]):
        jlog("dom_callback_received", status=payload.get("status"), reason=payload.get("reason"), snapshot_status=payload.get("snapshot_status"), level="INFO")
        if self._done_evt.is_set(): return
        status = payload.get("status"); reason = payload.get("reason", "unknown_js_reason"); snapshot = payload.get("snapshot")
        if status == "error_detected":
            jlog("orchestrator_dom_error_detected", js_reason=reason, snapshot_len=len(snapshot or ""), snapshot_head=(snapshot or "")[:60])
            self._last_dom_seen = (snapshot or "").strip()
            self._emit_meta['invalid_response'] = True; self._emit_meta['invalid_reason'] = reason
            self._on_done("dom", final_text=self._last_dom_seen, strong=True, ui_markup=True)
        elif status == "ready" and isinstance(snapshot, str):
            snapshot_strip = snapshot.strip()
            if snapshot_strip:
                self._last_dom_seen = snapshot_strip
                # NOUVEAU: Marquer le progrès réel
                self._last_real_progress_ts = time.monotonic()
                jlog("dom_stable_ready_snapshot", len=len(snapshot_strip), head=snapshot_strip[:60], js_reason=reason)
                self._on_done("dom", final_text=snapshot_strip, strong=False, ui_markup=True)
            else: jlog("dom_stable_ready_empty_snapshot", js_reason=reason, snapshot_status=payload.get("snapshot_status"), level="WARN")
        elif status == "js_error": jlog("dom_callback_js_error", js_reason=reason, error_details=payload.get("error"), level="ERROR")
        elif status == "not_ready": jlog("dom_stable_not_ready", js_reason=reason, level="DEBUG")
        else: jlog("dom_callback_invalid_payload", payload=str(payload)[:200], level="WARN")


    # --- _on_done (MODIFIÉ: Utilise clean_text_with_stats + extraction JSON) ---
    def _on_done(self, src: str, final_text: Optional[str] = None, *, strong: bool = True, ui_markup: bool = False) -> None:
        # ... (Logique identique à la correction précédente pour l'extraction JSON et la décision) ...
        if self._done_evt.is_set(): return
        try: prod = getattr(self, src, None);
        except Exception: prod = None
        if prod and hasattr(prod, 'done') and not prod.done: prod.done = True; jlog("producer_marked_done", src=src)

        # Logique garde DOM (inchangée)
        if src == "dom" and self.fast_path_strict:
            network_seen = self._first_network_seen_ts is not None
            if network_seen and not self._allow_dom_after_seen_guard:
                guard_elapsed_ms = (time.monotonic() - (self._first_network_seen_ts or time.monotonic())) * 1000
                remaining_guard_ms = max(0, self.seen_guard_ms - guard_elapsed_ms)
                jlog("dom_blocked_by_guard", src=src, remaining_ms=int(remaining_guard_ms))
                return

        # Déterminer le texte brut à analyser
        if src == "dom": text_raw = final_text if final_text is not None else self._last_dom_seen;
        else: text_raw = final_text if final_text is not None else self._buf.get(src, "")
        text_raw = (text_raw or "").strip()

        # MODIFIÉ: Appeler le cleaner existant et extraire le JSON ensuite
        extracted_json: Optional[Dict[str, str]] = None
        cleaned_text: str = ""; cleaner_stats: Dict[str, Any] = {}
        if text_raw:
            try:
                use_ui_markup_final = (src == "dom")
                cleaned_text, cleaner_stats = clean_text_with_stats(
                    text_raw, src=src, ui_markup=use_ui_markup_final
                )
                self._final_cleaner_stats = cleaner_stats or {}

                # AJOUTÉ: Logique d'extraction JSON post-nettoyage
                if cleaned_text.endswith("<<END>>"):
                    json_sentinel_regex = re.search(r"({.*})<<END>>\s*$", cleaned_text, re.DOTALL)
                    if json_sentinel_regex:
                        try:
                            extracted_json = json.loads(json_sentinel_regex.group(1))
                            jlog("on_done_extracted_json_from_cleaned", trigger_src=src, json_keys=list(extracted_json.keys()))
                        except json.JSONDecodeError:
                            jlog("on_done_invalid_json_in_sentinel", trigger_src=src, text_snippet=cleaned_text[-100:], level="WARN")
                            extracted_json = None 

                jlog("on_done_cleaning_complete_v7.10_corrected", trigger_src=src, raw_len=len(text_raw), cleaned_len=len(cleaned_text), json_extracted=bool(extracted_json), stats_keys=list(self._final_cleaner_stats.keys()))

            except Exception as clean_err:
                 jlog("on_done_cleaner_error_v7.10_corrected", trigger_src=src, error=str(clean_err), error_type=type(clean_err).__name__, level="ERROR"); cleaned_text = text_raw # Fallback texte brut
                 self._final_cleaner_stats = {"cleaner_error": str(clean_err)}
                 extracted_json = None 
        else:
             jlog("on_done_raw_text_empty", trigger_src=src, level="DEBUG")

        # MODIFIÉ: Logique "First Win" basée sur JSON valide extrait
        if extracted_json:
            jlog("on_done_json_sentinel_winner", winner=src, trigger_src=src)
            final_output_text = json.dumps(extracted_json, ensure_ascii=False, separators=(",",":")) + "<<END>>" # Rajouter le marqueur attendu
            self._emit(final_output_text, src=f"{src}_json") # Marquer la source
            return 

        # Sinon (pas de JSON trouvé), continuer avec l'ancienne logique _choose_winner basée sur le texte nettoyé
        if strong or src == "dom":
            winner = self._choose_winner_legacy(src, cleaned_text) # Utilise l'ancien choose_winner avec le texte nettoyé
            if winner is None: return 

            jlog("on_done_legacy_winner_selected", winner=winner, trigger_src=src, cleaned_len=len(cleaned_text))
            if cleaned_text or self._emit_meta.get("invalid_response"):
                self._emit(cleaned_text, src=winner)
            else:
                jlog("on_done_legacy_winner_text_empty", winner=winner, trigger_src=src, level="WARN")

    # --- _choose_winner_legacy (inchangé) ---
    def _choose_winner_legacy(self, trigger_src: str, current_cleaned_text: str) -> Optional[str]:
        if current_cleaned_text.strip(): jlog("choose_winner_legacy_trigger_has_content", winner=trigger_src); return trigger_src
        for s in PRIO:
             prod = getattr(self, s, None);
             if prod and getattr(prod, "done", False):
                  if s == "dom" and self.fast_path_strict:
                     network_seen = self._first_network_seen_ts is not None
                     if network_seen and not self._allow_dom_after_seen_guard: continue
                  content_check = self._last_dom_seen if s == "dom" else self._buf.get(s, "")
                  if (content_check or "").strip() or (s == "dom" and self._emit_meta.get("invalid_response")): jlog("choose_winner_legacy_found_alternative", winner=s, trigger=trigger_src); return s
                  else: jlog("choose_winner_legacy_skipping_empty", candidate=s, trigger=trigger_src)
        return None

    # --- run_fast_path (MODIFIÉ: log version V7.11) ---
    async def run_fast_path(self, *, start_dom: bool = True) -> Tuple[str, Dict[str, object]]:
        t0 = time.perf_counter(); t0_mono = time.monotonic()
        jlog("orchestrator_run_fast_path_start_v7.11_corrected", ts=t0, start_dom=start_dom, strategy="MutationObserver_v7.10_Adaptative") # Version V7.11
        self._emit_meta = {"t0_ms": int(t0 * 1000)}; self._emit_text = None; self._fallback_called = False; tasks = []
        producers_to_start = {"sse": self.sse, "ws": self.ws, "be": self.be}
        dom_observer_ok = False
        if start_dom and not isinstance(self.dom, BaseProducerPlaceholder):
            try:
                jlog("exposing_dom_callback", name=self._dom_callback_name)
                await self.page.expose_function(self._dom_callback_name, self._on_dom_stable_ready)
                jlog("injecting_snapshot_helper_from_dom_py_v7.10")
                await self.page.add_init_script(f"window.__gh_get_best_text = {_GET_BEST_TEXT_JS}")
                jlog("injecting_dom_observer_script_v7.10")
                observer_script_content = _DOM_OBSERVER_SCRIPT_V7_10_PRINCIPLE
                await self.page.evaluate(f"({observer_script_content})('{self._dom_callback_name}')")
                self._dom_observer_installed = True; dom_observer_ok = True
                jlog("dom_observer_install_success_v7.10")
            except Exception as dom_init_err: jlog("dom_observer_install_failed_v7.10", error=str(dom_init_err).split('\n')[0], error_type=type(dom_init_err).__name__, level="ERROR")
        else: jlog("dom_observer_skipped", reason="start_dom_false" if not start_dom else "placeholder_producer")
        for name, prod in producers_to_start.items():
            if not isinstance(prod, BaseProducerPlaceholder):
                start_method = getattr(prod, "start", None)
                if start_method and inspect.iscoroutinefunction(start_method): tasks.append(asyncio.create_task(start_method(), name=f"start_{name}")); jlog("producer_start_task_created", name=name)
                elif start_method: jlog("producer_start_method_not_coroutine", name=name, level="WARN")
            else: jlog("producer_skipped_start", name=name, reason="is_placeholder_instance", level="WARN")
        wait_succeeded = False
        try: jlog("orchestrator_wait_start_adaptative"); await self._done_evt.wait(); wait_succeeded = True; jlog("orchestrator_wait_done_event_received")
        except Exception as wait_err: jlog("orchestrator_wait_error", error=str(wait_err), error_type=type(wait_err).__name__, level="ERROR")
        finally:
            jlog("orchestrator_cleanup_start", wait_succeeded=wait_succeeded, emit_text_present=self._emit_text is not None, invalid_response=self._emit_meta.get('invalid_response', False))
            if self._emit_text is None and not self._fallback_called: await self._emit_best_snapshot_fallback(reason="final_fallback_check_in_finally") 
            await self._stop_all()
            cleanup_tasks = [("seen_guard", self._seen_guard_task)]
            for task_name, task_instance in cleanup_tasks:
                try:
                    if task_instance and not task_instance.done(): jlog("cancelling_aux_task", name=task_name); task_instance.cancel(); await asyncio.wait_for(task_instance, timeout=0.5)
                except asyncio.CancelledError: pass
                except asyncio.TimeoutError: jlog("cleanup_task_cancel_timeout", name=task_name, level="WARN")
                except Exception as e: jlog("cleanup_task_cancel_error", name=task_name, error=str(e), level="WARN")
            if tasks:
                jlog("waiting_for_producer_start_tasks", count=len(tasks))
                try: await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=1.0)
                except asyncio.TimeoutError: jlog("start_tasks_gather_timeout", level="WARN")
                except Exception as gather_err: jlog("start_tasks_gather_error", error=str(gather_err), level="WARN")
            jlog("orchestrator_cleanup_done")
        t1 = time.perf_counter(); t1_mono = time.monotonic(); total_ms = int((t1_mono - t0_mono) * 1000)
        self._emit_meta["t1_ms"] = int(t1 * 1000); self._emit_meta["total_ms"] = total_ms
        if self._final_cleaner_stats:
            self._emit_meta["cleaner_stats"] = self._final_cleaner_stats; initial = self._final_cleaner_stats.get('initial_char_count', 0); final = self._final_cleaner_stats.get('final_char_count', 0)
            if initial > 0: removed = max(0, initial - final); self._emit_meta["dom_dup_ratio"] = round(removed / initial, 4)
            else: self._emit_meta["dom_dup_ratio"] = 0.0
            self._emit_meta["repaired_initial_chars"] = self._final_cleaner_stats.get("repaired_initial_chars", 0)
        final_text_result = self._emit_text if self._emit_text is not None else ""
        jlog("orchestrator_run_fast_path_end", final_len=len(final_text_result), source=self._emit_meta.get("source_chosen", "unknown"), total_ms=total_ms, wait_succeeded=wait_succeeded, invalid_response=self._emit_meta.get('invalid_response', False))
        return final_text_result, self._emit_meta

    # --- _emit_best_snapshot_fallback (MODIFIÉ: log version V7.11) ---
    async def _emit_best_snapshot_fallback(self, reason="unknown") -> None:
        # ... (Logique identique à la correction précédente) ...
        if self._done_evt.is_set() or self._fallback_called: return
        self._fallback_called = True
        jlog("emit_fallback_snapshot_start_v7.11_corrected", reason=reason) # Log version V7.11
        final_json: Optional[Dict[str, str]] = None; final_text = ""; src = "unknown_fallback"; snapshot_attempted = False
        selected_src_buffer: Optional[str] = None; buffered_text: str = ""
        dom_snap_text: str = ""

        # 1. Essayer snapshot DOM direct via JS (robuste)
        if not isinstance(self.dom, BaseProducerPlaceholder):
             snapshot_attempted = True
             try:
                 jlog("fallback_attempting_dom_snapshot_v7.10", reason=reason)
                 snap_js_expr = f"({_GET_BEST_TEXT_JS})()" 
                 snap = await asyncio.wait_for(self.page.evaluate(snap_js_expr), timeout=7.0)
                 if isinstance(snap, str) and snap.strip():
                     dom_snap_text = snap.strip()
                     jlog("fallback_snapshot_success_v7.10", src="dom_snapshot_fallback", len=len(dom_snap_text))
                     text_snap_cleaned, stats_snap = clean_text_with_stats(dom_snap_text, src="dom_snapshot_fallback", ui_markup=True)
                     json_snap = None
                     if text_snap_cleaned.endswith("<<END>>"):
                         json_sentinel_regex = re.search(r"({.*})<<END>>\s*$", text_snap_cleaned, re.DOTALL)
                         if json_sentinel_regex:
                             try:
                                 json_snap = json.loads(json_sentinel_regex.group(1))
                                 jlog("fallback_extracted_json_from_dom_snap", json_keys=list(json_snap.keys()))
                             except json.JSONDecodeError: json_snap = None 
                     if json_snap: final_json = json_snap; src = "dom_snapshot_json_fallback"
                     elif text_snap_cleaned: final_text = text_snap_cleaned; src = "dom_snapshot_text_fallback"
                     self._final_cleaner_stats = stats_snap or {}
                 else: jlog("fallback_snapshot_empty_v7.10")
             except asyncio.TimeoutError: jlog("fallback_snapshot_timeout_v7.10", level="WARN")
             except asyncio.CancelledError: jlog("fallback_snapshot_cancelled_v7.10")
             except Exception as e: jlog("fallback_snapshot_error_v7.10", error=str(e), error_type=type(e).__name__, level="ERROR")
        else: jlog("fallback_snapshot_skipped_placeholder_v7.10", level="WARN")

        # 2. Si snapshot DOM n'a pas donné de JSON, essayer les buffers réseau
        if not final_json:
            selected_src_buffer, buffered_text = self._best_snapshot() 
            if buffered_text:
                jlog("fallback_using_buffer_v7.10", src=selected_src_buffer, len=len(buffered_text))
                text_buf_cleaned, stats_buf = clean_text_with_stats(buffered_text, src=f"{selected_src_buffer}_buffer_fallback", ui_markup=False)
                json_buf = None
                if text_buf_cleaned.endswith("<<END>>"):
                    json_sentinel_regex = re.search(r"({.*})<<END>>\s*$", text_buf_cleaned, re.DOTALL)
                    if json_sentinel_regex:
                        try:
                            json_buf = json.loads(json_sentinel_regex.group(1))
                            jlog("fallback_extracted_json_from_buffer", src=selected_src_buffer, json_keys=list(json_buf.keys()))
                        except json.JSONDecodeError: json_buf = None 
                if json_buf: final_json = json_buf; src = f"{selected_src_buffer}_buffer_json_fallback"
                elif text_buf_cleaned: final_text = text_buf_cleaned; src = f"{selected_src_buffer}_buffer_text_fallback"
                self._final_cleaner_stats = stats_buf or {}
            else:
                 jlog("fallback_no_valid_buffer_found_v7.10", level="WARN")

        # 3. Dernier recours: _last_dom_seen 
        if not final_json and not final_text:
            last_dom_content = (self._last_dom_seen or "").strip()
            if last_dom_content:
                jlog("fallback_using_last_dom_seen_v7.10", len=len(last_dom_content))
                text_last_cleaned, stats_last = clean_text_with_stats(last_dom_content, src="dom_last_seen_fallback", ui_markup=True)
                json_last = None
                if text_last_cleaned.endswith("<<END>>"):
                    json_sentinel_regex = re.search(r"({.*})<<END>>\s*$", text_last_cleaned, re.DOTALL)
                    if json_sentinel_regex:
                         try:
                             json_last = json.loads(json_sentinel_regex.group(1))
                             jlog("fallback_extracted_json_from_last_dom", json_keys=list(json_last.keys()))
                         except json.JSONDecodeError: json_last = None 
                if json_last: final_json = json_last; src = "dom_last_seen_json_fallback"
                elif text_last_cleaned: final_text = text_last_cleaned; src = "dom_last_seen_text_fallback"
                self._final_cleaner_stats = stats_last or {}
            else:
                jlog("fallback_no_text_found_all_sources_v7.10", level="WARN");
                final_text = ""; src = "empty_fallback"

        # Émission finale
        if not self._done_evt.is_set():
            output_to_emit = ""
            if final_json:
                output_to_emit = json.dumps(final_json, ensure_ascii=False, separators=(",",":")) + "<<END>>" 
                jlog("fallback_emitting_json", src=src, len=len(output_to_emit))
            elif final_text:
                output_to_emit = final_text
                jlog("fallback_emitting_cleaned_text", src=src, len=len(output_to_emit))
            else:
                jlog("fallback_emitting_empty", src=src)

            self._emit(output_to_emit, src=src)


    # --- _seen_guard_loop (MODIFIÉ: Ajout de la logique de Stagnation) ---
    async def _seen_guard_loop(self) -> None:
        try:
            # AJOUT: Log stagnation
            jlog("seen_guard_loop_started_v7.11_corrected", guard_ms=self.seen_guard_ms, stagnation_ms=self.stagnation_timeout_ms)
            while not self._done_evt.is_set():
                await asyncio.sleep(5.0) # Vérifier toutes les 5s

                if self._done_evt.is_set(): break
                now_mono = time.monotonic()
                
                # --- NOUVEAU: Logique Stagnation ---
                # Vérifier si un progrès a été vu, et si cela fait trop longtemps
                if self._last_real_progress_ts is not None and self.stagnation_timeout_ms > 0:
                    elapsed_since_last_progress = (now_mono - self._last_real_progress_ts) * 1000.0
                    
                    if elapsed_since_last_progress > self.stagnation_timeout_ms:
                        # Si le heartbeat a été étendu par la sonde (activity_probe) mais que le progrès réel stagne
                        is_any_producer_done = any(getattr(getattr(self, s, None), "done", False) for s in PRIO)
                        if not is_any_producer_done and not self._fallback_called:
                            jlog("orchestrator_stagnation_detected", elapsed_ms=int(elapsed_since_last_progress), threshold_ms=self.stagnation_timeout_ms, level="WARN")
                            # Déclencher le fallback car le processus est actif (via probe) mais silencieux (via réseau/DOM)
                            await self._emit_best_snapshot_fallback(reason="stagnation_timeout")
                            break # Sortir après avoir déclenché le fallback
                
                # --- Logique Seen Guard (inchangée) ---
                if self._allow_dom_after_seen_guard: continue # Si déjà passé, continuer la boucle
                if self._first_network_seen_ts is None: continue
                if any(getattr(getattr(self, s, None), "done", False) for s in ("sse", "ws", "be")): jlog("seen_guard_loop_exit_network_done"); return
                elapsed_ms = (now_mono - (self._first_network_seen_ts or now_mono)) * 1000.0
                if elapsed_ms >= float(self.seen_guard_ms):
                    jlog("seen_guard_tripped", elapsed_ms=int(elapsed_ms), configured_ms=self.seen_guard_ms)
                    self._allow_dom_after_seen_guard = True
                    if not hasattr(self, '_emit_meta'): self._emit_meta = {}
                    self._emit_meta["guard_wait_ms"] = int(elapsed_ms)

                    # Utiliser le texte DOM mis en cache (_last_dom_seen)
                    dom_seen_text_raw = (self._last_dom_seen or "").strip()
                    if dom_seen_text_raw:
                        jlog("seen_guard_checking_dom_cache", raw_len=len(dom_seen_text_raw))
                        text_cache_cleaned, stats_cache = clean_text_with_stats(dom_seen_text_raw, src="dom_cached_after_guard", ui_markup=True)
                        json_cache = None
                        if text_cache_cleaned.endswith("<<END>>"):
                            json_sentinel_regex = re.search(r"({.*})<<END>>\s*$", text_cache_cleaned, re.DOTALL)
                            if json_sentinel_regex:
                                try:
                                    json_cache = json.loads(json_sentinel_regex.group(1))
                                except json.JSONDecodeError: json_cache = None 

                        self._final_cleaner_stats = stats_cache or {}
                        final_output_text = ""; final_src = "unknown_guard_emit"

                        if json_cache:
                            final_output_text = json.dumps(json_cache, ensure_ascii=False, separators=(",",":")) + "<<END>>" 
                            final_src = "dom_cached_json_after_guard"
                            jlog("dom_emit_json_from_seen_cache_after_guard", got=len(dom_seen_text_raw), json_len=len(final_output_text))
                        elif text_cache_cleaned: 
                            final_output_text = text_cache_cleaned
                            final_src = "dom_cached_text_after_guard"
                            jlog("dom_emit_text_from_seen_cache_after_guard", got=len(dom_seen_text_raw), cleaned_len=len(final_output_text))
                        else:
                             jlog("dom_seen_cache_empty_after_cleaning_v7.10_corrected", raw_len=len(dom_seen_text_raw), level="WARN")

                        if final_output_text and not self._done_evt.is_set():
                            self._emit(final_output_text, src=final_src)
                            return 
                        elif not self._done_evt.is_set(): 
                             if not self._fallback_called: jlog("seen_guard_triggering_fallback_post_clean_v7.10_corrected"); await self._emit_best_snapshot_fallback(reason="seen_guard_dom_cleaned_empty")
                             return
                    else: 
                         if not self._done_evt.is_set() and not self._fallback_called: jlog("seen_guard_triggering_fallback_no_cache_v7.10_corrected"); await self._emit_best_snapshot_fallback(reason="seen_guard_no_cache")
                         return
        except asyncio.CancelledError: jlog("seen_guard_cancelled")
        except Exception as e: jlog("seen_guard_error", error=str(e), error_type=type(e).__name__, level="ERROR")
        finally: jlog("seen_guard_loop_finished")

    # --- _best_snapshot (inchangé) ---
    def _best_snapshot(self) -> Tuple[Optional[str], str]:
        jlog("attempting_best_snapshot_from_buffers_v7.10")
        for s in ("sse", "ws", "be"):
            prod = getattr(self, s, None); buffer_content = (self._buf.get(s) or "").strip()
            if prod and (getattr(prod, "done", False) or getattr(prod, "seen", False)) and buffer_content:
                status = "done" if getattr(prod, "done", False) else "seen"; jlog("best_snapshot_found", src=s, status=status, len=len(buffer_content)); return s, self._buf.get(s, "")
        prod_dom = getattr(self, "dom", None); buffer_content_dom = (self._last_dom_seen or "").strip()
        if prod_dom and (getattr(prod_dom, "done", False) or getattr(prod_dom, "seen", False)) and buffer_content_dom:
            network_seen = self._first_network_seen_ts is not None; dom_allowed = not (self.fast_path_strict and network_seen and not self._allow_dom_after_seen_guard)
            if dom_allowed: status = "done" if getattr(prod_dom, "done", False) else "seen"; jlog("best_snapshot_found", src="dom", status=status, len=len(buffer_content_dom)); return "dom", self._last_dom_seen
            else: status = "done" if getattr(prod_dom, "done", False) else "seen"; jlog("best_snapshot_skip_dom_buffer_guard", status=status)
        jlog("best_snapshot_not_found_v7.10", level="WARN"); return None, ""


    # --- _emit (MODIFIÉ: log version V7.11) ---
    def _emit(self, text: str, *, src: str) -> None:
        if self._done_evt.is_set(): jlog("emit_ignored_already_done", src=src, len=len(text or ""), level="WARN"); return
        self._emit_text = text if isinstance(text, str) else ""
        if not hasattr(self, '_emit_meta'): self._emit_meta = {}
        self._emit_meta["source_chosen"] = src; self._emit_meta["final_len"] = len(self._emit_text); is_invalid = self._emit_meta.get("invalid_response", False)
        log_payload = {"source_chosen": src, "final_len": self._emit_meta["final_len"], "invalid_response": is_invalid, **(self._final_cleaner_stats or {})}
        if "guard_wait_ms" in self._emit_meta: log_payload["guard_wait_ms"] = self._emit_meta["guard_wait_ms"]
        if "total_ms" in self._emit_meta: log_payload["total_ms"] = self._emit_meta["total_ms"]
        if not self._emit_text and not is_invalid: jlog("emit_final_empty", **log_payload, level="WARN")
        else: jlog("emit_final_success_v7.11_corrected", **log_payload) # Log version V7.11
        self._done_evt.set()


    # --- _stop_all (inchangé) ---
    async def _stop_all(self) -> None:
        jlog("stop_all_producers_start"); coros = []; stopped_names = []; producers_to_stop = PRIO
        for prod_name in producers_to_stop:
            prod = getattr(self, prod_name, None)
            if prod and not isinstance(prod, BaseProducerPlaceholder):
                stop_method = getattr(prod, "stop", None)
                if stop_method and inspect.iscoroutinefunction(stop_method):
                    async def safe_stop(p=prod, name=prod_name):
                        try: await asyncio.wait_for(p.stop(), timeout=2.0)
                        except asyncio.TimeoutError: jlog("producer_stop_timeout", name=name, level="WARN")
                        except Exception as e: jlog("producer_stop_error_internal", name=name, error=str(e), level="WARN")
                    coros.append(safe_stop()); stopped_names.append(prod_name)
        jlog("stop_all_producers_gathered", count=len(coros), names=stopped_names)
        if coros:
            try:
                results = await asyncio.wait_for(asyncio.gather(*coros, return_exceptions=True), timeout=5.0)
                for i, res in enumerate(results):
                    if isinstance(res, Exception) and not isinstance(res, asyncio.TimeoutError):
                        name = stopped_names[i] if i < len(stopped_names) else f"unknown_index_{i}"; jlog("producer_stop_error_gather", name=name, error=str(res), level="WARN")
            except asyncio.TimeoutError: jlog("stop_all_gather_timeout", level="WARN")
            except Exception as e: jlog("stop_all_gather_unexpected_error", error=str(e), error_type=type(e).__name__, level="ERROR")
        jlog("stop_all_producers_finish")