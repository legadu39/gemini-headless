# gemini_headless/collect/producers/sse.py
# CORRIGÉ FINAL : Harmonisation des fonctions d'extraction avec be.py
from __future__ import annotations
import json, re, time, random # Ajout random
from typing import Callable, Dict, Optional, Set, Any, List
from playwright.async_api import Page, Error as PlaywrightError
import asyncio
import traceback # Ajout traceback

try:
    # Use relative import within the package
    from ...connectors.cdp_multiattach import CDPMultiTarget
except ImportError:
    # Fallback for running script directly or package structure issues
    try:
        from gemini_headless.connectors.cdp_multiattach import CDPMultiTarget # type: ignore
    except ImportError as e:
         raise ImportError(f"Could not import CDPMultiTarget. Ensure 'connectors' is accessible. Original error: {e}")


try:
    from ..utils.logs import jlog
except ImportError:
    # Fallback logger plus robuste
    import sys
    _jlog_fallback_cache_sse = {}
    def jlog(*_a, evt="unknown_sse_fallback", level="INFO", **_k):
        global _jlog_fallback_cache_sse
        if not _jlog_fallback_cache_sse.get("used"):
            print("WARNING: Using fallback jlog in sse.py", file=sys.stderr)
            _jlog_fallback_cache_sse["used"] = True
        try:
            payload = {"evt": evt, "ts": time.time(), "level": level, "module": "sse.py", **_k}
            print(json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr)
            sys.stderr.flush()
        except Exception:
            print(f'{{"evt": "jlog_sse_fallback_error", "original_evt": "{evt}"}}', file=sys.stderr)
            sys.stderr.flush()


_DONE_EVENTS: Set[str] = {"done", "complete", "completed", "finish", "finished", "end", "ended"}
# Seuil de silence (en secondes)
_SSE_SILENCE_THRESHOLD_S = 2.5 # Légèrement augmenté

# --- Heuristique V2 (Copiée de be.py) ---
def _looks_like_potential_answer_text(s: str) -> bool:
    """Heuristic V2: Stricter rejection of metadata, better prose detection."""
    if not s: return False
    s_strip = s.strip()
    if len(s_strip) < 15: return False # Minimum length

    low = s_strip.lower()
    if low in {"null", "{}", "[]", "true", "false", "ok", "[start]", "[end]", "ping"}: return False

    # *** Explicit rejection of known metadata patterns ***
    if s_strip.startswith('[[[[') or \
       s_strip.startswith('[[["me"') or \
       s_strip.startswith('[null,'): # Stricter check
        jlog("sse_heuristic_reject_reason", reason="starts_with_metadata_pattern", head=s_strip[:60]) # Modifié be_ -> sse_
        return False

    # Reject simple URLs/paths or pure JSON structures
    if re.fullmatch(r"[\w\-_./:=?&]+", s_strip) and ' ' not in s_strip and '\n' not in s_strip: return False
    if re.fullmatch(r"[{}\[\]:, \"'0-9\-_]+", s_strip): return False
    if re.match(r'^\[\s*".*?"\s*,\s*".*?"\s*(?:,.*)?\]$', s_strip): return False # Metadata array like ["Wrb.fr", ...]

    # Require letters and reasonable prose characteristics
    if not re.search(r"[a-zA-Z]{3,}", s_strip): return False # Need at least one word
    word_count = len(s_strip.split())
    has_newline = '\n' in s_strip
    if word_count < 3 and not has_newline: # Slightly stricter word count for single lines
        jlog("sse_heuristic_reject_reason", reason="too_few_words_no_newline_v2", head=s_strip[:60]) # Modifié be_ -> sse_
        return False

    alpha_chars = sum(1 for char in s_strip if char.isalpha())
    total_chars = len(s_strip)
    symbol_chars = sum(1 for char in s_strip if char in '[]{},":') # Count specific symbols
    if total_chars > 0:
        alpha_ratio = alpha_chars / total_chars
        # Stricter ratio check
        if alpha_ratio < 0.5 or symbol_chars > alpha_chars / 2.5:
             jlog("sse_heuristic_reject_reason", reason="low_alpha_or_high_symbol_ratio_v2", head=s_strip[:60], alpha_ratio=round(alpha_ratio, 2), symbol_count=symbol_chars) # Modifié be_ -> sse_
             return False

    # If it passes all checks, log acceptance (Optional)
    # jlog("sse_heuristic_accept_reason", reason="prose_detected_v2", head=s_strip[:60])
    return True
# --- Fin Heuristique V2 ---


# --- Collecte Texte V2 (Copiée de be.py) ---
def _collect_texts_robust(node: Any, acc: List[str], visited: set, depth: int = 0, max_depth: int = 20) -> None:
    """Recursively traverses JSON structure using the refined heuristic."""
    if depth > max_depth or not node: return
    node_id_repr = None; is_hashable = True
    try: node_id = id(node); hash(node)
    except TypeError:
        is_hashable = False
        try:
            if isinstance(node, list): node_id_repr = f"list_{len(node)}_{id(node)}"
            elif isinstance(node, dict): node_id_repr = f"dict_{len(node.keys())}_{id(node)}"
            else: node_id_repr = repr(node)[:100]
        except Exception: node_id_repr = f"unrepr_{id(node)}"

    item_to_check = node_id if is_hashable else node_id_repr
    try:
        # Check if hashable item is already visited
        if item_to_check in visited: return
        # Add hashable item to visited set
        visited.add(item_to_check)
    except TypeError: pass # Ignore unhashable types for visited check (handled by repr below)
    except Exception as e:
        # Log error if adding to visited set fails unexpectedly
        jlog("collect_texts_visited_error", error=str(e), item_repr=repr(item_to_check)[:100], level="WARN"); return

    if isinstance(node, str):
        st = node.strip()
        # Check if the stripped string looks like potential answer text and hasn't been added yet
        if _looks_like_potential_answer_text(st) and st not in acc: acc.append(st)
        return

    if isinstance(node, dict):
        # Specific handling for Gemini structure
        candidates = node.get("candidates")
        if isinstance(candidates, list):
            for c in candidates:
                try:
                    content = c.get("content") if isinstance(c, dict) else None
                    if isinstance(content, dict):
                        parts = content.get("parts")
                        if isinstance(parts, list):
                            for p in parts:
                                text = p.get("text") if isinstance(p, dict) else None
                                # Check if text is a non-empty string
                                if isinstance(text, str):
                                    st = text.strip()
                                    # Add text if it's potential answer and not already in accumulator
                                    if st and _looks_like_potential_answer_text(st) and st not in acc: acc.append(st)
                except Exception as e_cand: jlog("collect_texts_candidate_error", error=str(e_cand), level="DEBUG")

        # Generic handling for common keys
        for key in ("text", "content", "message", "snippet", "title"):
            value = node.get(key)
            try: value_id = id(value); value_hashable = True; hash(value)
            except TypeError: value_hashable = False; value_id = None # Check if value is hashable

            # Skip if already visited
            if value_hashable and value_id in visited: continue

            if isinstance(value, str):
                st = value.strip()
                # Add text if it's potential answer and not already in accumulator
                if st and _looks_like_potential_answer_text(st) and st not in acc:
                     acc.append(st)
            elif isinstance(value, (dict, list)):
                 # Recursively call for dicts or lists, create copy of visited if node wasn't hashable
                 _collect_texts_robust(value, acc, visited.copy() if not is_hashable else visited, depth + 1, max_depth)
        return # End processing for dictionary node

    if isinstance(node, list):
         for item in node:
              try: item_id = id(item); item_hashable = True; hash(item)
              except TypeError: item_hashable = False; item_id = None # Check if item is hashable

              # Skip if already visited
              if item_hashable and item_id in visited: continue

              # Recursively call for dicts, lists, or strings
              if isinstance(item, (dict, list, str)):
                   _collect_texts_robust(item, acc, visited.copy() if not is_hashable else visited, depth + 1, max_depth)
         return # End processing for list node
# --- Fin Collecte Texte V2 ---


class SSEProducer:
    """Producteur SSE NASA++ V3: Logging événementiel détaillé."""

    def __init__(self, page: Page, on_progress: Callable[[str], None], on_done: Callable[[str, Optional[str]], None]):
        self.page = page
        self.on_progress_cb = on_progress
        self.on_done_cb = on_done
        self.seen: bool = False
        self.done: bool = False
        self._mt: Optional[CDPMultiTarget] = None
        self._buf: Dict[str, str] = {}
        self._active_es: Set[str] = set()
        self._token_count: int = 0
        self._last_message_ts: Optional[float] = None
        self._silence_check_task: Optional[asyncio.Task] = None
        jlog("sse_producer_init", object_id=id(self))

    async def start(self) -> None:
        if self._mt is not None: jlog("sse_start_skipped_already_started", level="WARN"); return
        if self.page.is_closed() or self.page.context.is_closed(): jlog("sse_start_fail_page_closed"); return
        try:
            # Reset state
            self.seen = False; self.done = False; self._token_count = 0
            self._buf.clear(); self._active_es.clear(); self._last_message_ts = None

            self._mt = CDPMultiTarget(self.page)
            # Register listeners BEFORE starting CDPMultiTarget
            self._mt.on("Network.responseReceived", self._on_response_received)
            self._mt.on("Network.eventSourceMessageReceived", self._on_sse_message)
            self._mt.on("Network.loadingFinished", self._on_loading_finished)
            # Start CDPMultiTarget
            await self._mt.start()
            # Start silence check task AFTER CDPMultiTarget is started
            if self._silence_check_task is None or self._silence_check_task.done():
                self._silence_check_task = asyncio.create_task(self._check_silence_loop(), name="sse_silence_check")
                jlog("sse_start_ok_silence_task_created")
            else: jlog("sse_start_ok_silence_task_reused", level="WARN")

        except Exception as e:
             jlog("sse_start_error", error=str(e), error_type=type(e).__name__, traceback=traceback.format_exc(limit=3), level="CRITICAL")
             await self._cleanup_resources() # Attempt cleanup on start failure

    async def _cleanup_resources(self):
        """Helper for robust resource cleanup."""
        # ... (code inchangé) ...
        jlog("sse_cleanup_resources_start")
        if self._silence_check_task and not self._silence_check_task.done():
            self._silence_check_task.cancel()
            try: await asyncio.wait_for(self._silence_check_task, timeout=0.5)
            except (asyncio.CancelledError, asyncio.TimeoutError): pass
            except Exception as e_task: jlog("sse_cleanup_silence_task_error", error=str(e_task), level="WARN")
        self._silence_check_task = None
        if self._mt:
            try: await self._mt.stop()
            except Exception as e_mt: jlog("sse_cleanup_mt_stop_error", error=str(e_mt), level="WARN")
        self._mt = None
        self._buf.clear(); self._active_es.clear()
        jlog("sse_cleanup_resources_complete")

    async def stop(self) -> None:
        # ... (code inchangé) ...
        jlog("sse_stop_called")
        await self._cleanup_resources()
        self.seen = False; self.done = False; self._token_count = 0; self._last_message_ts = None
        jlog("sse_stop_complete")

    # --- Handlers d'événements CDP ---

    def _on_response_received(self, params: Dict) -> None:
        # ... (code inchangé) ...
        if self.done: return
        try:
            r = params.get("response", {})
            req_id = params.get("requestId")
            mime = r.get("mimeType", "")
            if isinstance(mime, str) and "event-stream" in mime and req_id:
                if req_id not in self._active_es:
                    self._active_es.add(req_id)
                    self._last_message_ts = time.monotonic()
                    jlog("sse_seen_stream", requestId=req_id, mimeType=mime, active_count=len(self._active_es))
        except Exception as e: jlog("sse_on_response_received_error", error=str(e), error_type=type(e).__name__, level="WARN", params_head=str(params)[:200])

    def _on_loading_finished(self, params: Dict) -> None:
        # ... (code inchangé) ...
        if self.done: return
        try:
            req_id = params.get("requestId")
            if req_id in self._active_es:
                 jlog("sse_loading_finished_for_active_stream", requestId=req_id)
        except Exception as e: jlog("sse_on_loading_finished_error", error=str(e), error_type=type(e).__name__, level="WARN", params_head=str(params)[:200])

    def _on_sse_message(self, params: Dict) -> None:
        # *** Log de diagnostic ajouté ***
        jlog("_on_sse_message_invoked_debug", requestId=params.get("requestId", "unknown_reqid"), params_keys=list(params.keys()), level="DEBUG")

        # *** LOG D'INVOCATION CRITIQUE - PREMIÈRE LIGNE (maintenu) ***
        req_id = params.get("requestId", "unknown_reqid")
        jlog("sse_on_message_callback_invoked_debug", requestId=req_id, params_keys=list(params.keys()), level="DEBUG") # Maintenu pour cohérence

        if self.done: return # Ignorer si déjà marqué comme terminé

        # Robustesse: Envelopper toute la logique dans try/except
        try:
            # Vérifier si le stream est connu ou tolérer si aucun n'est actif
            if self._active_es and req_id not in self._active_es:
                 jlog("sse_ignore_inactive_stream_message", requestId=req_id, active_ids=list(self._active_es), level="DEBUG")
                 return

            self.seen = True
            current_ts = time.monotonic()
            # *** MISE À JOUR CRITIQUE DU TIMESTAMP ***
            self._last_message_ts = current_ts
            self._token_count += 1

            event_name = (params.get("eventName") or "").strip().lower()
            data = params.get("data") or ""

            # Log brut pour analyse (peut être mis en DEBUG)
            jlog("sse_raw_message_data_debug", requestId=req_id, event_name=event_name, data_head=data[:200], data_len=len(data), level="DEBUG")

            # Utilise la fonction _extract_text_robust harmonisée
            text_chunk = self._extract_text_robust(data)

            # *** LA LOGIQUE SUIVANTE EST MAINTENUE ***
            if text_chunk:
                current_buf = self._buf.get(req_id, "")
                MAX_BUF_SIZE = 10 * 1024 * 1024 # 10 MB
                if len(current_buf) < MAX_BUF_SIZE:
                    prefix = " " if current_buf and not current_buf.endswith(("\n", " ", "\t")) else ""
                    clean_chunk = text_chunk.replace('\r\n','\n').replace('\r','\n').strip()
                    if clean_chunk: # Only add if not empty after cleaning
                        new_content = current_buf + prefix + clean_chunk
                        # Check size again after potential prefix addition
                        if len(new_content) <= MAX_BUF_SIZE:
                            self._buf[req_id] = new_content
                            try:
                                jlog("sse_on_progress_callback_call", chunk_len=len(prefix + clean_chunk), requestId=req_id, level="DEBUG")
                                self.on_progress_cb(prefix + clean_chunk)
                            except Exception as cb_err: jlog("sse_on_progress_callback_error", error=str(cb_err), error_type=type(cb_err).__name__, level="WARN")
                        else:
                             jlog("sse_buffer_limit_reached_on_append", requestId=req_id, limit=MAX_BUF_SIZE, current_len=len(current_buf), chunk_len=len(clean_chunk), level="WARN")
                else: jlog("sse_buffer_limit_reached", requestId=req_id, limit=MAX_BUF_SIZE, level="WARN")

            # Détection de fin explicite (basée sur 'data' brut)
            is_explicit_done = (event_name in _DONE_EVENTS) or self._looks_final(data)

            if is_explicit_done:
                if not self.done: # Marquer et notifier une seule fois
                    self.done = True
                    final_text_snapshot = self._snapshot()
                    try:
                        jlog("sse_on_done_callback_call_explicit", final_text_len=len(final_text_snapshot or ""), requestId=req_id, event_name=event_name, data_head=data[:50])
                        self.on_done_cb("sse", final_text_snapshot)
                    except Exception as cb_err: jlog("sse_on_done_callback_error", error=str(cb_err), error_type=type(cb_err).__name__, level="WARN")
                    jlog("producer_done", src="sse", size=len(final_text_snapshot or ""), token_count=self._token_count, reason="explicit_marker")
                    # Pas besoin d'annuler silence task, elle verra self.done

        # Log d'erreur général pour ce callback (avec traceback complet)
        except Exception as e:
            jlog("sse_on_message_error", error=str(e), error_type=type(e).__name__, level="ERROR",
                 traceback=traceback.format_exc(), # Traceback complet
                 raw_params_head=str(params)[:200])

    # --- Méthodes internes ---

    def _snapshot(self) -> Optional[str]:
        # ... (code inchangé) ...
        if not self._buf: return None
        full_text = " ".join(self._buf[k] for k in sorted(self._buf.keys())).strip()
        if full_text:
            full_text = full_text.replace("\r\n","\n").replace("\r","\n")
            full_text = re.sub(r"[ \t]+\n", "\n", full_text)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text).strip()
        return full_text if full_text else None

    async def _check_silence_loop(self) -> None:
        # ... (code inchangé) ...
        jlog("sse_silence_check_loop_started", threshold_s=_SSE_SILENCE_THRESHOLD_S)
        try:
            while not self.done:
                await asyncio.sleep((_SSE_SILENCE_THRESHOLD_S / 2) + random.uniform(0.1, 0.3))
                if self.done: break # Double check
                if self.seen and self._last_message_ts is not None:
                    silence_duration = time.monotonic() - self._last_message_ts
                    if silence_duration >= _SSE_SILENCE_THRESHOLD_S:
                        jlog("sse_silence_threshold_reached", silence_duration=round(silence_duration,1))
                        final_text_snapshot = self._snapshot()
                        if final_text_snapshot and not self.done:
                            self.done = True # Marquer ICI
                            try:
                                jlog("sse_on_done_callback_call_silence", final_text_len=len(final_text_snapshot), silence_duration=round(silence_duration,1))
                                self.on_done_cb("sse", final_text_snapshot)
                            except Exception as cb_err: jlog("sse_on_done_callback_error_silence", error=str(cb_err), error_type=type(cb_err).__name__, level="WARN")
                            jlog("producer_done", src="sse", size=len(final_text_snapshot), token_count=self._token_count, reason="silence_threshold")
                            break # Sortir après done par silence
                        else:
                            if not self.done: jlog("sse_silence_detected_no_text", silence_duration=round(silence_duration,1), level="DEBUG")
                            else: break # Sortir si déjà done par une autre voie
        except asyncio.CancelledError: jlog("sse_silence_check_cancelled")
        except Exception as e: jlog("sse_silence_check_error", error=str(e), error_type=type(e).__name__, level="ERROR", traceback=traceback.format_exc(limit=3))
        finally: jlog("sse_silence_check_loop_ended", done_state=self.done)

    @staticmethod
    def _looks_final(raw: str) -> bool:
        # ... (code inchangé) ...
        if not raw: return False
        L_norm = raw.lower().replace(" ", "").replace("\n","").replace("\r","")
        final_markers_json = ['"done":true', '"final":true', '"isfinal":true', '"finishreason":"stop"', '"finishreason":"complete"', '"state":"completed"', ',"rc":"FIN"']
        if any(marker in L_norm for marker in final_markers_json): return True
        try:
            if re.search(r'"finish(?:_?reason)?"\s*:\s*"(stop|complete|finished|done)"', L_norm): return True
            if re.search(r'"event"\s*:\s*"(done|complete|finish|end)"', L_norm): return True
            if re.search(r'^event\s*:\s*(done|complete|finish|end)\s*$', raw, re.MULTILINE | re.IGNORECASE): return True
        except Exception: pass
        return False

    # Méthode renommée pour éviter conflit potentiel et utiliser la version copiée
    @staticmethod
    def _extract_text_robust(raw: str) -> str:
        """Utilise _collect_texts_robust (copié de be.py) pour extraire le texte."""
        if not raw: return ""
        s = raw
        # Correction : Assurer que le préfixe 'data:' est bien supprimé, même avec des espaces.
        if s.lstrip().startswith("data:"): s = s.lstrip()[len("data:"):]
        s_strip = s.strip()
        if not s_strip: return ""

        texts_found: List[str] = []
        try:
            # Essayer de parser comme JSON
            obj = json.loads(s_strip) # Utiliser s_strip ici
            # Appeler la fonction de collecte robuste (qui utilise l'heuristique robuste)
            _collect_texts_robust(obj, texts_found, set(), max_depth=15)
            if texts_found:
                 joined_text = " ".join(t.strip() for t in texts_found if t.strip())
                 # Vérifier à nouveau avec l'heuristique après collecte
                 if _looks_like_potential_answer_text(joined_text):
                     return joined_text
                 else:
                     jlog("sse_extract_text_robust_rejected_post_collect", head=joined_text[:100], level="DEBUG")
                     return "" # Rejeté par heuristique post-collecte
            else: return "" # JSON analysé mais vide de texte pertinent

        except json.JSONDecodeError:
             # Si ce n'est pas du JSON, traiter comme texte brut mais vérifier avec l'heuristique
             if _looks_like_potential_answer_text(s_strip):
                 return s_strip # Accepter si texte brut semble pertinent
             else:
                 jlog("sse_extract_text_robust_rejected_raw", head=s_strip[:100], level="DEBUG")
                 return "" # Rejeté par heuristique

        except Exception as e:
            jlog("sse_extract_text_robust_unexpected_error", error=str(e), error_type=type(e).__name__, raw_head=raw[:100], level="WARN")
            # Fallback prudent: accepter si texte brut semble pertinent
            if _looks_like_potential_answer_text(s_strip): return s_strip

        return "" # Default vide