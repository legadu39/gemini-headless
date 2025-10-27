# gemini_headless/collect/producers/ws.py
from __future__ import annotations
import json, re
from typing import Callable, Dict, Optional, List # Ajout List
from playwright.async_api import Page, Error as PlaywrightError # Ajout PlaywrightError
import asyncio # Ajout asyncio

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
    def jlog(*_a, **_k): pass  # type: ignore

class WSProducer:
    """
    Producteur WebSocket via CDPMultiTarget (Page + Service Worker).
    Emet on_done('ws') des qu'un marqueur de fin est detecte.
    """

    def __init__(self, page: Page, on_progress: Callable[[str], None], on_done: Callable[[str, Optional[str]], None]):
        self.page = page
        self.on_progress_cb = on_progress
        self.on_done_cb = on_done
        self.seen: bool = False
        self.done: bool = False
        self._mt: Optional[CDPMultiTarget] = None
        self._buf: str = ""

    async def start(self) -> None:
        if self._mt is not None: return
        if self.page.is_closed() or self.page.context.is_closed():
             jlog("ws_start_fail_page_closed"); return
        try:
            self._mt = CDPMultiTarget(self.page)
            self._mt.on("Network.webSocketFrameReceived", self._on_ws_frame)
            self._mt.on("Network.webSocketClosed", self._on_ws_closed)
            await self._mt.start()
            jlog("ws_start_ok")
        except Exception as e:
             jlog("ws_start_error", error=str(e), error_type=type(e).__name__)
             if self._mt:
                 try: await self._mt.stop()
                 except Exception: pass
                 self._mt = None


    async def stop(self) -> None:
        try:
            if self._mt is not None: await self._mt.stop()
        except Exception as e: jlog("ws_stop_error", error=str(e), level="WARN")
        finally:
             self._mt = None
             self.seen = False

    def _on_ws_frame(self, params: Dict) -> None:
        # *** LOG BRUT ***
        jlog("ws_raw_frame_received", params_head=str(params)[:300]) # Augmenté la taille

        if self.done: return
        try:
            self.seen = True
            payload = params.get("response", {}).get("payloadData", "")
            if not isinstance(payload, str) or not payload.strip():
                # jlog("ws_frame_empty_or_not_string", payload_type=type(payload).__name__) # Peut être bruyant
                return

            text_chunk = self._extract_text(payload)
            if text_chunk:
                # Ajouter espace si buffer non vide et ne finit pas par espace/nl
                prefix = " " if self._buf and not self._buf.endswith(("\n", " ")) else ""
                self._buf += prefix + text_chunk
                try: self.on_progress_cb(prefix + text_chunk)
                except Exception as cb_err: jlog("ws_on_progress_callback_error", error=str(cb_err), level="WARN")

            if self._looks_final(payload):
                if not self.done:
                    self.done = True
                    final_text = self._buf or None
                    try: self.on_done_cb("ws", final_text)
                    except Exception as cb_err: jlog("ws_on_done_callback_error", error=str(cb_err), level="WARN")
                    jlog("producer_done", src="ws", size=len(final_text or ""), reason="final_marker_detected")
        except Exception as e:
            jlog("ws_on_frame_error", error=str(e), error_type=type(e).__name__, level="ERROR")


    def _on_ws_closed(self, _params: Dict) -> None:
        jlog("ws_closed_event_received")
        # Ne pas marquer comme 'done' ici, car cela pourrait être prématuré.
        pass

    @staticmethod
    def _looks_final(raw: str) -> bool:
        # (Logique existante)
        if not raw: return False
        L = raw.lower()
        final_markers = [
            '"final":true', '"complete":true', '"finished":true', '"is_final":true',
            '"state":"done"', '"state":"completed"'
        ]
        if any(k in L for k in final_markers): return True
        if re.search(r'"finish(?:_?reason)?"\s*:\s*"(stop|complete|finished|done)"', L): return True
        return False

    @staticmethod
    def _extract_text(raw: str) -> str:
        # (Logique existante - revue légèrement)
        if not raw: return ""
        try:
            obj = json.loads(raw)
            texts_found: List[str] = []
            # Fonction interne simple pour chercher récursivement
            def find_strings(node):
                if isinstance(node, str):
                    s = node.strip()
                    # Heuristique simple pour éviter bruit courant
                    if len(s) > 10 and s.lower() not in {"ok", "done", "error", "success", "[start]", "[end]"}:
                        texts_found.append(s)
                elif isinstance(node, dict):
                    # Prioriser clés communes
                    for k in ("text", "message", "content", "result", "data", "snippet"):
                        if k in node: find_strings(node[k])
                    # Parcourir reste si rien trouvé? Moins fiable.
                elif isinstance(node, list):
                    for item in node: find_strings(item) # Itérer sur les listes

            find_strings(obj)
            if texts_found:
                 # Joindre avec espace pour éviter mots collés
                 return " ".join(texts_found)
            return ""

        except json.JSONDecodeError:
             s = raw.strip()
             # Accepter comme texte brut si assez long et pas un contrôle
             if len(s) > 15 and s.lower() not in {"[start]", "[end]", "ok", "ping", "pong", "[]", "{}"}:
                 return s
             return ""
        except Exception as e:
            jlog("ws_extract_text_error", error=str(e), raw_head=raw[:100], level="WARN")
            return ""