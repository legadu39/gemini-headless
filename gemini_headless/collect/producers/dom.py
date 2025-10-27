# gemini_headless/collect/producers/dom.py
# CORRIGÉ V7.11 (Sélecteurs DOM étendus + Priorité JSON Sentinel + JS stable)
# - FIX: Helpers JS implémentés (isVisible, closestAuthor, isActionOrStatus, hasActionsNearby)
# - FIX: Multi-sélecteurs querySelectorAll corrigés (join(','))
# - FIX: Fallbacks effectivement exécutés (pickCandidate)
# - FIX: Aucune référence à jlog côté navigateur (utilise console.* / logPage guard)
# - ADD: Extraction JSON+<<END>> robuste (balance d’accolades, gestion des chaînes)
# - ADD: Parcours récursif de toutes les frames (BFS) côté Python
# - KEEP: Priorité au JSON+<<END>>, sinon meilleur texte brut

from __future__ import annotations
import asyncio, time, re, json
from typing import Callable, Optional, List, Dict, Any
from playwright.async_api import Page, Frame, Error as PlaywrightError

try:
    from ..utils.logs import jlog
except ImportError:
    try:
        from utils.logs import jlog  # type: ignore
    except ImportError:
        def jlog(*_a, **_k): pass  # type: ignore

# -------------------------------
# JS V7.11 (Sélecteurs étendus + Priorité JSON Sentinel + Helpers implémentés)
# -------------------------------

_GET_STATE_JS = r"""
() => {
  try {
    // ==== Constantes ====
    const NON_ANSWER_TEXTS_FR = [
      "je ne peux pas", "je ne suis pas sûr", "réessaye", "réessayer",
      "désolé", "impossible de", "limit", "limite", "erreur", "aucune réponse",
      "je suis un modèle", "j'ai été entraîné", "je ne peux pas accéder"
    ];
    const NON_ANSWER_TEXTS_EN = [
      "i cannot", "i'm not sure", "try again", "please try",
      "sorry", "unable to", "error", "no answer", "as an ai model",
      "i was trained", "i can't access"
    ];
    const NON_ANSWER_TEXTS = [...NON_ANSWER_TEXTS_FR, ...NON_ANSWER_TEXTS_EN].map(t => t.toLowerCase());
    const MIN_VALID_LENGTH = 50;

    // ==== Logger côté page (safe) ====
    const logPage = (...args) => { try { console.debug("[DOM V7.11]", ...args); } catch(_) {} };

    // ==== Helpers ====
    const isVisible = (el) => {
      if (!el || !el.isConnected) return false;
      const r = el.getBoundingClientRect();
      if (r.width <= 0 || r.height <= 0) return false;
      const s = getComputedStyle(el);
      if (s.visibility === "hidden" || s.display === "none" || s.opacity === "0") return false;
      // check ancestors visibility
      let p = el.parentElement;
      for (let i=0; i<25 && p; i++, p = p.parentElement) {
        const ps = getComputedStyle(p);
        if (ps.display === "none" || ps.visibility === "hidden") return false;
      }
      return true;
    };

    const closestAuthor = (el) => {
      const n = el.closest('[data-message-author],[data-author]');
      const a = n && (n.getAttribute('data-message-author') || n.getAttribute('data-author'));
      if (a === 'user') return 'user';
      return 'assistant';
    };

    const isActionOrStatus = (el) => {
      if (!el) return false;
      if (el.closest('[role="status"],[aria-busy="true"],[aria-live],.loading,.progress,.spinner')) return true;
      return false;
    };

    const hasActionsNearby = (el, depth = 3) => {
      if (!el) return false;
      let p = el;
      let d = 0;
      const ACTION_SEL = [
        'button', '[role="button"]', '[aria-label*="copy" i]', '[aria-label*="copier" i]',
        '[aria-label*="share" i]', '[aria-label*="partager" i]',
        '[data-tooltip*="Copy" i]', '[data-tooltip*="Copier" i]'
      ].join(',');
      while (p && d++ < depth) {
        if (p.querySelector(ACTION_SEL)) return true;
        p = p.parentElement;
      }
      return false;
    };

    // ==== Extraction JSON+<<END>> robuste ====
    const extractJsonSentinelFromText = (rawText) => {
      if (!rawText) return null;
      const endToken = "<<END>>";
      const endIdx = rawText.lastIndexOf(endToken);
      if (endIdx === -1) return null;
      let i = endIdx - endToken.length;
      // ignore trailing spaces/newlines before token if any
      while (i >= 0 && /\s/.test(rawText[i])) i--;
      // scan backward to find matching '{' for JSON object
      let depth = 0, inStr = false, esc = false, startIdx = -1;
      for (; i >= 0; i--) {
        const ch = rawText[i];
        if (inStr) {
          if (esc) { esc = false; continue; }
          if (ch === '\\\\') { esc = true; continue; }
          if (ch === '"') { inStr = false; continue; }
          continue;
        }
        if (ch === '"') { inStr = true; continue; }
        if (ch === '}') { depth++; continue; }
        if (ch === '{') {
          if (depth === 0) { startIdx = i; break; }
          depth--; continue;
        }
      }
      if (startIdx === -1) return null;
      const candidate = rawText.slice(startIdx, endIdx).trim();
      try { JSON.parse(candidate); return candidate + endToken; } catch(_e) { return null; }
    };

    const normalizeText = (t) => (t || "").replace(/\n{3,}/g, "\n\n").trim();

    // deepTextContent : priorité JSON+<<END>>, sinon innerText/textContent
    const deepTextContent = (rootEl) => {
      if (!rootEl || !rootEl.isConnected) return "";
      // 1) Essai JSON+<<END>>
      const raw = rootEl.innerText || rootEl.textContent || "";
      const j = extractJsonSentinelFromText(raw);
      if (j) { logPage("JSON+END found"); return j; }
      // 2) Texte brut
      return normalizeText(raw);
    };

    // Heuristique : rechercher le meilleur container de texte assistant
    const findBestCandidateHeuristically = () => {
      let best = null, bestScore = -1;
      const searchZones = Array.from(document.querySelectorAll('main, [role="main"], [aria-live], body')) || [document.body];

      const SELS = [
        'div.response-container-content md-block',
        'div[data-message-author="assistant"]:not([aria-busy="true"]) .inner-content',
        'div[role="listitem"] article',
        'div[data-message-id]',
        'div.model-response-text',
        '.prose',
        'mat-mdc-card',
        '.multimodal-chunk',
        'div[jsname]',
        'article[aria-roledescription], article[role="article"]',
        '[data-md-type="content"]',
        // fallbacks larges
        'div, section, article'
      ].join(',');

      for (const zone of searchZones) {
        if (!zone || !zone.isConnected) continue;
        const candidates = Array.from(zone.querySelectorAll(SELS));
        if (zone.matches('div, section, article') && isVisible(zone)) candidates.push(zone);
        for (let i = candidates.length - 1; i >= 0; i--) {
          const el = candidates[i];
          if (!el || !el.isConnected || !isVisible(el)) continue;
          let score = 0;
          const author = closestAuthor(el);
          if (author === 'assistant') score += 100;
          else if (author === 'user') continue; // ignore user messages
          if (isActionOrStatus(el)) score -= 50;
          let textContent = "";
          try { textContent = deepTextContent(el); } catch(_) { textContent = (el.textContent || "").trim(); }
          if (!textContent) textContent = (el.textContent || "").trim();
          const textLength = textContent.length;
          const lower = textContent.toLowerCase();
          if (NON_ANSWER_TEXTS.some(n => lower.includes(n))) score -= 200;
          if (textLength > MIN_VALID_LENGTH) score += 20 + Math.min(textLength / 50, 20);
          else if (textLength > 15) score += 5;
          if (hasActionsNearby(el)) score += 30;
          const role = el.getAttribute('role');
          if (role === 'article') score += 10;
          let depthPenalty = 0, p = el.parentElement, d = 0;
          while (p && d < 20) { d++; p = p.parentElement; }
          depthPenalty = d;
          score -= depthPenalty;
          if (score > bestScore) { bestScore = score; best = el; }
        }
      }
      if (best) logPage("Heuristic best candidate score=", bestScore);
      return best;
    };

    // pickCandidate : préférés -> heuristique -> fallbacks
    const pickCandidate = () => {
      const preferredSelectors = [
        'div.response-container-content md-block:last-of-type',
        'div[data-last-interaction] .model-response-text:last-of-type',
        'article[data-author="assistant"]:last-of-type',
        '[data-message-author="assistant"]:not([aria-busy="true"]) .model-response-text:last-of-type'
      ];
      for (const sel of preferredSelectors) {
        try {
          const nodes = Array.from(document.querySelectorAll(sel));
          for (let i = nodes.length - 1; i >= 0; i--) {
            const n = nodes[i];
            if (isVisible(n) && closestAuthor(n) !== 'user') {
              logPage("Pick preferred:", sel);
              return n;
            }
          }
        } catch(e) { /* noop */ }
      }
      const h = findBestCandidateHeuristically();
      if (h) { logPage("Pick heuristic"); return h; }

      const fallbackSelectors = [
        "[data-message-author='assistant']:not([aria-busy='true']) .model-response-text",
        "[data-message-author='assistant']:not([aria-busy='true']) [role='article']",
        ".model-response-text",
        ".response-content-container",
        "main > div > div > div[jscontroller]",
        "main > div"
      ];
      for (const sel of fallbackSelectors) {
        try {
          const nodes = Array.from(document.querySelectorAll(sel));
          for (let i = nodes.length - 1; i >= 0; i--) {
            const n = nodes[i];
            if (isVisible(n) && closestAuthor(n) !== 'user') {
              logPage("Pick fallback:", sel);
              return n;
            }
          }
        } catch(e) { /* noop */ }
      }
      return null;
    };

    // ==== Main ====
    const el = pickCandidate();

    const infoFor = (node) => {
      if (!node) return "none";
      const cls = (node.className && typeof node.className === "string") ? ("." + node.className.split(/\s+/).slice(0,3).join(".")) : "";
      return (node.tagName || "el") + cls;
    };

    if (!el) {
      return { ready: false, text: "", aria: null, actions: false, reason: "no_candidate_v7.11", selected_el: "none" };
    }

    const text = deepTextContent(el);
    const isJsonSentinel = text && text.endsWith("<<END>>") && text.trim().startsWith("{");

    // aria-busy (ancêtre le plus proche)
    let ariaBusy = null;
    const busyNode = el.closest("[aria-busy]");
    if (busyNode) ariaBusy = (busyNode.getAttribute("aria-busy") || "").toLowerCase();

    const actions = hasActionsNearby(el);
    const lowerText = (text || "").toLowerCase();

    if (!isJsonSentinel) {
      if (!text || text.length < MIN_VALID_LENGTH) {
        return { ready: false, text, aria: ariaBusy, actions, reason: `text_too_short_or_empty_v7.11 (len: ${(text||"").length})`, selected_el: infoFor(el) };
      }
      if (NON_ANSWER_TEXTS.some(n => lowerText.includes(n))) {
        return { ready: false, text, aria: ariaBusy, actions, reason: "non_answer_text_detected_v7.11", selected_el: infoFor(el) };
      }
    }

    const isBusy = (ariaBusy === "true");
    // prêt si pas busy et (JSON OK ou actions proches pour un envoi)
    const ready = !isBusy && (isJsonSentinel || actions);
    const reason = ready ? "ready_v7.11" : (isBusy ? "aria_busy_true_v7.11" : "not_ready_no_json_or_actions_v7.11");

    return { ready, text, aria: ariaBusy, actions, reason, selected_el: infoFor(el) };

  } catch (err) {
    console.error("Error in _GET_STATE_JS V7.11:", err);
    return { ready: false, text: "", aria: null, actions: false, reason: "js_error_v7.11", error: (err && (err.message || String(err))) || "unknown", selected_el: "error_state" };
  }
}
"""

# Version "best text only"
_GET_BEST_TEXT_JS = r"""
() => {
  try {
    const logPage = (...args) => { try { console.debug("[DOM V7.11]", ...args); } catch(_) {} };

    const isVisible = (el) => {
      if (!el || !el.isConnected) return false;
      const r = el.getBoundingClientRect();
      if (r.width <= 0 || r.height <= 0) return false;
      const s = getComputedStyle(el);
      if (s.visibility === "hidden" || s.display === "none" || s.opacity === "0") return false;
      let p = el.parentElement;
      for (let i=0; i<25 && p; i++, p = p.parentElement) {
        const ps = getComputedStyle(p);
        if (ps.display === "none" || ps.visibility === "hidden") return false;
      }
      return true;
    };
    const closestAuthor = (el) => {
      const n = el.closest('[data-message-author],[data-author]');
      const a = n && (n.getAttribute('data-message-author') || n.getAttribute('data-author'));
      if (a === 'user') return 'user';
      return 'assistant';
    };

    const extractJsonSentinelFromText = (rawText) => {
      if (!rawText) return null;
      const endToken = "<<END>>";
      const endIdx = rawText.lastIndexOf(endToken);
      if (endIdx === -1) return null;
      let i = endIdx - endToken.length;
      while (i >= 0 && /\s/.test(rawText[i])) i--;
      let depth = 0, inStr = false, esc = false, startIdx = -1;
      for (; i >= 0; i--) {
        const ch = rawText[i];
        if (inStr) {
          if (esc) { esc = false; continue; }
          if (ch === '\\\\') { esc = true; continue; }
          if (ch === '"') { inStr = false; continue; }
          continue;
        }
        if (ch === '"') { inStr = true; continue; }
        if (ch === '}') { depth++; continue; }
        if (ch === '{') {
          if (depth === 0) { startIdx = i; break; }
          depth--; continue;
        }
      }
      if (startIdx === -1) return null;
      const candidate = rawText.slice(startIdx, endIdx).trim();
      try { JSON.parse(candidate); return candidate + endToken; } catch(_e) { return null; }
    };

    const deepTextContent = (rootEl) => {
      if (!rootEl || !rootEl.isConnected) return "";
      const raw = rootEl.innerText || rootEl.textContent || "";
      const j = extractJsonSentinelFromText(raw);
      if (j) return j;
      return (raw || "").replace(/\n{3,}/g, "\n\n").trim();
    };

    const preferredSelectors = [
      'div.response-container-content md-block:last-of-type',
      'div[data-last-interaction] .model-response-text:last-of-type',
      'article[data-author="assistant"]:last-of-type',
      '[data-message-author="assistant"]:not([aria-busy="true"]) .model-response-text:last-of-type'
    ];
    const tryPick = (selectors) => {
      for (const sel of selectors) {
        try {
          const nodes = Array.from(document.querySelectorAll(sel));
          for (let i = nodes.length - 1; i >= 0; i--) {
            const n = nodes[i];
            if (isVisible(n) && closestAuthor(n) !== 'user') return n;
          }
        } catch(_) {}
      }
      return null;
    };

    let el = tryPick(preferredSelectors);
    if (!el) {
      const SELS = [
        'div.response-container-content md-block',
        'div[data-message-author="assistant"]:not([aria-busy="true"]) .inner-content',
        'div[role="listitem"] article',
        'div[data-message-id]',
        'div.model-response-text',
        '.prose',
        'mat-mdc-card',
        '.multimodal-chunk',
        'div[jsname]',
        'article[aria-roledescription], article[role="article"]',
        '[data-md-type="content"]',
        'div, section, article'
      ].join(',');
      const zones = Array.from(document.querySelectorAll('main, [role="main"], [aria-live], body')) || [document.body];
      let best = null, bestScore = -1;
      for (const zone of zones) {
        if (!zone || !zone.isConnected) continue;
        const cands = Array.from(zone.querySelectorAll(SELS));
        if (zone.matches('div, section, article') && isVisible(zone)) cands.push(zone);
        for (let i = cands.length - 1; i >= 0; i--) {
          const n = cands[i];
          if (!n || !n.isConnected || !isVisible(n)) continue;
          const a = closestAuthor(n);
          if (a === 'user') continue;
          let t = "";
          try { t = deepTextContent(n); } catch(_) { t = (n.textContent || "").trim(); }
          const len = (t || "").length;
          let score = 0;
          if (a === 'assistant') score += 100;
          if (len > 50) score += 20 + Math.min(len / 50, 20);
          if (score > bestScore) { bestScore = score; best = n; }
        }
      }
      el = best;
    }

    if (!el) {
      const fallbacks = [
        "[data-message-author='assistant']:not([aria-busy='true']) .model-response-text",
        "[data-message-author='assistant']:not([aria-busy='true']) [role='article']",
        ".model-response-text",
        ".response-content-container",
        "main > div > div > div[jscontroller]",
        "main > div"
      ];
      el = tryPick(fallbacks);
    }

    if (!el) return "";

    return deepTextContent(el);
  } catch (err) {
    console.error("Error in _GET_BEST_TEXT_JS V7.11:", err);
    return "";
  }
}
"""

class DOMProducer:
    """Producteur DOM (V7.11 - Sélecteurs étendus, priorité JSON+<<END>>, JS robuste)."""

    def __init__(self, page: Page, on_progress: Callable[[str], None], on_done: Callable[[str, Optional[str]], None]):
        self.page = page
        self.on_progress_cb = on_progress
        self.on_done_cb = on_done
        self.seen: bool = False
        self.done: bool = False
        self._last_full_text: str = ""

    async def start(self) -> None:
        """Vérifie que les fonctions JS (V7.11) sont évaluables (prewarm)."""
        if self.page.is_closed():
            jlog("dom_start_fail_page_closed_v7.11", level="WARN")
            return
        try:
            await self.page.evaluate(_GET_STATE_JS)
            await self.page.evaluate(_GET_BEST_TEXT_JS)
            jlog("dom_js_prewarm_ok_v7.11")
        except Exception as e:
            jlog("dom_js_prewarm_failed_v7.11", error=str(e).split('\n')[0], js_location="_GET_STATE_JS/_GET_BEST_TEXT_JS V7.11", level="ERROR")

    async def stop(self) -> None:
        jlog("dom_stop_v7.11")
        self.seen = False
        self.done = False

    async def snapshot_now(self) -> str:
        """Extraction cross-frames : retourne le meilleur texte via JS V7.11 robuste (BFS frames)."""
        best_text = ""
        page_closed_logged = False

        t_start_snap = time.monotonic()
        jlog("snapshot_now_start_v7.11", strategy="evaluate_best_text_js_bfs")

        # Récupération frames en BFS (main + tous descendants)
        try:
            if self.page.is_closed():
                jlog("snapshot_now_page_closed_at_start_v7.11", level="WARN")
                return ""

            frames_to_check: List[Frame] = []
            try:
                main_frame = self.page.main_frame
            except Exception as mf_err:
                jlog("snapshot_now_main_frame_error_v7.11", error=str(mf_err), level="ERROR")
                return ""

            if not main_frame or main_frame.is_detached():
                jlog("snapshot_now_main_frame_detached_or_invalid_v7.11", level="WARN")
                return ""

            # BFS frames
            queue: List[Frame] = [main_frame]
            seen_ids = set()
            while queue:
                fr = queue.pop(0)
                try:
                    fid = getattr(fr, "guid", None) or id(fr)
                except Exception:
                    fid = id(fr)
                if fid in seen_ids:
                    continue
                seen_ids.add(fid)
                frames_to_check.append(fr)
                try:
                    children = [cf for cf in fr.child_frames if cf and not cf.is_detached()]
                except Exception as child_err:
                    jlog("snapshot_now_child_frame_list_error_v7.11", error=str(child_err), level="WARN")
                    children = []
                queue.extend(children)

            jlog("snapshot_now_frames_to_check_v7.11", count=len(frames_to_check))
        except Exception as frame_err:
            jlog("snapshot_now_frame_access_error_v7.11", error=str(frame_err), error_type=type(frame_err).__name__, level="ERROR")
            return ""

        # Évalue _GET_BEST_TEXT_JS sur chaque frame
        for idx, fr in enumerate(frames_to_check):
            frame_label = "main" if idx == 0 else f"child_{idx}"
            if self.page.is_closed():
                if not page_closed_logged:
                    jlog("snapshot_now_page_closed_during_iteration_v7.11", level="WARN")
                    page_closed_logged = True
                break
            if fr.is_detached():
                jlog("snapshot_now_skip_detached_frame_v7.11", frame=frame_label, frame_url=fr.url, level="DEBUG")
                continue
            try:
                js_expr = f"({_GET_BEST_TEXT_JS})()"
                txt = await asyncio.wait_for(fr.evaluate(js_expr), timeout=4.0)
                if isinstance(txt, str):
                    t_strip = txt.strip()
                    if t_strip.endswith("<<END>>") and t_strip.startswith("{"):
                        jlog("snapshot_now_json_sentinel_found_v7.11", frame=frame_label, len=len(t_strip))
                        best_text = t_strip
                        break  # JSON+END trouvé : résultat optimal
                    elif len(t_strip) > len(best_text):
                        jlog("snapshot_now_new_best_text_v7.11", frame=frame_label, old_len=len(best_text), new_len=len(t_strip), head=t_strip[:80])
                        best_text = t_strip
            except asyncio.TimeoutError:
                jlog("snapshot_now_eval_timeout_v7.11", frame=frame_label, frame_url=fr.url, level="WARN")
            except PlaywrightError as pw_err:
                err_str = str(pw_err).lower()
                short_err = err_str.split('\n')[0]
                is_context_destroyed = ("target closed" in err_str or
                                        "frame was detached" in err_str or
                                        "context was destroyed" in err_str)
                if is_context_destroyed:
                    jlog("snapshot_now_eval_context_destroyed_v7.11", frame=frame_label, frame_url=fr.url, error_short=short_err, level="WARN")
                    break
                else:
                    jlog("snapshot_now_eval_playwright_error_v7.11", frame=frame_label, frame_url=fr.url, error=short_err, level="WARN")
            except Exception as eval_err:
                short_err = str(eval_err).split('\n')[0]
                jlog("snapshot_now_eval_unexpected_error_v7.11", frame=frame_label, frame_url=fr.url, error=short_err, error_type=type(eval_err).__name__, level="ERROR")

        final_len = len(best_text)
        snap_ms = int((time.monotonic() - t_start_snap) * 1000)
        jlog("snapshot_now_result_v7.11",
             final_len=final_len,
             head=best_text[:80] if final_len > 0 else "EMPTY",
             snap_ms=snap_ms,
             is_json_sentinel=(best_text.endswith("<<END>>") and best_text.startswith("{")))
        return best_text
