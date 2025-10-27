# gemini_headless/collect/producers/be.py
from __future__ import annotations
import asyncio, json, re
from typing import Any, Callable, Dict, List, Optional, Tuple, Set
from playwright.async_api import Page, Response
from urllib.parse import urlparse, parse_qs

try:
    from ..utils.logs import jlog
except ImportError:
    def jlog(*_a, **_k): pass

_BEX_URL_PAT = re.compile(r"/_(/)?BardChatUi/data/batchexecute", re.I)
_XSSI_PREFIX = ")]}'"
_BL_VERSION_PAT = re.compile(r"[?&]bl=([^&]+)")

def _strip_xssi(s: str) -> str:
    """Removes potential XSSI prefix, handling variations."""
    if not s: return s
    s_strip = s.lstrip()
    match = re.match(r"^\s*(\d+\s*\n\s*)?" + re.escape(_XSSI_PREFIX), s_strip, re.DOTALL)
    if match:
        prefix_len = len(match.group(0))
        return s_strip[prefix_len:].lstrip("\n\r \t")
    elif s_strip.startswith(_XSSI_PREFIX):
         return s_strip[len(_XSSI_PREFIX):].lstrip("\n\r \t")
    return s_strip

# --- PATCHED HEURISTIC V2 ---
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
        jlog("be_heuristic_reject_reason", reason="starts_with_metadata_pattern", head=s_strip[:60])
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
        jlog("be_heuristic_reject_reason", reason="too_few_words_no_newline_v2", head=s_strip[:60])
        return False

    alpha_chars = sum(1 for char in s_strip if char.isalpha())
    total_chars = len(s_strip)
    symbol_chars = sum(1 for char in s_strip if char in '[]{},":') # Count specific symbols
    if total_chars > 0:
        alpha_ratio = alpha_chars / total_chars
        # Stricter ratio check
        if alpha_ratio < 0.5 or symbol_chars > alpha_chars / 2.5:
             jlog("be_heuristic_reject_reason", reason="low_alpha_or_high_symbol_ratio_v2", head=s_strip[:60], alpha_ratio=round(alpha_ratio, 2), symbol_count=symbol_chars)
             return False

    # If it passes all checks, log acceptance
    # jlog("be_heuristic_accept_reason", reason="prose_detected_v2", head=s_strip[:60]) # Optional: Log acceptance
    return True
# --- END PATCHED HEURISTIC V2 ---


def _collect_texts_robust(node: Any, acc: List[str], visited: set, depth: int = 0, max_depth: int = 20) -> None:
    """Recursively traverses JSON structure using the refined heuristic."""
    # ... (code _collect_texts_robust inchangé, il utilise la nouvelle heuristique) ...
    if depth > max_depth: return
    node_id_repr = None; is_hashable = True
    try: hash(node); node_id = id(node)
    except TypeError:
        is_hashable = False
        try:
            if isinstance(node, list): node_id_repr = f"list_{len(node)}_{str(node[:3])}"
            elif isinstance(node, dict): node_id_repr = f"dict_{len(node.keys())}_{str(list(node.keys())[:3])}"
            else: node_id_repr = str(node)[:100]
        except Exception: node_id_repr = None
    item_to_check = node_id if is_hashable else node_id_repr
    if item_to_check is not None:
        try:
            if item_to_check in visited: return
            visited.add(item_to_check)
        except Exception: pass

    if isinstance(node, str):
        st = node.strip()
        if _looks_like_potential_answer_text(st): # Utiliser l'heuristique V2
            if st not in acc: acc.append(st)
        return

    if isinstance(node, dict):
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
                                if isinstance(text, str):
                                    st = text.strip()
                                    if st and _looks_like_potential_answer_text(st) and st not in acc:
                                        acc.append(st)
                except Exception: pass
        for key in ("text", "content", "message", "snippet", "title"):
            value = node.get(key)
            if isinstance(value, str):
                st = value.strip()
                if st and _looks_like_potential_answer_text(st) and st not in acc:
                     acc.append(st)
            elif isinstance(value, (dict, list)):
                _collect_texts_robust(value, acc, visited.copy() if not is_hashable else visited, depth + 1, max_depth)
        return

    if isinstance(node, list):
         for item in node:
              if isinstance(item, (dict, list, str)):
                   _collect_texts_robust(item, acc, visited.copy() if not is_hashable else visited, depth + 1, max_depth)
         return

def _join_and_clean(parts: List[str]) -> str:
    """Cleans and joins collected text fragments."""
    # ... (code _join_and_clean inchangé) ...
    if not parts: return ""
    seen_norm = set(); out: List[str] = []
    min_len_threshold = 10

    for p in parts:
        q = p.strip()
        q = re.sub(r'<br\s*/?>', '\n', q, flags=re.IGNORECASE)
        q = re.sub(r'<[^>]+>', '', q)
        q = q.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
        q = q.strip()

        if not q or len(q) < min_len_threshold or q.lower() in {"ok", "done", "[start]", "[end]"}: continue
        if q.startswith('[[[[') or q.startswith('[[["me"') or q.startswith('[null,'): continue

        norm_q = ' '.join(q.lower().split())
        if norm_q in seen_norm: continue
        seen_norm.add(norm_q)
        out.append(q)

    if not out: return ""

    s = "\n".join(out)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+(\n)", r"\1", s);
    s = re.sub(r"\n{3,}", "\n\n", s);
    return s.strip()

def _parse_batchexecute_robust(raw: str) -> Tuple[str, Dict[str, Any]]:
    """Handles multi-JSON responses using raw_decode and the refined heuristic."""
    # ... (code _parse_batchexecute_robust inchangé, il appelle _collect_texts_robust et _join_and_clean corrigés) ...
    s_cleaned = _strip_xssi(raw).strip()
    if not s_cleaned:
         return "", {"matched": False, "segments_tried": 0, "json_errors": 0, "decoder_advances": 0}

    meta: Dict[str, Any] = {
        "matched": False, "segments_tried": 0, "json_errors": 0,
        "decoder_advances": 0, "final_text_segments_found": 0
    }
    texts: List[str] = []
    idx = 0
    decoder = json.JSONDecoder()
    parse_errors = []

    while idx < len(s_cleaned):
        original_idx = idx
        match_prefix = re.match(r"[\s\d,]*", s_cleaned[idx:])
        if match_prefix: idx += len(match_prefix.group(0))
        if idx >= len(s_cleaned): break

        if s_cleaned[idx] not in ('{', '['):
            next_brace = s_cleaned.find('{', idx); next_bracket = s_cleaned.find('[', idx)
            if next_brace == -1 and next_bracket == -1:
                skipped_content = s_cleaned[original_idx:idx]
                if len(skipped_content) > 10 and skipped_content.strip(): jlog("be_parse_skip_non_json", skipped_head=repr(skipped_content[:50]))
                jlog("be_parse_no_more_json_start_found", final_pos=idx); break
            if next_brace != -1 and (next_bracket == -1 or next_brace < next_bracket): next_json_start = next_brace
            elif next_bracket != -1: next_json_start = next_bracket
            else: break
            skipped_content = s_cleaned[idx:next_json_start]
            if len(skipped_content) > 5 and skipped_content.strip() and skipped_content.strip() != ',': jlog("be_parse_skip_to_next_json", skipped=repr(skipped_content[:50]))
            idx = next_json_start

        try:
            obj, end_idx = decoder.raw_decode(s_cleaned, idx)
            meta["segments_tried"] += 1; meta["decoder_advances"] += 1
            try: _collect_texts_robust(obj, texts, set())
            except Exception as collect_err: jlog("be_parse_collect_error", error=str(collect_err), obj_head=str(obj)[:100], at_pos=idx, error_type=type(collect_err).__name__)
            idx = end_idx

        except json.JSONDecodeError as json_err:
            meta["json_errors"] += 1; err_pos = json_err.pos + idx
            err_info = {"error": str(json_err), "location": f"pos {err_pos}", "near": repr(s_cleaned[err_pos:err_pos+50])}; parse_errors.append(err_info)
            next_brace = s_cleaned.find('{', err_pos + 1); next_bracket = s_cleaned.find('[', err_pos + 1)
            if next_brace == -1 and next_bracket == -1: break
            elif next_brace != -1 and (next_bracket == -1 or next_brace < next_bracket): idx = next_brace
            elif next_bracket != -1: idx = next_bracket
            else: break
        except StopIteration: jlog("be_parse_stop_iteration", at_pos=idx); break
        except Exception as general_err:
             meta["json_errors"] += 1; err_info = {"error": str(general_err), "location": f"pos {idx}", "type": type(general_err).__name__}; parse_errors.append(err_info)
             jlog("be_parse_loop_unexpected_error", **err_info);
             idx += 1

    if parse_errors: jlog("be_parse_json_errors_summary", count=len(parse_errors), errors=parse_errors[:3])

    out = _join_and_clean(texts)
    meta["matched"] = bool(out); meta["final_text_segments_found"] = len(texts)

    if not out and meta["segments_tried"] > 0:
         jlog("be_parse_no_valid_text_found", **meta)
    elif out:
         log_head = out[:100] + ('...' if len(out) > 100 else '')
         jlog("be_parse_output_snippet", head=log_head, final_len=len(out), original_segments=len(texts))

    return out, meta


class BEProducer:
    """BE producer acting only as fallback source."""
    # ... (code __init__, start, stop inchangé) ...
    def __init__(self, page: Page, on_progress: Callable[[str], None], on_done: Callable[[str, Optional[str]], None]) -> None:
        self.page = page
        self.on_progress_cb = on_progress
        self.seen: bool = False
        self.done: bool = False
        self._resp_handler = self._on_response

    async def start(self) -> None:
        if self.page.is_closed(): jlog("be_start_fail_page_closed"); return
        try: self.page.on("response", self._resp_handler); jlog("be_start_ok")
        except Exception as e: jlog("be_start_error", error=str(e), error_type=type(e).__name__)

    async def stop(self) -> None:
        try:
            if not self.page.is_closed():
                 if hasattr(self.page, "remove_listener"): self.page.remove_listener("response", self._resp_handler)
                 elif hasattr(self.page, "off"): self.page.off("response", self._resp_handler)
            jlog("be_stop_ok")
        except Exception as e: jlog("be_stop_remove_listener_error", error=str(e), error_type=type(e).__name__)
        self.seen = False

    async def _on_response(self, resp: Response) -> None:
        # ... (code _on_response inchangé, il appelle _parse_batchexecute_robust corrigé) ...
        bl_version = "unknown"; rpcids_seen = []; url = "unknown"
        try:
            url = resp.url or ""
            if not _BEX_URL_PAT.search(url): return
            self.seen = True

            try:
                bl_match = _BL_VERSION_PAT.search(url); bl_version = bl_match.group(1) if bl_match else "unknown"
                parsed_url = urlparse(url); query_params = parse_qs(parsed_url.query); rpcids_seen = query_params.get('rpcids', [])
            except Exception as url_parse_err: jlog("be_url_parse_warn", url=url, error=str(url_parse_err))

            status = resp.status
            if not (200 <= status < 300): jlog("be_skip_non_2xx", url=url, status=status); return

            try: body = await asyncio.wait_for(resp.text(), timeout=15.0)
            except asyncio.TimeoutError: jlog("be_resp_text_timeout", url=url); return
            except Exception as e: jlog("be_resp_text_error", url=url, error=str(e)); return
            if not body: jlog("be_skip_empty_body", url=url, status=status); return

            text, meta = _parse_batchexecute_robust(body)

            if text:
                final_text_size = len(text)
                jlog("be_progress_with_valid_text", size=final_text_size, bl_version=bl_version, rpcids_seen=rpcids_seen, head=text[:50])
                try:
                    self.on_progress_cb(text)
                except Exception as cb_err:
                    jlog("be_on_progress_callback_error", error=str(cb_err))
            else:
                 if meta.get("decoder_advances", 0) > 0:
                     jlog("be_parsed_but_no_valid_text", url=url, bl_version=bl_version, rpcids_seen=rpcids_seen, **meta)

        except Exception as e:
             if not self.page.is_closed():
                  jlog("be_on_response_unexpected_error", url=url, error=type(e).__name__, message=str(e))