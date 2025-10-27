# gemini_headless/collect/utils/logs.py
from __future__ import annotations
import sys, json, time

def jlog(evt: str, **payload) -> None:
    payload.setdefault("ts", time.time())
    try:
        sys.stderr.write(json.dumps({"evt": evt, **payload}, ensure_ascii=False, separators=(",", ":")) + "\n")
        sys.stderr.flush()
    except Exception:
        pass
