# -*- coding: utf-8 -*-
"""
session_guardian.py — SessionGuardian Nasa++ (Critical Mode Extrême)

But
----
Fournir une vérification et une réparation **robustes** de la session,
sans jamais lever d'exception côté appelant. Toutes les méthodes
retournent des dictionnaires structurés et logguent en JSON.

Principes
---------
- `health(page=None)` doit **toujours** retourner un dict:
  {
    "ok": bool,
    "err": Optional[str],
    "page_closed": bool,
    "missing": List[str],        # cookies manquants (si applicable)
    "present": List[str],        # cookies présents (si applicable)
    "domain_sample": List[str],  # domaines inspectés (si applicable)
  }
- Tolère `page=None` ou page fermée.
- `repair_if_needed(page, ...)` ne jette jamais, loggue et renvoie un dict.
- Journalisation homogène (evt, ts, lvl).

Notes
-----
- Aucun reset automatique destructif ici. On se limite à des actions
  non-invasives (navigation, consent, reload). Si échec, on marque le
  profil pour reset via `mark_profile_for_reset(...)`.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

# Import "Page" uniquement pour l'analyse statique afin d'éviter
# "Variable not allowed in type expression" (Pylance/mypy).
if TYPE_CHECKING:  # pas d'import runtime (Playwright peut être absent)
    from playwright.async_api import Page


# ---------------------------------------------------------------------------
# Logging JSON homogène
# ---------------------------------------------------------------------------
def _jsonlog(logger: Any, level: str, payload: Dict[str, Any]) -> None:
    payload.setdefault("ts", time.time())
    payload.setdefault("lvl", level.upper())
    try:
        if logger and hasattr(logger, "info"):
            if payload["lvl"] == "INFO":
                logger.info(payload)
            elif payload["lvl"] == "WARNING" and hasattr(logger, "warning"):
                logger.warning(payload)
            elif payload["lvl"] == "ERROR" and hasattr(logger, "error"):
                logger.error(payload)
            else:
                print(json.dumps(payload, ensure_ascii=False))
        else:
            print(json.dumps(payload, ensure_ascii=False))
    except Exception:
        try:
            print(json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass


class SessionGuardian:
    """
    Guardian de session "safe-by-default".

    Paramètres
    ----------
    profile_root : Path
        Dossier racine du profil (ex: sandbox_profiles/user_{id})
    logger : Any
        Logger proxy compatible .info/.warning/.error(dict)
    """

    def __init__(self, profile_root: Path, logger: Any):
        self.profile_root = Path(profile_root)
        self.logger = logger

    # -----------------------------------------------------------------------
    # API publique
    # -----------------------------------------------------------------------
    async def health(
        self,
        page: Optional["Page"] = None,
        *,
        required_cookie_names: Optional[List[str]] = None,
        cookie_domains: Optional[List[str]] = None,
        timeout_s: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Vérifie l'état de la session. Ne jette **jamais**.

        - Supporte page=None.
        - Vérifie la présence de cookies critiques.
        """
        required = required_cookie_names or [
            "SAPISID", "SID", "__Secure-1PSID", "__Secure-3PSID"
        ]
        domains = cookie_domains or [
            ".google.com", "google.com", ".gemini.google.com", "gemini.google.com"
        ]

        out: Dict[str, Any] = {
            "ok": False,
            "err": None,
            "page_closed": False,
            "missing": [],
            "present": [],
            "domain_sample": domains[:],
        }

        # Cas page absente
        if page is None:
            out["err"] = "page_none"
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_health",
                "ok": False,
                "err": "page_none",
                "profile_dir": str(self.profile_root),
            })
            return out

        # Cas page fermée
        try:
            if hasattr(page, "is_closed") and page.is_closed():
                out["err"] = "page_closed"
                out["page_closed"] = True
                _jsonlog(self.logger, "WARNING", {
                    "evt": "session_health",
                    "ok": False,
                    "err": "page_closed",
                    "profile_dir": str(self.profile_root),
                })
                return out
        except Exception:
            # On continue: on traitera comme fermé
            out["err"] = "page_closed_check_failed"
            out["page_closed"] = True
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_health",
                "ok": False,
                "err": "page_closed_check_failed",
                "profile_dir": str(self.profile_root),
            })
            return out

        # Récupération cookies via context (non bloquant)
        try:
            ctx = getattr(page, "context", None)() if callable(getattr(page, "context", None)) else getattr(page, "context", None)
        except Exception:
            ctx = None

        if not ctx:
            out["err"] = "no_context"
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_health",
                "ok": False,
                "err": "no_context",
                "profile_dir": str(self.profile_root),
            })
            return out

        try:
            cookies = await ctx.cookies()
        except Exception as e:
            out["err"] = f"cookies_error: {e}"
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_health",
                "ok": False,
                "err": out["err"],
                "profile_dir": str(self.profile_root),
            })
            return out

        # Filtrer cookies par domaine et collecter les noms
        names_present: List[str] = []
        try:
            for c in cookies or []:
                try:
                    dom = (c.get("domain") if isinstance(c, dict) else getattr(c, "domain", "")) or ""
                    name = (c.get("name") if isinstance(c, dict) else getattr(c, "name", "")) or ""
                except Exception:
                    dom, name = "", ""
                if not name:
                    continue
                if any(d in (dom or "") for d in domains):
                    if name not in names_present:
                        names_present.append(name)
        except Exception:
            # Défensif: on n'échoue pas pour autant.
            pass

        missing = [n for n in required if n not in names_present]

        out["present"] = sorted(names_present)
        out["missing"] = missing
        out["ok"] = len(missing) == 0

        _jsonlog(self.logger, "INFO", {
            "evt": "session_health",
            "ok": out["ok"],
            "missing": missing,
            "present": out["present"][:8],  # échantillon
            "profile_dir": str(self.profile_root),
        })

        return out

    async def repair_if_needed(
        self,
        page: Optional["Page"],
        *,
        timeout_s: float = 30.0,
    ) -> Dict[str, Any]:
        """
        Tentative **non-invasive** de réparation si `health()` n'est pas OK.
        Ne jette **jamais**. Retourne:
        {
          "ok": bool,
          "attempted": bool,
          "err": Optional[str],
          "need_reset": bool
        }
        """
        result = {
            "ok": True,
            "attempted": False,
            "err": None,
            "need_reset": False,
        }

        # Vérification initiale
        h = await self.health(page)
        if h.get("ok", False):
            _jsonlog(self.logger, "INFO", {
                "evt": "session_repair_skip",
                "reason": "already_healthy",
                "profile_dir": str(self.profile_root),
            })
            return result

        result["ok"] = False
        result["attempted"] = True

        if page is None or (hasattr(page, "is_closed") and page.is_closed()):
            result["err"] = "page_none_or_closed"
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_repair_abort",
                "err": result["err"],
                "profile_dir": str(self.profile_root),
            })
            return result

        # Étape 1 : navigation vers l'app + consent best-effort
        try:
            await page.goto("https://gemini.google.com/app", wait_until="domcontentloaded", timeout=int(timeout_s * 1000))
            await _safe_sleep(0.6)
        except Exception as e:
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_repair_nav_error",
                "err": str(e),
                "profile_dir": str(self.profile_root),
            })

        # Consent best-effort (optionnel)
        try:
            from gemini_headless.utils.consent_detector import ConsentDetector  # type: ignore
            try:
                if hasattr(ConsentDetector, "handle_if_present"):
                    await ConsentDetector.handle_if_present(page, timeout_ms=10_000, retries=1)
                else:
                    cd = ConsentDetector(page, logger=self.logger)
                    await cd.skip_if_present(timeout_ms=10_000)
            except Exception as e:
                _jsonlog(self.logger, "WARNING", {
                    "evt": "session_repair_consent_error",
                    "err": str(e),
                    "profile_dir": str(self.profile_root),
                })
        except Exception:
            pass

        # Étape 2 : reload
        try:
            await page.reload(wait_until="domcontentloaded", timeout=int(timeout_s * 1000))
            await _safe_sleep(0.5)
        except Exception as e:
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_repair_reload_error",
                "err": str(e),
                "profile_dir": str(self.profile_root),
            })

        # Étape 3 : re-check
        h2 = await self.health(page)
        if not h2.get("ok", False):
            result["ok"] = False
            result["need_reset"] = True
            result["err"] = "cookies_missing_after_repair"
            await self.mark_profile_for_reset(reason=result["err"], details={"missing": h2.get("missing", [])})
            _jsonlog(self.logger, "WARNING", {
                "evt": "session_repair_need_reset",
                "profile_dir": str(self.profile_root),
                "missing": h2.get("missing", []),
            })
            return result

        # OK après réparation
        result["ok"] = True
        _jsonlog(self.logger, "INFO", {
            "evt": "session_repair_ok",
            "profile_dir": str(self.profile_root),
        })
        return result

    async def mark_profile_for_reset(self, reason: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Marque le profil comme nécessitant un reset (écrit un journal JSON).
        N'échoue jamais.
        """
        payload = {
            "ts": time.time(),
            "reason": reason,
            "details": details or {},
        }
        try:
            path = self.profile_root / ".reset_log.json"
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            _jsonlog(self.logger, "WARNING", {
                "evt": "profile_mark_reset",
                "profile_dir": str(self.profile_root),
                "reason": reason,
            })
        except Exception:
            # silencieux
            pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _safe_sleep(seconds: float) -> None:
    try:
        import asyncio
        await asyncio.sleep(max(0.0, float(seconds)))
    except Exception:
        pass
