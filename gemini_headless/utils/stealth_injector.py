# utils/stealth_injector.py
# -----------------------------------------------------------------------------
# Stealth & fingerprint helpers for headless Playwright contexts/pages.
# - API fonctionnelle: inject_fingerprint(), apply_stealth()
# - Compatibilité: classe StealthInjector (shim) pour les anciens imports
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, Optional


# -----------------------------------------------------------------------------
# Helpers (safe no-op if fields are missing)
# -----------------------------------------------------------------------------

def _get(d: Optional[Dict[str, Any]], key: str, default=None):
    try:
        return (d or {}).get(key, default)
    except Exception:
        return default


# -----------------------------------------------------------------------------
# Context-level fingerprint injection
# Appelle ceci AVANT d’ouvrir une page si possible (user_agent, locale, tz, etc.).
# -----------------------------------------------------------------------------

async def inject_fingerprint(context, fingerprint: Optional[Dict[str, Any]] = None) -> None:
    """
    Configure un contexte Playwright à partir d’un "fingerprint" dict (optionnel).
    Champs supportés (tous optionnels):
      - user_agent: str
      - locale: str (ex: "fr-FR")
      - languages: list[str] (ex: ["fr-FR","fr"])
      - timezone_id: str (ex: "Europe/Paris")
      - viewport: {"width": int, "height": int}
      - geolocation: {"latitude": float, "longitude": float}
      - permissions: list[str] (ex: ["geolocation"])
      - device_scale_factor: float|int
      - is_mobile: bool
    """
    if context is None:
        return

    # Best-effort: certaines options ne sont prises en compte qu’à la création du contexte.
    # Ici on applique ce qui est modifiable dynamiquement.
    try:
        ua = _get(fingerprint, "user_agent")
        if ua:
            await context.set_extra_http_headers({"User-Agent": ua})
    except Exception:
        pass

    try:
        tz = _get(fingerprint, "timezone_id")
        if tz:
            # Playwright ne permet pas de changer le fuseau après création;
            # cependant certaines intégrations custom supportent ceci:
            try:
                await context.set_timezone_id(tz)  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        pass

    try:
        locale = _get(fingerprint, "locale")
        if locale:
            # Idem: si non supporté dynamiquement, on ignore silencieusement.
            try:
                await context.set_extra_http_headers({"Accept-Language": locale})
            except Exception:
                pass
    except Exception:
        pass

    # Permissions et géolocalisation si la plateforme les supporte dynamiquement
    try:
        geoloc = _get(fingerprint, "geolocation")
        if geoloc:
            try:
                await context.set_geolocation(geoloc)  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        pass

    try:
        perms = _get(fingerprint, "permissions")
        if perms:
            try:
                await context.grant_permissions(perms)  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Page-level stealth: patches JS (navigator, plugins, webdriver, WebGL, canvas…)
# Appelle ceci APRES l’ouverture de la page (avant navigation si possible).
# -----------------------------------------------------------------------------

async def apply_stealth(page, fingerprint: Optional[Dict[str, Any]] = None, config: Optional[Dict[str, Any]] = None) -> None:
    """
    Injecte des patches furtifs côté page. Sûr et idempotent.
    """
    if page is None:
        return

    # (1) UA override si fourni (en header via context + JS navigator)
    ua = _get(fingerprint, "user_agent")
    nav_langs = _get(fingerprint, "languages") or []
    locale = _get(fingerprint, "locale") or (_get(fingerprint, "language") or "en-US")
    device_scale_factor = _get(fingerprint, "device_scale_factor")
    is_mobile = bool(_get(fingerprint, "is_mobile", False))

    # (2) Viewport si présent
    viewport = _get(fingerprint, "viewport")
    try:
        if viewport and isinstance(viewport, dict) and "width" in viewport and "height" in viewport:
            await page.set_viewport_size({"width": int(viewport["width"]), "height": int(viewport["height"])})
    except Exception:
        pass

    # (3) Patches JS: webdriver, plugins, languages, WebGL, canvas, permissions
    js = f"""
(() => {{
  try {{
    // navigator.webdriver -> false
    Object.defineProperty(navigator, 'webdriver', {{ get: () => false }});

    // languages
    try {{
      const langs = {nav_langs!r};
      if (Array.isArray(langs) && langs.length) {{
        Object.defineProperty(navigator, 'languages', {{ get: () => langs }});
      }}
    }} catch (e) {{}}

    // language
    try {{
      const lang = {locale!r};
      if (lang) {{
        Object.defineProperty(navigator, 'language', {{ get: () => lang }});
      }}
    }} catch (e) {{}}

    // userAgent
    try {{
      const ua = {ua!r};
      if (ua) {{
        Object.defineProperty(navigator, 'userAgent', {{ get: () => ua }});
      }}
    }} catch (e) {{}}

    // plugins length > 0
    try {{
      const fakePlugins = [{{name: 'Chrome PDF Viewer'}}, {{name: 'PDF Viewer'}}, {{name: 'Native Client'}}];
      Object.defineProperty(navigator, 'plugins', {{
        get: () => fakePlugins
      }});
    }} catch (e) {{}}

    // platform / maxTouchPoints
    try {{
      const isMobile = {str(is_mobile).lower()};
      if (isMobile) {{
        Object.defineProperty(navigator, 'platform', {{ get: () => 'Linux armv8l' }});
        Object.defineProperty(navigator, 'maxTouchPoints', {{ get: () => 5 }});
      }} else {{
        Object.defineProperty(navigator, 'platform', {{ get: () => 'Win32' }});
        Object.defineProperty(navigator, 'maxTouchPoints', {{ get: () => 0 }});
      }}
    }} catch (e) {{}}

    // devicePixelRatio
    try {{
      const dpr = {device_scale_factor if device_scale_factor is not None else 'null'};
      if (dpr) {{
        Object.defineProperty(window, 'devicePixelRatio', {{ get: () => dpr }});
      }}
    }} catch (e) {{}}

    // Canvas noise
    try {{
      const addNoise = (c) => {{
        const ctx = c.getContext('2d');
        const w = c.width, h = c.height;
        const id = ctx.getImageData(0,0,w,h);
        for (let i = 0; i < id.data.length; i += 4) {{
          id.data[i+0] += 1;
          id.data[i+1] += 1;
          id.data[i+2] += 1;
        }}
        ctx.putImageData(id,0,0);
      }};
      const toBlob = HTMLCanvasElement.prototype.toBlob;
      const toDataURL = HTMLCanvasElement.prototype.toDataURL;
      HTMLCanvasElement.prototype.toDataURL = function(...args) {{
        addNoise(this);
        return toDataURL.apply(this, args);
      }};
      HTMLCanvasElement.prototype.toBlob = function(...args) {{
        addNoise(this);
        return toBlob.apply(this, args);
      }};
    }} catch (e) {{}}

    // WebGL spoof
    try {{
      const getParameter = WebGLRenderingContext.prototype.getParameter;
      WebGLRenderingContext.prototype.getParameter = function(param) {{
        if (param === 37445) return 'Intel Open Source Technology Center'; // UNMASKED_VENDOR_WEBGL
        if (param === 37446) return 'ANGLE (Intel, Intel(R) HD Graphics, D3D11)';
        return getParameter.call(this, param);
      }};
    }} catch (e) {{}}

    // Permissions (notifications) – éviter 'denied' par défaut
    try {{
      const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
      if (originalQuery) {{
        window.navigator.permissions.query = (parameters) => (
          parameters && parameters.name === 'notifications'
            ? Promise.resolve({{ state: Notification.permission }})
            : originalQuery(parameters)
        );
      }}
    }} catch (e) {{}}
  }} catch (e) {{
    // Silence total — stealth must never break the page
  }}
}})();
"""
    try:
        await page.add_init_script(js)
    except Exception:
        # Si add_init_script échoue (rare), essaye au moins d’évaluer après navigation.
        try:
            await page.evaluate(js)
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Aliases historiques (si ton code existant appelle d’autres noms)
# -----------------------------------------------------------------------------

async def enable_stealth(page, fingerprint: Optional[Dict[str, Any]] = None, config: Optional[Dict[str, Any]] = None) -> None:
    await apply_stealth(page, fingerprint=fingerprint, config=config)


async def apply_fingerprint(context, fingerprint: Optional[Dict[str, Any]] = None) -> None:
    await inject_fingerprint(context, fingerprint=fingerprint)


# -----------------------------------------------------------------------------
# Compatibility shim for connectors expecting `StealthInjector`
# (ne fait rien si une classe du même nom est déjà définie ailleurs dans ce fichier)
# -----------------------------------------------------------------------------

try:
    StealthInjector  # type: ignore  # noqa: F401
except NameError:

    class StealthInjector:
        """
        Wrapper minimal attendu par:
            from utils.stealth_injector import StealthInjector

        Usage:
            s = StealthInjector(fingerprint=fp, config=cfg)
            await s.apply(context=ctx, page=page)
        """
        def __init__(self, fingerprint: Optional[Dict[str, Any]] = None, config: Optional[Dict[str, Any]] = None):
            self.fingerprint = fingerprint or {}
            self.config = config or {}

        async def apply(self, context=None, page=None):
            # Ordre: d'abord contexte (headers/UA…), puis patchs page (navigator/webgl/canvas…)
            try:
                await inject_fingerprint(context, self.fingerprint)
            except Exception:
                pass
            try:
                await apply_stealth(page, fingerprint=self.fingerprint, config=self.config)
            except Exception:
                pass

    __all__ = ("StealthInjector", "inject_fingerprint", "apply_stealth", "enable_stealth", "apply_fingerprint")
