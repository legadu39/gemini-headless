# utils/stealth_injector.py
# -----------------------------------------------------------------------------
# Stealth & fingerprint helpers for headless Playwright contexts/pages.
# - API fonctionnelle: inject_fingerprint(), apply_stealth()
# - Compatibilité: classe StealthInjector (shim) pour les anciens imports
# -----------------------------------------------------------------------------

from __future__ import annotations

import json # Added json import for JS serialization
from typing import Any, Dict, Optional
import logging # Use standard logging

logger = logging.getLogger(__name__) # Setup logger for this module

# -----------------------------------------------------------------------------
# Helpers (safe no-op if fields are missing)
# -----------------------------------------------------------------------------

def _get(d: Optional[Dict[str, Any]], key: str, default=None):
    """Safely get a value from a dictionary."""
    try:
        # Check if d is dict-like and key exists
        if hasattr(d, 'get') and callable(d.get):
            return d.get(key, default)
        # Handle cases where d might be None or not a dict
        return default
    except Exception as e:
        # Log unexpected errors during get operation
        logger.debug(f"Error getting key '{key}': {e}", exc_info=False)
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
        logger.warning("inject_fingerprint called with context=None")
        return
    if fingerprint is None:
        fingerprint = {} # Use empty dict if None

    # Best-effort: certaines options ne sont prises en compte qu’à la création du contexte.
    # Ici on applique ce qui est modifiable dynamiquement.

    # User-Agent Header
    try:
        ua = _get(fingerprint, "user_agent")
        if ua and isinstance(ua, str):
            await context.set_extra_http_headers({"User-Agent": ua})
        elif ua:
             logger.warning(f"Invalid user_agent type: {type(ua)}. Expected str.")
    except Exception as e:
        logger.warning(f"Failed to set User-Agent header: {e}", exc_info=False)

    # Timezone (Often requires context creation option, try dynamic if available)
    try:
        tz = _get(fingerprint, "timezone_id")
        if tz and isinstance(tz, str):
            # Playwright's standard API doesn't allow changing timezone after creation.
            # This might work with custom builds or specific browser versions.
            if hasattr(context, "set_timezone_id") and callable(context.set_timezone_id):
                try:
                    await context.set_timezone_id(tz) # type: ignore[attr-defined]
                except Exception as e_tz:
                    logger.debug(f"Dynamic set_timezone_id failed (may require creation option): {e_tz}", exc_info=False)
            else:
                 logger.debug("Context does not support dynamic set_timezone_id.")
        elif tz:
             logger.warning(f"Invalid timezone_id type: {type(tz)}. Expected str.")
    except Exception as e:
        logger.warning(f"Error processing timezone_id: {e}", exc_info=False)


    # Locale / Accept-Language Header
    try:
        # Prefer 'locale' if present, fallback to 'language'
        locale = _get(fingerprint, "locale") or _get(fingerprint, "language")
        if locale and isinstance(locale, str):
            await context.set_extra_http_headers({"Accept-Language": locale})
        elif locale:
            logger.warning(f"Invalid locale/language type: {type(locale)}. Expected str.")

        # Potentially set languages header too if provided and different
        languages = _get(fingerprint, "languages")
        if languages and isinstance(languages, list):
             # Format languages list correctly for header (e.g., "fr-FR,fr;q=0.9,en;q=0.8")
             # Simple join for now, browser might handle quality factors automatically
             lang_header = ",".join(filter(lambda x: isinstance(x, str), languages))
             if lang_header and lang_header != locale: # Avoid redundant header if same as locale
                # Note: This might override or conflict with the single locale header depending on browser behavior.
                # Setting only Accept-Language based on 'locale' might be safer.
                # await context.set_extra_http_headers({"Accept-Language": lang_header})
                 pass # Sticking with single locale for simplicity now
        elif languages:
             logger.warning(f"Invalid languages type: {type(languages)}. Expected list.")

    except Exception as e:
        logger.warning(f"Failed to set Accept-Language header: {e}", exc_info=False)


    # Geolocation (requires context creation option in standard Playwright)
    try:
        geoloc = _get(fingerprint, "geolocation")
        if geoloc and isinstance(geoloc, dict):
            # Dynamic setting might not be supported by standard Playwright context
            if hasattr(context, "set_geolocation") and callable(context.set_geolocation):
                try:
                    # Validate keys and types before calling
                    if 'latitude' in geoloc and 'longitude' in geoloc and \
                       isinstance(geoloc['latitude'], (int, float)) and \
                       isinstance(geoloc['longitude'], (int, float)):
                        await context.set_geolocation(geoloc) # type: ignore[attr-defined]
                    else:
                        logger.warning(f"Invalid geolocation format: {geoloc}. Requires dict with latitude/longitude numbers.")
                except Exception as e_geo:
                    logger.debug(f"Dynamic set_geolocation failed (may require creation option): {e_geo}", exc_info=False)
            else:
                 logger.debug("Context does not support dynamic set_geolocation.")
        elif geoloc:
             logger.warning(f"Invalid geolocation type: {type(geoloc)}. Expected dict.")
    except Exception as e:
        logger.warning(f"Error processing geolocation: {e}", exc_info=False)

    # Permissions (requires context creation option in standard Playwright)
    try:
        perms = _get(fingerprint, "permissions")
        if perms and isinstance(perms, list):
             if hasattr(context, "grant_permissions") and callable(context.grant_permissions):
                try:
                    # Filter to ensure only strings are passed
                    valid_perms = [p for p in perms if isinstance(p, str)]
                    if valid_perms:
                        await context.grant_permissions(valid_perms) # type: ignore[attr-defined]
                    if len(valid_perms) != len(perms):
                        logger.warning(f"Some invalid permission types found in list: {perms}")
                except Exception as e_perm:
                    logger.debug(f"Dynamic grant_permissions failed (may require creation option): {e_perm}", exc_info=False)
             else:
                  logger.debug("Context does not support dynamic grant_permissions.")
        elif perms:
             logger.warning(f"Invalid permissions type: {type(perms)}. Expected list.")
    except Exception as e:
        logger.warning(f"Error processing permissions: {e}", exc_info=False)


# -----------------------------------------------------------------------------
# Page-level stealth: patches JS (navigator, plugins, webdriver, WebGL, canvas…)
# Appelle ceci APRES l’ouverture de la page (avant navigation si possible via add_init_script).
# -----------------------------------------------------------------------------

async def apply_stealth(page, fingerprint: Optional[Dict[str, Any]] = None, config: Optional[Dict[str, Any]] = None) -> None:
    """
    Injecte des patches furtifs côté page via add_init_script. Sûr et idempotent.
    """
    if page is None:
        logger.warning("apply_stealth called with page=None")
        return
    if page.is_closed():
        logger.warning("apply_stealth called on closed page")
        return
    if fingerprint is None:
        fingerprint = {} # Use empty dict if None
    # Config is currently unused, but kept for signature compatibility
    _ = config

    # --- Prepare values for JS injection ---
    # Ensure values are JSON serializable and handle None cases
    ua = _get(fingerprint, "user_agent") or ""
    # Use 'languages' if available, otherwise construct from 'locale' or 'language'
    nav_langs_raw = _get(fingerprint, "languages")
    if isinstance(nav_langs_raw, list) and nav_langs_raw:
        nav_langs = json.dumps([str(lang) for lang in nav_langs_raw if isinstance(lang, str)])
    else:
        locale_lang = _get(fingerprint, "locale") or _get(fingerprint, "language") or "en-US"
        # Create a basic languages list from locale if languages not provided
        nav_langs = json.dumps([str(locale_lang)] + ([locale_lang.split('-')[0]] if '-' in locale_lang else []))

    locale = _get(fingerprint, "locale") or _get(fingerprint, "language") or "en-US"
    # Ensure dpr is number or null for JS
    dpr_raw = _get(fingerprint, "device_scale_factor")
    device_scale_factor = float(dpr_raw) if isinstance(dpr_raw, (int, float)) else 'null'

    is_mobile = str(bool(_get(fingerprint, "is_mobile", False))).lower() # JS boolean 'true' or 'false'
    # WebGL Spoofing values (consider making these configurable via fingerprint)
    webgl_vendor = _get(fingerprint, "webgl_vendor", "Intel Inc.") # Simplified default
    webgl_renderer = _get(fingerprint, "renderer", "ANGLE (Intel)") # Simplified default
    platform = _get(fingerprint, "platform", "Win32") # Get platform from fingerprint

    # (2) Viewport - Applied AFTER init script, as it affects the page directly
    try:
        viewport = _get(fingerprint, "viewport")
        if viewport and isinstance(viewport, dict) and "width" in viewport and "height" in viewport:
             # Ensure width/height are integers
             vp_width = int(_get(viewport, "width", 1366))
             vp_height = int(_get(viewport, "height", 768))
             await page.set_viewport_size({"width": vp_width, "height": vp_height})
        elif viewport and isinstance(viewport, (list, tuple)) and len(viewport) == 2:
             # Support for list/tuple format [width, height]
             vp_width = int(viewport[0])
             vp_height = int(viewport[1])
             await page.set_viewport_size({"width": vp_width, "height": vp_height})
        elif viewport:
             logger.warning(f"Invalid viewport format: {viewport}. Expected dict {{'width': w, 'height': h}} or list/tuple [w, h].")
    except Exception as e:
        logger.warning(f"Failed to set viewport size: {e}", exc_info=False)


    # (3) Patches JS via add_init_script (runs on every navigation BEFORE page scripts)
    # Using f-string formatting with json.dumps for safe JS literal injection
    js = f"""
(() => {{
  try {{
    // === Webdriver Flag ===
    if (navigator.webdriver === true) {{
      Object.defineProperty(navigator, 'webdriver', {{ get: () => false }});
    }}

    // === Languages ===
    try {{
      const langs = {nav_langs}; // Injected as JSON array string
      if (Array.isArray(langs) && langs.length) {{
        Object.defineProperty(navigator, 'languages', {{ get: () => langs, configurable: true }});
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to set navigator.languages', e); }}

    // === Language ===
    try {{
      const lang = {json.dumps(locale)}; // Injected as JSON string
      if (lang) {{
        Object.defineProperty(navigator, 'language', {{ get: () => lang, configurable: true }});
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to set navigator.language', e); }}

    // === User Agent ===
    try {{
      const ua = {json.dumps(ua)}; // Injected as JSON string
      if (ua && navigator.userAgent !== ua) {{ // Avoid overriding if already correct
        Object.defineProperty(navigator, 'userAgent', {{ get: () => ua, configurable: true }});
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to set navigator.userAgent', e); }}

    // === Plugins Mimicry ===
    try {{
      if (navigator.plugins === undefined || navigator.plugins.length === 0) {{
          const fakePlugins = [
            Object.freeze({{ name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format', mimeTypes: [Object.freeze({{ type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' }})] }}),
            Object.freeze({{ name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '', mimeTypes: [Object.freeze({{ type: 'application/pdf', suffixes: 'pdf', description: ''}})] }}),
            Object.freeze({{ name: 'Native Client', filename: 'internal-nacl-plugin', description: '', mimeTypes: [Object.freeze({{ type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' }}), Object.freeze({{ type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }})] }})
          ];
          // Use defineProperty for better stealth than direct assignment
           Object.defineProperty(navigator, 'plugins', {{
                get: () => {{
                    // Make it behave like a PluginArray
                    fakePlugins.item = (index) => fakePlugins[index];
                    fakePlugins.namedItem = (name) => fakePlugins.find(p => p.name === name) || null;
                    fakePlugins.refresh = () => {{}};
                    return Object.freeze(fakePlugins);
                }},
                configurable: true
           }});
           // Mimic MimeTypeArray as well
            Object.defineProperty(navigator, 'mimeTypes', {{
                get: () => {{
                    const mimeTypes = Object.freeze(fakePlugins.flatMap(p => p.mimeTypes));
                    mimeTypes.item = (index) => mimeTypes[index];
                    mimeTypes.namedItem = (name) => mimeTypes.find(m => m.type === name) || null;
                    return Object.freeze(mimeTypes);
                }},
                configurable: true
            }});
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to mimic plugins', e); }}

    // === Platform & Touch Points ===
    try {{
      const isMobile = {is_mobile}; // Injected as 'true' or 'false'
      const platformValue = isMobile ? 'Linux armv8l' : {json.dumps(platform)}; // Use fingerprint platform or Win32 default
      const maxTouchPointsValue = isMobile ? 5 : 0;

      if(navigator.platform !== platformValue) {{
          Object.defineProperty(navigator, 'platform', {{ get: () => platformValue, configurable: true }});
      }}
      if(navigator.maxTouchPoints !== maxTouchPointsValue) {{
         Object.defineProperty(navigator, 'maxTouchPoints', {{ get: () => maxTouchPointsValue, configurable: true }});
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to set platform/maxTouchPoints', e); }}

    // === Device Pixel Ratio ===
    try {{
      const dpr = {device_scale_factor}; // Injected as number or 'null'
      if (dpr !== null && window.devicePixelRatio !== dpr) {{
        Object.defineProperty(window, 'devicePixelRatio', {{ get: () => dpr, configurable: true }});
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to set devicePixelRatio', e); }}

    // === Canvas Noise ===
    // Adds subtle noise to canvas data to disrupt fingerprinting
    try {{
        const R = (context, method) => {{
            const S = context.canvas.toDataURL;
            context.canvas.toDataURL = function() {{ return S.apply(context.canvas, arguments); }}; // Temporarily restore original
            const M = context[method].apply(context, arguments); // Call original method
            context.canvas.toDataURL = S; // Re-apply our hook
            return M;
        }};
        const addNoise = (canvas) => {{
            if (!canvas) return;
            try {{
                const ctx = canvas.getContext('2d');
                if (!ctx) return;
                const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
                const data = imageData.data;
                for (let i = 0; i < data.length; i += 4) {{
                    const noise = Math.floor(Math.random() * 3) -1; // -1, 0, or 1
                    data[i] = Math.max(0, Math.min(255, data[i] + noise));     // R
                    data[i + 1] = Math.max(0, Math.min(255, data[i + 1] + noise)); // G
                    data[i + 2] = Math.max(0, Math.min(255, data[i + 2] + noise)); // B
                    // Alpha (data[i + 3]) is usually left unchanged
                }}
                ctx.putImageData(imageData, 0, 0);
            }} catch (e) {{ /* Ignore context errors */ }}
        }};

        // Hook toDataURL
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(...args) {{
            try {{ addNoise(this); }} catch (e) {{ console.warn("Canvas noise failed for toDataURL", e); }}
            return originalToDataURL.apply(this, args);
        }};

        // Hook toBlob
        const originalToBlob = HTMLCanvasElement.prototype.toBlob;
        HTMLCanvasElement.prototype.toBlob = function(...args) {{
            try {{ addNoise(this); }} catch (e) {{ console.warn("Canvas noise failed for toBlob", e); }}
            return originalToBlob.apply(this, args);
        }};

        // Hook getImageData
        // const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
        // CanvasRenderingContext2D.prototype.getImageData = function(...args) {{
        //     const imageData = originalGetImageData.apply(this, args);
        //     // Optionally add noise here too, but might be redundant if toDataURL/toBlob are hooked
        //     return imageData;
        // }};

    }} catch (e) {{ console.warn('Stealth: Failed to apply canvas noise', e); }}


    // === WebGL Spoofing ===
    try {{
      const config = {{
          'webglVendor': {json.dumps(webgl_vendor)},
          'webglRenderer': {json.dumps(webgl_renderer)}
      }};
      const getParameterProxy = function(originalGetParameter) {{
          return function(parameter) {{
              // UNMASKED_VENDOR_WEBGL (37445)
              if (parameter === 37445 && config.webglVendor) {{ return config.webglVendor; }}
              // UNMASKED_RENDERER_WEBGL (37446)
              if (parameter === 37446 && config.webglRenderer) {{ return config.webglRenderer; }}
              return originalGetParameter.call(this, parameter);
          }};
      }};

      // --- Apply to WebGLRenderingContext ---
      if (typeof WebGLRenderingContext !== 'undefined' && WebGLRenderingContext.prototype.getParameter) {{
          const originalGetParameterWebGL = WebGLRenderingContext.prototype.getParameter;
          WebGLRenderingContext.prototype.getParameter = getParameterProxy(originalGetParameterWebGL);
      }}
      // --- Apply to WebGL2RenderingContext ---
      if (typeof WebGL2RenderingContext !== 'undefined' && WebGL2RenderingContext.prototype.getParameter) {{
           const originalGetParameterWebGL2 = WebGL2RenderingContext.prototype.getParameter;
           WebGL2RenderingContext.prototype.getParameter = getParameterProxy(originalGetParameterWebGL2);
      }}

    }} catch (e) {{ console.warn('Stealth: Failed to spoof WebGL', e); }}


    // === Permissions API Hook (Notifications) ===
    // Prevents immediate 'denied' state for notifications permission query
    try {{
      if (navigator.permissions && navigator.permissions.query) {{
          const originalQuery = navigator.permissions.query;
          // Use a function declaration for 'this' context preservation if needed (though not strictly here)
          navigator.permissions.query = function query(parameters) {{
            // Check for notifications permission specifically
            if (parameters && parameters.name === 'notifications') {{
                // Return a Promise resolving to a state based on Notification.permission
                // Defaulting to 'prompt' or 'granted' instead of 'denied' if Notification.permission is 'default'
                const resolvedState = Notification.permission === 'denied' ? 'denied' : (Notification.permission === 'granted' ? 'granted' : 'prompt');
                return Promise.resolve({{ state: resolvedState, name: 'notifications', onchange: null }});
            }}
            // For other permissions, call the original query method
            // Need to ensure 'this' context is correct, using apply/call
            return originalQuery.apply(navigator.permissions, arguments);
          }};
      }}
    }} catch (e) {{ console.warn('Stealth: Failed to hook permissions API', e); }}

    // === Console Debug Hook (Less common, optional) ===
    // Detects if devtools are open by checking execution time of console.debug
    // try {
    //   let devtoolsOpen = false;
    //   const threshold = 160; // Milliseconds threshold
    //   const check = () => {
    //     const start = performance.now();
    //     console.debug(''); // Execution time varies significantly if devtools are open
    //     const duration = performance.now() - start;
    //     devtoolsOpen = duration > threshold;
    //   };
    //   // Check periodically
    //   // setInterval(check, 1000);
    //   // You might expose devtoolsOpen via a property if needed by other scripts
    //   // Object.defineProperty(window, '__devtoolsOpen', { get: () => devtoolsOpen });
    // } catch(e) {}

  }} catch (e) {{
    // Global catch for the entire stealth script - should not happen often
    console.error('Stealth Master Error:', e);
  }}
}})();
"""
    try:
        # Add the script to run at the beginning of document creation
        await page.add_init_script(js)
    except Exception as e:
        logger.error(f"Failed to add init script for stealth patches: {e}", exc_info=True)
        # Fallback: Try evaluating immediately, though less effective
        try:
            await page.evaluate(js)
            logger.warning("Applied stealth patches via evaluate() as fallback.")
        except Exception as e_eval:
            logger.error(f"Failed to apply stealth patches via evaluate() fallback: {e_eval}", exc_info=True)


# -----------------------------------------------------------------------------
# Aliases historiques (si ton code existant appelle d’autres noms)
# -----------------------------------------------------------------------------

async def enable_stealth(page, fingerprint: Optional[Dict[str, Any]] = None, config: Optional[Dict[str, Any]] = None) -> None:
    """Alias for apply_stealth."""
    await apply_stealth(page, fingerprint=fingerprint, config=config)


async def apply_fingerprint(context, fingerprint: Optional[Dict[str, Any]] = None) -> None:
    """Alias for inject_fingerprint."""
    await inject_fingerprint(context, fingerprint=fingerprint)


# -----------------------------------------------------------------------------
# Compatibility shim for connectors expecting `StealthInjector`
# (ne fait rien si une classe du même nom est déjà définie ailleurs dans ce fichier)
# -----------------------------------------------------------------------------

try:
    StealthInjector # type: ignore # noqa: F401 # Check if already defined
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
            logger.debug("StealthInjector shim initialized.")

        async def apply(self, context=None, page=None):
            """Applies fingerprint to context and stealth patches to page."""
            logger.debug(f"StealthInjector apply called with context: {bool(context)}, page: {bool(page)}")
            # Ordre: d'abord contexte (headers/UA…), puis patchs page (navigator/webgl/canvas…)
            if context:
                try:
                    await inject_fingerprint(context, self.fingerprint)
                except Exception as e:
                     logger.error(f"StealthInjector: Error in inject_fingerprint: {e}", exc_info=True)

            if page:
                try:
                    await apply_stealth(page, fingerprint=self.fingerprint, config=self.config)
                except Exception as e:
                     logger.error(f"StealthInjector: Error in apply_stealth: {e}", exc_info=True)

    # Export names for discoverability if shim is created
    __all__ = ("StealthInjector", "inject_fingerprint", "apply_stealth", "enable_stealth", "apply_fingerprint")