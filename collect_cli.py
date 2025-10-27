# collect_cli.py
# -*- coding: utf-8 -*-
# VERSION 7.11-IntelliFix (Stagnation V1 + Enhanced Probe + Fixes)
# CHANGEMENTS CLEFS :
# - FIX: profile.dir -> Path(profile.profile_dir) (upload cache)
# - FIX: attente login/debug (wait_for_event("close") sans timeout 0)
# - FIX: invalid responses -> détection par sous-chaîne (pas égalité stricte)
# - FIX: selectors Playwright invalides remplacés (plus réalistes)
# - FIX: activity probe applique ACTIVITY_PROBE_TIMEOUT_MS via asyncio.wait_for
# - FIX: logs version homogénéisés en 7.11
# - FIX: phase exhaustive import ne recalcul plus count plusieurs fois
# - ADD: SIGNAL_DIR lisible via env GH_SIGNAL_DIR (fallback Windows)
# - ADD: Headless par défaut si non-login/non-debug, sauf override YAML
# - ADD: Guards & observations supplémentaires (portabilité, stabilité)

from __future__ import annotations
import argparse
import asyncio
import os
import sys
import re
import time
import traceback
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# --- Imports (robustes) ---
try:
    from gemini_headless.collect.orchestrator import Orchestrator
    from gemini_headless.collect.utils.logs import jlog
    from gemini_headless.utils.sandbox_profile import SandboxProfile
    from gemini_headless.utils.session_guardian import SessionGuardian
    from gemini_headless.utils.fingerprint import Fingerprint, build_launch_args
    from gemini_headless.utils.stealth_injector import apply_stealth
    from gemini_headless.connectors.input_and_session import fast_send_prompt
    from gemini_headless.connectors.input_and_session import _SELECTORS_PREF_V3 as INPUT_BOX_SELECTORS
    from gemini_headless.collect.filters.cleaner import clean_text_with_stats
    # Import du JS du DOMProducer pour le Last Gasp (V7.11)
    from gemini_headless.collect.producers.dom import _GET_BEST_TEXT_JS
    from playwright.async_api import (
        async_playwright, Page, Error as PlaywrightError, BrowserContext,
        FileChooser, TimeoutError as PWTimeoutError, Locator
    )
    jlog("imports_successful", level="INFO")
except ImportError as e:
    error_details = f"FATAL: Erreur d'importation: {e}. Vérifiez l'installation, PYTHONPATH et l'exécution dans le bon environnement."
    print(f"--- collect_cli.py import error ---", file=sys.stderr, flush=True)
    try:
        jlog("import_error_fatal", error=str(e), details=error_details, level="CRITICAL")
    except NameError:
        pass
    sys.exit(1)

# --- Constantes & configuration légère ---
UPLOAD_BEHAVIOR_CACHE_FILE = ".upload_selectors_cache_v2.json"

# Sélecteurs plus réalistes / compatibles Playwright
DEFAULT_PLUS_BUTTON_SELECTORS = [
    'button[aria-label*="joindre" i][aria-label*="fichier" i]',
    'button[aria-label*="attach" i][aria-label*="file" i]',
    'button:has-text("+")',
    'button:has-text("Ajouter")',
    'button:has-text("Add")',
    'button:has(svg[aria-label*="add" i])',
    'button:near(div[role="textbox"], 75):visible',
]
DEFAULT_IMPORT_OPTION_SELECTORS = [
    'button:has-text("Importer des fichiers")',
    'button[aria-label*="Importer des fichiers" i]',
    '[role="menuitem"]:has-text("Importer des fichiers")',
    'button:has-text("Import files")',
    'button[aria-label*="Import files" i]',
    '[role="menuitem"]:has-text("Import files")',
]

failure_reason = "unknown_init"

# Portabilité : lis depuis l'env d'abord, sinon fallback Windows
SIGNAL_DIR = Path(os.getenv("GH_SIGNAL_DIR", r"C:\Users\Mathieu\Desktop\bot tiktok vdeux\var\signals"))
LAST_GASP_SNAPSHOT_START_MARKER = "__LAST_GASP_SNAPSHOT_START__"
LAST_GASP_SNAPSHOT_END_MARKER = "__LAST_GASP_SNAPSHOT_END__"
SNAPSHOT_REQUEST_TIMEOUT_S: float = 4.5  # Gardé court
ACTIVITY_PROBE_INTERVAL_S: float = float(os.getenv("GH_ACTIVITY_PROBE_INTERVAL_S", "15.0"))
ACTIVITY_PROBE_TIMEOUT_MS: int = int(os.getenv("GH_ACTIVITY_PROBE_TIMEOUT_MS", "5000"))

# --- Liste noire réponses invalides (tout en minuscules, on teste par inclusion) ---
INVALID_GEMINI_RESPONSES = {
    "vous avez interrompu cette réponse", "you stopped this answer",
    "une erreur s'est produite", "an error occurred",
    "impossible de traiter la demande", "unable to process the request",
    "veuillez patienter", "please wait",
    "comment puis-je vous aider", "how can i help today",
}

# --- get_browser_executable_path ---
def get_browser_executable_path() -> Optional[str]:
    program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
    program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")
    local_app_data = os.environ.get("LOCALAPPDATA", "")
    potential_paths_win = [
        Path(program_files, "Google", "Chrome", "Application", "chrome.exe"),
        Path(program_files_x86, "Google", "Chrome", "Application", "chrome.exe"),
        Path(local_app_data, "Google", "Chrome", "Application", "chrome.exe"),
        Path(program_files, "Microsoft", "Edge", "Application", "msedge.exe"),
        Path(program_files_x86, "Microsoft", "Edge", "Application", "msedge.exe"),
        Path(local_app_data, "Microsoft", "Edge", "Application", "msedge.exe"),
        Path(program_files, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        Path(program_files_x86, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
    ]
    potential_paths_linux = [
        Path("/usr/bin/google-chrome-stable"),
        Path("/usr/bin/google-chrome"),
        Path("/usr/bin/chromium-browser"),
        Path("/usr/bin/chromium"),
        Path("/snap/bin/chromium"),
        Path("/usr/bin/microsoft-edge-stable"),
        Path("/usr/bin/microsoft-edge"),
        Path("/usr/bin/brave-browser-stable"),
        Path("/usr/bin/brave-browser"),
    ]
    potential_paths_mac = [
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
        Path("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"),
        Path(os.path.expanduser("~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")),
        Path(os.path.expanduser("~/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge")),
        Path(os.path.expanduser("~/Applications/Brave Browser.app/Contents/MacOS/Brave Browser")),
    ]
    if sys.platform == "win32":
        candidates = potential_paths_win
    elif sys.platform == "darwin":
        candidates = potential_paths_mac
    else:
        candidates = potential_paths_linux
    for path in candidates:
        try:
            if path.exists() and path.is_file():
                jlog("browser_found", path=str(path), level="INFO")
                return str(path)
        except OSError:
            pass
    jlog("browser_not_found", paths_checked=len(candidates), platform=sys.platform, level="CRITICAL")
    return None

# --- Fonctions Cache Comportemental ---
def load_behavior_cache(profile_dir: Path) -> Tuple[Optional[str], Optional[str]]:
    cache_path = profile_dir / UPLOAD_BEHAVIOR_CACHE_FILE
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            plus_selector = data.get("plus_button_selector")
            import_selector = data.get("import_option_selector")
            if isinstance(plus_selector, str) and isinstance(import_selector, str):
                jlog("behavior_cache_loaded", plus_selector=plus_selector, import_selector=import_selector, level="INFO")
                return plus_selector, import_selector
        except Exception as e:
            jlog("behavior_cache_load_error", error=str(e), path=str(cache_path), level="WARN")
    return None, None

def save_behavior_cache(profile_dir: Path, plus_selector: str, import_selector: str):
    cache_path = profile_dir / UPLOAD_BEHAVIOR_CACHE_FILE
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump({"plus_button_selector": plus_selector, "import_option_selector": import_selector, "ts": time.time()}, f, indent=2)
        jlog("behavior_cache_saved", plus_selector=plus_selector, import_selector=import_selector, level="INFO")
    except Exception as e:
        jlog("behavior_cache_save_error", error=str(e), path=str(cache_path), level="WARN")

def invalidate_behavior_cache(profile_dir: Path):
    cache_path = profile_dir / UPLOAD_BEHAVIOR_CACHE_FILE
    if cache_path.exists():
        try:
            cache_path.unlink()
            jlog("behavior_cache_invalidated", level="INFO")
        except Exception as e:
            jlog("behavior_cache_invalidate_error", error=str(e), path=str(cache_path), level="WARN")

# --- Fonctions de Validation Comportementale ---
async def validate_plus_button_click(page: Page, plus_locator: Locator, import_option_selectors: List[str], strategy_name: str, plus_selector_str: str) -> bool:
    jlog("upload_validate_plus_attempt", strategy=strategy_name, selector=plus_selector_str, level="DEBUG")
    clicked = False
    try:
        try:
            await plus_locator.hover(timeout=500)
            await asyncio.sleep(0.1)
        except Exception:
            pass
        try:
            await plus_locator.focus(timeout=500)
            await asyncio.sleep(0.1)
        except Exception:
            pass
        try:
            await plus_locator.click(timeout=1500, delay=50)
            clicked = True
        except PlaywrightError:
            jlog("upload_validate_plus_click_failed_std", strategy=strategy_name, selector=plus_selector_str, level="DEBUG")
            try:
                await plus_locator.dispatch_event('click', timeout=1000)
                clicked = True
            except PlaywrightError:
                jlog("upload_validate_plus_click_failed", strategy=strategy_name, selector=plus_selector_str, level="WARN")
                return False
        if clicked:
            await asyncio.sleep(0.3)
            combined_import_selector = ", ".join(import_option_selectors)
            import_option_locator = page.locator(combined_import_selector).first
            try:
                await import_option_locator.wait_for(state="visible", timeout=2000)
                jlog("upload_validate_plus_success", strategy=strategy_name, selector=plus_selector_str, level="INFO")
                return True
            except PWTimeoutError:
                jlog("upload_validate_plus_fail_option_not_visible", strategy=strategy_name, selector=plus_selector_str, level="WARN")
                return False
        return False
    except Exception as e:
        jlog("upload_validate_plus_unexpected_error", strategy=strategy_name, selector=plus_selector_str, error=str(e).split('\n')[0], level="WARN")
        return False

async def try_click_import_option_validation(page: Page, locator: Locator, strategy_name: str, selector: str) -> Optional[FileChooser]:
    file_chooser: Optional[FileChooser] = None
    jlog("upload_validate_import_option_attempt", strategy=strategy_name, selector=selector, level="DEBUG")
    try:
        is_visible = False
        try:
            is_visible = await locator.is_visible(timeout=500)
        except Exception:
            pass
        if not is_visible:
            jlog("upload_validate_import_skip_not_visible", strategy=strategy_name, selector=selector, level="DEBUG")
            return None
        try:
            await locator.hover(timeout=500)
            await asyncio.sleep(0.1)
            jlog("upload_validate_import_hovered", strategy=strategy_name, level="DEBUG")
        except Exception as hover_err:
            jlog("upload_validate_import_hover_failed", strategy=strategy_name, error=str(hover_err).split('\n')[0], level="DEBUG")
        try:
            await locator.focus(timeout=500)
            await asyncio.sleep(0.1)
            jlog("upload_validate_import_focused", strategy=strategy_name, level="DEBUG")
        except Exception as focus_err:
            jlog("upload_validate_import_focus_failed", strategy=strategy_name, error=str(focus_err).split('\n')[0], level="DEBUG")
        async with page.expect_file_chooser(timeout=2500) as fc_info:
            clicked = False
            click_method_used = "none"
            try:
                await locator.click(timeout=2000, delay=100)
                clicked = True
                click_method_used = "click_with_delay"
                jlog("upload_validate_import_click_method", method=click_method_used, strategy=strategy_name, level="DEBUG")
            except PlaywrightError as click_err:
                jlog("upload_validate_import_click_failed_trying_dispatch", strategy=strategy_name, error=str(click_err).split('\n')[0], level="DEBUG")
                try:
                    await locator.dispatch_event('click', timeout=1500)
                    clicked = True
                    click_method_used = "dispatch_event"
                    jlog("upload_validate_import_click_method", method=click_method_used, strategy=strategy_name, level="DEBUG")
                except PlaywrightError as dispatch_err:
                    jlog("upload_validate_import_dispatch_failed_trying_enter", strategy=strategy_name, error=str(dispatch_err).split('\n')[0], level="DEBUG")
                    try:
                        await locator.focus(timeout=300)
                        await asyncio.sleep(0.1)
                        await page.keyboard.press("Enter", delay=100)
                        clicked = True
                        click_method_used = "focus_and_enter"
                        jlog("upload_validate_import_click_method", method=click_method_used, strategy=strategy_name, level="DEBUG")
                    except Exception as enter_err:
                        jlog("upload_validate_import_focus_enter_failed", strategy=strategy_name, error=str(enter_err).split('\n')[0], level="WARN")
                        pass
        if clicked:
            try:
                await asyncio.sleep(0.2)
                file_chooser = await fc_info.value
                if file_chooser:
                    jlog("upload_validate_import_success", strategy=strategy_name, selector=selector, method_used=click_method_used, level="INFO")
                    return file_chooser
                else:
                    jlog("upload_validate_import_click_ok_but_no_fc_value", strategy=strategy_name, selector=selector, method_used=click_method_used, level="WARN")
                    return None
            except PWTimeoutError:
                jlog("upload_validate_import_fc_value_timeout", strategy=strategy_name, selector=selector, method_used=click_method_used, level="WARN")
                return None
            except Exception as fc_val_err:
                jlog("upload_validate_import_fc_value_error", strategy=strategy_name, selector=selector, method_used=click_method_used, error=str(fc_val_err).split('\n')[0], level="WARN")
                return None
        else:
            jlog("upload_validate_import_all_click_methods_failed", strategy=strategy_name, selector=selector, level="WARN")
            return None
    except PWTimeoutError:
        jlog("upload_validate_import_no_filechooser", strategy=strategy_name, selector=selector, level="DEBUG")
        return None
    except Exception as e:
        jlog("upload_validate_import_unexpected_error", strategy=strategy_name, selector=selector, error=str(e).split('\n')[0], level="WARN")
        return None

# --- handle_file_upload ---
async def handle_file_upload(page: Page, file_path: str, explicit_selectors_str: Optional[str], profile_dir: Path) -> bool:
    global failure_reason
    jlog("file_upload_started_robust_v8_two_phase", path=file_path, level="INFO")
    found_file_chooser: Optional[FileChooser] = None
    final_plus_selector: Optional[str] = None
    final_import_selector: Optional[str] = None
    strategy_used = "none_yet"
    explicit_plus_selector: Optional[str] = None
    explicit_import_selector: Optional[str] = None

    if explicit_selectors_str and ";;" in explicit_selectors_str:
        parts = explicit_selectors_str.split(";;", 1)
        explicit_plus_selector = parts[0].strip()
        explicit_import_selector = parts[1].strip()
        jlog("using_cli_upload_selectors_pair", plus=explicit_plus_selector, import_opt=explicit_import_selector, level="INFO")
    elif explicit_selectors_str:
        explicit_plus_selector = explicit_selectors_str.strip()
        jlog("using_cli_upload_selector_plus_only", plus=explicit_plus_selector, level="INFO")

    try:
        jlog("file_upload_initial_wait", duration_ms=750, level="DEBUG")
        await page.wait_for_timeout(750)
    except Exception as wait_err:
        jlog("file_upload_initial_wait_error", error=str(wait_err), level="WARN")

    plus_button_locator: Optional[Locator] = None
    plus_strategy_used = "none"

    # Phase 1: bouton '+'
    cached_plus, cached_import = load_behavior_cache(profile_dir)

    # 1a. Cache
    if cached_plus and cached_import:
        jlog("upload_phase1_attempt_strategy", strategy="behavior_cache", selector=cached_plus, level="DEBUG")
        locator = page.locator(cached_plus).first
        try:
            await locator.wait_for(state="visible", timeout=1500)
            if await validate_plus_button_click(page, locator, [cached_import] + DEFAULT_IMPORT_OPTION_SELECTORS, "behavior_cache_plus", cached_plus):
                plus_button_locator = locator
                final_plus_selector = cached_plus
                final_import_selector = cached_import
                plus_strategy_used = "behavior_cache"
                jlog("upload_phase1_success", strategy=plus_strategy_used, selector=final_plus_selector, level="INFO")
            else:
                jlog("behavior_cache_plus_invalid_menu_not_shown", selector=cached_plus, level="WARN")
                invalidate_behavior_cache(profile_dir)
        except Exception as cache_plus_err:
            jlog("behavior_cache_plus_error", selector=cached_plus, error=str(cache_plus_err).split('\n')[0], level="WARN")
            invalidate_behavior_cache(profile_dir)

    # 1b. Sélecteur explicite
    if not plus_button_locator and explicit_plus_selector:
        jlog("upload_phase1_attempt_strategy", strategy="explicit_plus", selector=explicit_plus_selector, level="DEBUG")
        locator = page.locator(explicit_plus_selector).first
        try:
            await locator.wait_for(state="visible", timeout=2000)
            import_options_to_check = ([explicit_import_selector] if explicit_import_selector else []) + DEFAULT_IMPORT_OPTION_SELECTORS
            if await validate_plus_button_click(page, locator, import_options_to_check, "explicit_plus", explicit_plus_selector):
                plus_button_locator = locator
                final_plus_selector = explicit_plus_selector
                plus_strategy_used = "explicit_plus"
                jlog("upload_phase1_success", strategy=plus_strategy_used, selector=final_plus_selector, level="INFO")
            else:
                jlog("upload_explicit_plus_fail_menu_not_shown", selector=explicit_plus_selector, level="WARN")
        except Exception as explicit_plus_err:
            jlog("upload_explicit_plus_error", selector=explicit_plus_selector, error=str(explicit_plus_err).split('\n')[0], level="WARN")

    # 1c. Heuristiques
    if not plus_button_locator:
        jlog("upload_phase1_attempt_strategy", strategy="heuristics_plus", level="DEBUG")
        potential_plus_selectors = DEFAULT_PLUS_BUTTON_SELECTORS
        for i, selector in enumerate(potential_plus_selectors):
            jlog("upload_phase1_trying_heuristic", index=i, selector=selector, level="DEBUG")
            locator = page.locator(selector).first
            try:
                await locator.wait_for(state="visible", timeout=1000)
                import_options_to_check = ([explicit_import_selector] if explicit_import_selector else []) + DEFAULT_IMPORT_OPTION_SELECTORS
                if await validate_plus_button_click(page, locator, import_options_to_check, f"heuristic_plus_{i}", selector):
                    plus_button_locator = locator
                    final_plus_selector = selector
                    plus_strategy_used = f"heuristic_plus_{i}"
                    jlog("upload_phase1_success", strategy=plus_strategy_used, selector=final_plus_selector, level="INFO")
                    break
            except Exception as heuristic_plus_err:
                jlog("upload_heuristic_plus_error_or_fail", index=i, selector=selector, error=str(heuristic_plus_err).split('\n')[0], level="DEBUG")

    # 1d. Recherche exhaustive
    if not plus_button_locator:
        jlog("upload_phase1_attempt_strategy", strategy="behavioral_exhaustive_plus", level="DEBUG")
        try:
            input_box_selector = ", ".join(INPUT_BOX_SELECTORS)
            await page.locator(input_box_selector).first.wait_for(state="visible", timeout=3000)
            exhaustive_plus_selector = f"button:near({input_box_selector}, 150):visible"
            buttons_locator = page.locator(exhaustive_plus_selector)
            button_count = await buttons_locator.count()
            jlog("behavioral_exhaustive_plus_candidates", count=button_count, selector=exhaustive_plus_selector, level="DEBUG")
            for i in range(button_count):
                locator = buttons_locator.nth(i)
                current_selector_str = f"{exhaustive_plus_selector} >> nth={i}"
                jlog("behavioral_exhaustive_plus_trying", index=i, selector=current_selector_str, level="DEBUG")
                import_options_to_check = ([explicit_import_selector] if explicit_import_selector else []) + DEFAULT_IMPORT_OPTION_SELECTORS
                if await validate_plus_button_click(page, locator, import_options_to_check, f"exhaustive_plus_{i}", current_selector_str):
                    plus_button_locator = locator
                    try:
                        aria = await locator.get_attribute("aria-label", timeout=50)
                        final_plus_selector = f"button[aria-label='{aria}']" if aria else current_selector_str
                    except Exception:
                        final_plus_selector = current_selector_str
                    plus_strategy_used = f"exhaustive_plus_{i}"
                    jlog("upload_phase1_success", strategy=plus_strategy_used, selector=final_plus_selector, level="INFO")
                    break
        except Exception as exhaustive_plus_err:
            jlog("behavioral_exhaustive_plus_error", error=str(exhaustive_plus_err).split('\n')[0], level="WARN")

    if not plus_button_locator or not final_plus_selector:
        jlog("upload_phase1_failed_all_strategies", level="ERROR")
        failure_reason = "upload_plus_button_not_found_v8"
        return False

    # Phase 2: Option "Importer" + FileChooser
    import_option_locator: Optional[Locator] = None
    import_strategy_used = "none"
    found_file_chooser: Optional[FileChooser] = None

    # 2a. Import cache (si Phase 1 via cache)
    if final_import_selector and plus_strategy_used == "behavior_cache":
        jlog("upload_phase2_attempt_strategy", strategy="behavior_cache_import", selector=final_import_selector, level="DEBUG")
        locator = page.locator(final_import_selector).first
        found_file_chooser = await try_click_import_option_validation(page, locator, "behavior_cache_import", final_import_selector)
        if found_file_chooser:
            import_option_locator = locator
            import_strategy_used = "behavior_cache_import"
            strategy_used = f"{plus_strategy_used} -> {import_strategy_used}"
            jlog("upload_phase2_success", strategy=import_strategy_used, selector=final_import_selector, level="INFO")
        else:
            jlog("behavior_cache_import_invalid", selector=final_import_selector, level="WARN")
            invalidate_behavior_cache(profile_dir)
            final_import_selector = None

    # 2b. Import explicite
    if not import_option_locator and explicit_import_selector:
        jlog("upload_phase2_attempt_strategy", strategy="explicit_import", selector=explicit_import_selector, level="DEBUG")
        locator = page.locator(explicit_import_selector).first
        found_file_chooser = await try_click_import_option_validation(page, locator, "explicit_import", explicit_import_selector)
        if found_file_chooser:
            import_option_locator = locator
            final_import_selector = explicit_import_selector
            import_strategy_used = "explicit_import"
            strategy_used = f"{plus_strategy_used} -> {import_strategy_used}"
            jlog("upload_phase2_success", strategy=import_strategy_used, selector=final_import_selector, level="INFO")
            save_behavior_cache(profile_dir, final_plus_selector, final_import_selector)

    # 2c. Heuristiques import
    if not import_option_locator:
        jlog("upload_phase2_attempt_strategy", strategy="heuristics_import", level="DEBUG")
        potential_import_selectors = DEFAULT_IMPORT_OPTION_SELECTORS
        for i, selector in enumerate(potential_import_selectors):
            jlog("upload_phase2_trying_heuristic", index=i, selector=selector, level="DEBUG")
            locator = page.locator(selector).first
            found_file_chooser = await try_click_import_option_validation(page, locator, f"heuristic_import_{i}", selector)
            if found_file_chooser:
                import_option_locator = locator
                final_import_selector = selector
                import_strategy_used = f"heuristic_import_{i}"
                strategy_used = f"{plus_strategy_used} -> {import_strategy_used}"
                jlog("upload_phase2_success", strategy=import_strategy_used, selector=final_import_selector, level="INFO")
                save_behavior_cache(profile_dir, final_plus_selector, final_import_selector)
                break

    # 2d. Exhaustive import
    if not import_option_locator:
        jlog("upload_phase2_attempt_strategy", strategy="behavioral_exhaustive_import", level="DEBUG")
        menu_selector = "[role='menu']:visible, [role='listbox']:visible, div[class*='menu']:visible"
        menu_locator = page.locator(menu_selector).first
        try:
            menu_count = await menu_locator.count()
        except Exception:
            menu_count = 0
        search_base_locator = menu_locator if menu_count > 0 else page
        base_label = menu_selector if menu_count > 0 else "page"
        exhaustive_import_selector = "button:visible, [role='menuitem']:visible"
        options_locator = search_base_locator.locator(exhaustive_import_selector)
        option_count = await options_locator.count()
        jlog("behavioral_exhaustive_import_candidates", count=option_count, base_selector=(menu_selector if menu_count > 0 else "page"), level="DEBUG")
        for i in range(option_count):
            locator = options_locator.nth(i)
            current_selector_str = f"{base_label} >> {exhaustive_import_selector} >> nth={i}"
            try:
                item_text = (await locator.text_content(timeout=50)) or ""
                item_aria = (await locator.get_attribute("aria-label", timeout=50)) or ""
                jlog("behavioral_exhaustive_import_trying", index=i, text=item_text[:30], aria=item_aria[:40], level="DEBUG")
            except Exception:
                jlog("behavioral_exhaustive_import_trying", index=i, text="<error>", aria="<error>", level="DEBUG")

            found_file_chooser = await try_click_import_option_validation(page, locator, f"exhaustive_import_{i}", current_selector_str)
            if found_file_chooser:
                import_option_locator = locator
                try:
                    aria = await locator.get_attribute("aria-label", timeout=50)
                    text_content = await locator.text_content(timeout=50)
                    final_import_selector = f"button[aria-label='{aria}']" if aria else (f"button:has-text('{text_content}')" if text_content else current_selector_str)
                except Exception:
                    final_import_selector = current_selector_str
                import_strategy_used = f"exhaustive_import_{i}"
                strategy_used = f"{plus_strategy_used} -> {import_strategy_used}"
                jlog("upload_phase2_success", strategy=import_strategy_used, selector=final_import_selector, level="INFO")
                save_behavior_cache(profile_dir, final_plus_selector, final_import_selector)
                break

    if not found_file_chooser or not import_option_locator or not final_import_selector:
        jlog("upload_phase2_failed_all_strategies", level="ERROR")
        failure_reason = "upload_import_option_not_found_v8"
        if plus_strategy_used == "behavior_cache":
            invalidate_behavior_cache(profile_dir)
        return False

    # Phase 3: set_files + confirmations
    try:
        jlog("file_chooser_obtained", strategy=strategy_used, plus_selector=final_plus_selector, import_selector=final_import_selector, level="INFO")
        file_to_upload = Path(file_path)
        if not file_to_upload.is_file():
            jlog("file_path_invalid_or_not_found", path=file_path, level="ERROR")
            failure_reason = "file_not_found"
            raise FileNotFoundError(f"Le fichier spécifié n'existe pas : {file_path}")

        await found_file_chooser.set_files(file_to_upload)
        jlog("file_chooser_set_files_success", ok=True, file=file_to_upload.name, level="INFO")

        confirmation_selectors = [
            'div.multimodal-chunk', 'mat-chip-row', 'div.file-preview-container',
            'img[alt*="Preview"]', 'div[aria-label*="file"]', '[data-testid="file-attachment-chip"]',
        ]
        combined_selector_visible = ", ".join(confirmation_selectors)
        spinner_selector_disappear = 'div.multimodal-chunk mat-progress-spinner'
        jlog("waiting_for_upload_confirmation", selectors_sample=confirmation_selectors[:2], timeout_s=180, level="INFO")
        visible_confirmation = page.locator(combined_selector_visible).first
        await visible_confirmation.wait_for(state='visible', timeout=180000)
        jlog("file_upload_confirmation_visible", level="INFO")

        try:
            spinner = page.locator(spinner_selector_disappear).first
            is_spinner_visible = False
            try:
                is_spinner_visible = await spinner.is_visible(timeout=1000)
            except PWTimeoutError:
                pass
            if is_spinner_visible:
                jlog("waiting_for_upload_spinner_to_disappear", level="INFO")
                await spinner.wait_for(state='hidden', timeout=120000)
                jlog("upload_spinner_disappeared", level="INFO")
            else:
                jlog("upload_spinner_not_found_or_already_hidden", level="DEBUG")
        except PWTimeoutError:
            jlog("upload_spinner_did_not_disappear_timeout", level="WARN")
        except Exception as spinner_e:
            jlog("upload_spinner_wait_error", error=str(spinner_e).split('\n')[0], level="WARN")

        return True

    except FileNotFoundError:
        return False
    except PWTimeoutError as timeout_err:
        error_msg = str(timeout_err).split('\n')[0]
        is_confirm_timeout = 'wait_for(state=\'visible\'' in error_msg
        failure_reason = "upload_confirmation_timeout" if is_confirm_timeout else "upload_finalization_timeout"
        jlog(failure_reason, strategy=strategy_used, plus_selector=final_plus_selector, import_selector=final_import_selector, error=error_msg, level="ERROR")
        return False
    except PlaywrightError as upload_pw_err:
        error_msg = str(upload_pw_err).split('\n')[0]
        jlog("file_upload_finalization_playwright_error", strategy=strategy_used, plus_selector=final_plus_selector, import_selector=final_import_selector, error=error_msg, level="ERROR")
        failure_reason = "upload_finalization_pw_error"
        return False
    except Exception as e:
        error_msg = str(e).split('\n')[0]
        jlog("file_upload_finalization_unexpected_error", strategy=strategy_used, error=error_msg, error_type=type(e).__name__, level="ERROR")
        failure_reason = "upload_finalization_unexpected_error"
        return False

# --- print_debug_instructions ---
def print_debug_instructions():
    print("\n" + "="*80)
    print(" MODE DEBUG : TROUVER LES SÉLECTEURS CSS (Optionnel) ".center(80, "="))
    print("="*80)
    print("Le navigateur est lancé en mode visible et mis en pause.")
    print("Utilisez les outils de développement (clic droit -> Inspecter) pour :")
    print("\n1. Sélecteur zone de réponse (OPTIONNEL) → ajoutez aux preferredSelectors dans dom.py (pickCandidate).")
    print("2. Sélecteurs upload (PLUS/MENU) → passez via --upload-selector 'PLUS;;IMPORT', ou config.yaml.")
    print("3. Sélecteur bouton Envoyer → env GH_SUBMIT_SELECTOR_OVERRIDE='button[aria-label=\"Envoyer le message\"]'.")
    print("\nFermez le navigateur une fois les sélecteurs identifiés.\n" + "-"*80)

# --- save_failure_artifacts ---
async def save_failure_artifacts(page: Optional[Page], profile: SandboxProfile, reason: str):
    if not page or page.is_closed():
        jlog("save_artifacts_skipped_page_closed", reason=reason, level="WARN")
        return
    try:
        ts = time.strftime("%Y%m%d_%H%M%S")
        safe_reason = re.sub(r'[\\/*?:"<>|\s]+', '_', reason)[:50]
        artifact_dir = Path(profile.profile_dir)
        artifact_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = artifact_dir / f"failure_{safe_reason}_{ts}.png"
        html_path = artifact_dir / f"failure_{safe_reason}_{ts}.html"
        await page.screenshot(path=str(screenshot_path), full_page=True)
        jlog("screenshot_saved_on_fail", path=str(screenshot_path), level="INFO")
        html_content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        jlog("html_saved_on_fail", path=str(html_path), level="INFO")
    except Exception as e:
        jlog("save_failure_artifacts_error", error=str(e).splitlines()[0], level="ERROR")

# --- Tâche Sonde d'Activité (détection visuelle + changement DOM/texte) ---
async def activity_probe_task(page: Optional[Page], orchestrator_done_event: asyncio.Event) -> None:
    """Vérifie périodiquement si Gemini est actif (spinner / texte changeant)."""
    jlog("activity_probe_task_started", interval_s=ACTIVITY_PROBE_INTERVAL_S)

    response_area_selectors = [
        'div.response-container-content md-block:last-of-type',
        'div[data-message-author="assistant"]:not([aria-busy="true"]) .inner-content:last-of-type',
        'article[data-author="assistant"]:last-of-type',
        '.model-response-text:last-of-type',
        'div[jscontroller][aria-live]',
    ]
    activity_selectors = [
        "[aria-busy='true']", "[role='progressbar']", ".loading-spinner", ".spinner",
        ".generating-indicator", ".material-icons-extended.animate-spin",
        'div[role="complementary"]:visible *[class*="icon"]',
        'div[role="complementary"]:visible *[class*="spinner"]',
    ]

    js_check_activity = f"""
    () => {{
        window.__last_response_text_hash = window.__last_response_text_hash || null;

        const isVisible = (el) => {{
            if (!el || !el.isConnected) return false;
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && (rect.width > 0 || rect.height > 0);
        }};

        const activitySelectors = {json.dumps(activity_selectors)};
        const responseSelectors = {json.dumps(response_area_selectors)};

        // 1) activité visuelle ?
        for (const selector of activitySelectors) {{
            try {{
                const element = document.querySelector(selector);
                if (element && isVisible(element)) {{
                    window.__last_response_text_hash = null;
                    return {{ active: true, selector: selector, reason: 'visual' }};
                }}
            }} catch (_e) {{}}
        }}

        // 2) changement de texte ?
        let responseArea = null;
        for (const sel of responseSelectors) {{
            try {{
                responseArea = document.querySelector(sel);
                if (responseArea && isVisible(responseArea)) break;
            }} catch(_e) {{}}
        }}

        if (responseArea) {{
            try {{
                const current_response_text = (responseArea.innerText || responseArea.textContent || "").trim();
                const current_hash = current_response_text.length + '_' + current_response_text.substring(0, 50);

                if (window.__last_response_text_hash === null) {{
                    window.__last_response_text_hash = current_hash;
                    return {{ active: false, selector: null, reason: 'initial_hash_set' }};
                }}
                if (current_hash !== window.__last_response_text_hash) {{
                    window.__last_response_text_hash = current_hash;
                    return {{ active: true, selector: 'response_area_change', reason: 'dom_change', hash: current_hash }};
                }}
                return {{ active: false, selector: null, reason: 'no_change' }};
            }} catch(_e) {{
                window.__last_response_text_hash = null;
                return {{ active: false, selector: null, reason: 'dom_scan_error' }};
            }}
        }}

        window.__last_response_text_hash = null;
        return {{ active: false, selector: null, reason: 'no_activity' }};
    }}
    """

    while not orchestrator_done_event.is_set():
        try:
            await asyncio.sleep(ACTIVITY_PROBE_INTERVAL_S)
            if orchestrator_done_event.is_set():
                break
            if not page or page.is_closed():
                jlog("activity_probe_skipped_page_closed", level="WARN")
                break

            jlog("activity_probe_check", level="DEBUG")
            # IMPORTANT : appliquer un vrai timeout au evaluate
            try:
                result = await asyncio.wait_for(page.evaluate(js_check_activity), timeout=ACTIVITY_PROBE_TIMEOUT_MS / 1000)
            except asyncio.TimeoutError:
                jlog("activity_probe_js_timeout", timeout_ms=ACTIVITY_PROBE_TIMEOUT_MS, level="WARN")
                continue

            jlog("activity_probe_js_result", result=result, level="DEBUG")

            if isinstance(result, dict) and result.get("active"):
                reason = result.get("reason", "unknown")
                selector = result.get("selector", "unknown")
                jlog("gemini_activity_probe", status="busy", reason=reason, selector_found=(selector or "")[:100], level="INFO")
            else:
                jlog("gemini_activity_probe", status="idle", reason=(result or {}).get("reason", "no_activity"),
                     warning_details="Relying on Heartbeat due to lack of detectable activity.", level="WARN")

        except asyncio.CancelledError:
            jlog("activity_probe_task_cancelled")
            break
        except PlaywrightError as pw_err:
            msg = str(pw_err).lower()
            if "closed" in msg or "navigated" in msg or "target was destroyed" in msg:
                jlog("activity_probe_playwright_error_ignored", error=str(pw_err).splitlines()[0], level="WARN")
                break
            else:
                jlog("activity_probe_playwright_error", error=str(pw_err).splitlines()[0], level="ERROR")
                await asyncio.sleep(ACTIVITY_PROBE_INTERVAL_S * 2)
        except Exception as e:
            jlog("activity_probe_error", error=str(e), error_type=type(e).__name__, level="ERROR",
                 traceback=traceback.format_exc(limit=2))
            await asyncio.sleep(ACTIVITY_PROBE_INTERVAL_S * 2)

    jlog("activity_probe_task_finished")

# --- main ---
async def main() -> int:
    global failure_reason
    start_ts = time.time()
    pid = os.getpid()
    jlog("script_start", pid=pid, version="7.11-IntelliFix", args=sys.argv[1:], level="INFO")

    # Args
    ap = argparse.ArgumentParser(description="Client CLI pour Gemini Headless.")
    ap.add_argument("--user-id", required=True)
    ap.add_argument("--profile-base", required=True)
    ap.add_argument("--prompt", help="Requis sauf si --login/--debug.")
    ap.add_argument("--file", help="Optionnel : chemin fichier.")
    ap.add_argument("--login", action="store_true")
    ap.add_argument("--debug-selectors", action="store_true")
    ap.add_argument("--screenshot-on-fail", action="store_true")
    ap.add_argument("--upload-selector", type=str, default=None, help="Optionnel: Sélecteur(s) bouton upload (ex: 'sel1;;sel2').")
    args = ap.parse_args()

    if not args.login and not args.prompt and not args.debug_selectors:
        error_msg = "Erreur : --prompt requis (ou --login/--debug)."
        print(error_msg, file=sys.stderr, flush=True)
        jlog("arg_validation_failed", reason="missing_prompt_or_mode", level="ERROR")
        failure_reason = "missing_prompt_or_mode"
        return 1

    profile: Optional[SandboxProfile] = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None
    exit_code = 1
    failure_reason = "main_init"
    orch: Optional[Orchestrator] = None
    playwright_instance = None
    probe_task: Optional[asyncio.Task] = None

    # Lecture config YAML (optionnelle)
    config = {}
    config_path = Path(__file__).resolve().parent / "config.yaml"
    if config_path.exists():
        try:
            import yaml as yaml_mod  # éviter collision nom
        except ImportError:
            jlog("yaml_import_error_config_skipped", level="WARN", message="PyYAML non installé.")
            yaml_mod = None
        if yaml_mod:
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml_mod.safe_load(f)
                jlog("config_loaded", path=str(config_path), level="INFO")
            except Exception as e:
                jlog("config_load_error", path=str(config_path), error=str(e), level="WARN")

    # Sélecteurs upload explicites (CLI ou YAML)
    explicit_upload_selectors_str: Optional[str] = None
    if args.upload_selector:
        explicit_upload_selectors_str = args.upload_selector
        jlog("using_cli_upload_selector_str", selector_str=args.upload_selector, level="INFO")
    else:
        config_selectors_list = config.get('upload_selectors', [])
        if isinstance(config_selectors_list, list):
            for item in config_selectors_list:
                if (isinstance(item, str) and ";;" in item and
                    not item.strip().startswith("#") and
                    "REMPLACEZ" not in item.upper() and
                    "AJOUTEZ" not in item.upper()):
                    explicit_upload_selectors_str = item.strip()
                    jlog("using_first_config_upload_selector_pair", selector_str=explicit_upload_selectors_str, level="INFO")
                    break
            if not explicit_upload_selectors_str:
                jlog("no_valid_selector_pair_found_in_config", level="DEBUG")
        else:
            jlog("config_upload_selectors_not_a_list", level="WARN")

    try:
        # Profil & fingerprint
        profile = SandboxProfile(user_id=args.user_id, base_dir=args.profile_base)
        profile.ensure_dirs()
        user_data_dir = profile.user_data_dir
        fp = Fingerprint.load_or_seed(profile)
        jlog("profile_initialized", user_id=args.user_id, dir=profile.profile_dir, level="INFO")
        browser_path = get_browser_executable_path()
        if not browser_path:
            error_msg = "Erreur CRITIQUE : Navigateur (Chrome/Edge/Brave) non trouvé."
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "browser_not_found"
            return 1
        jlog("using_browser", path=browser_path, level="INFO")

        playwright_instance = await async_playwright().start()
        p = playwright_instance

        # Headless intelligent : par défaut on est headless si non login/debug,
        # sauf si YAML fournit une valeur explicite (True/False).
        is_interactive = bool(args.login or args.debug_selectors)
        yaml_autospawn = config.get('autospawn', {}) if isinstance(config.get('autospawn', {}), dict) else {}
        yaml_headless = yaml_autospawn.get('headless', None)
        if yaml_headless is None:
            is_headless = not is_interactive
        else:
            is_headless = bool(yaml_headless)

        launch_args = build_launch_args(fp)
        common_args = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu', '--disable-dev-shm-usage']
        if not is_headless:
            common_args.append('--start-maximized')
        if sys.platform == "linux":
            common_args.append('--no-zygote')
        for arg in common_args:
            if arg not in launch_args:
                launch_args.append(arg)

        screen_width, screen_height = fp.screen if fp and fp.screen else (1366, 768)
        viewport_arg = f"--window-size={screen_width},{screen_height}"
        if not any(arg.startswith('--window-size') for arg in launch_args):
            launch_args.append(viewport_arg)
        if is_headless and "--start-maximized" in launch_args and any(arg.startswith('--window-size') for arg in launch_args):
            try:
                launch_args.remove("--start-maximized")
                jlog("removed_start_maximized_for_headless", level="DEBUG")
            except ValueError:
                pass

        jlog("launching_persistent_context", headless=is_headless, user_data_dir=user_data_dir, args_count=len(launch_args), level="INFO")
        try:
            launch_timeout = 240000  # 4 minutes
            context = await p.chromium.launch_persistent_context(
                user_data_dir,
                headless=is_headless,
                executable_path=browser_path,
                args=launch_args,
                ignore_default_args=["--enable-automation"],
                viewport={"width": screen_width, "height": screen_height},
                timeout=launch_timeout
            )
            jlog("persistent_context_launched", ok=True, level="INFO")
        except PlaywrightError as launch_err:
            error_msg = f"Erreur CRITIQUE lancement contexte Playwright: {str(launch_err).splitlines()[0]}"
            jlog("launch_context_failed", error=error_msg, level="CRITICAL")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "launch_failed"
            return 1
        except Exception as generic_launch_err:
            error_msg = f"Erreur CRITIQUE inattendue lancement contexte Playwright: {str(generic_launch_err).splitlines()[0]}"
            jlog("launch_context_failed_generic", error=error_msg, error_type=type(generic_launch_err).__name__, level="CRITICAL")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "launch_failed_generic"
            return 1

        page = context.pages[0] if context.pages else await context.new_page()
        if not page:
            error_msg = "Erreur CRITIQUE: Impossible d'obtenir une page Playwright."
            jlog("page_creation_failed", level="CRITICAL")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "page_creation"
            raise Exception(error_msg)

        jlog("page_obtained", count=len(context.pages), level="INFO")
        await apply_stealth(page, fingerprint=fp.__dict__)
        jlog("stealth_applied", level="INFO")

        # Navigation & session guardian
        target_url = "https://gemini.google.com/app"
        try:
            current_url = page.url or ""
            await page.wait_for_timeout(1500)
            current_url = page.url or ""
            if "gemini.google.com/app" not in current_url or "consent.google.com" in current_url or "/signin/" in current_url:
                jlog("navigating_to_gemini", url=target_url, from_url=current_url, level="INFO")
                await page.goto(target_url, wait_until="domcontentloaded", timeout=120000)
            else:
                jlog("already_on_gemini", url=current_url, level="INFO")
                await page.wait_for_load_state("domcontentloaded", timeout=60000)
        except PlaywrightError as nav_err:
            jlog("navigation_to_gemini_failed", url=target_url, error=str(nav_err).split('\n')[0], level="WARN")
            if "gemini.google.com" in (page.url or ""):
                jlog("attempting_reload_after_nav_fail", level="WARN")
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=120000)
                    jlog("reload_successful", level="INFO")
                except PlaywrightError as reload_err:
                    error_msg = f"Erreur CRITIQUE: Échec du rechargement après échec navigation: {str(reload_err).splitlines()[0]}"
                    jlog("reload_failed", error=error_msg, level="CRITICAL")
                    print(error_msg, file=sys.stderr, flush=True)
                    failure_reason = "reload_failed"
                    raise
            else:
                error_msg = f"Erreur CRITIQUE: Échec navigation vers Gemini: {str(nav_err).splitlines()[0]}"
                print(error_msg, file=sys.stderr, flush=True)
                failure_reason = "navigation_failed"
                raise
        except Exception as nav_e:
            error_msg = f"Erreur CRITIQUE: Échec navigation (autre): {nav_e}"
            jlog("navigation_generic_error", url=target_url, error=error_msg, level="CRITICAL")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "navigation_error"
            raise

        jlog("page_ready", url=page.url, level="INFO")
        try:
            guardian = SessionGuardian(profile_root=Path(profile.profile_dir), logger=None)
            repair_result = await guardian.repair_if_needed(page, timeout_s=60.0)
            jlog("session_guardian_check_complete", result=repair_result, level="INFO")
        except Exception as guard_err:
            error_msg = f"Erreur CRITIQUE vérification session: {guard_err}"
            jlog("session_guardian_run_error", error=error_msg, level="CRITICAL")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "session_guardian_failed"
            return 1
        if repair_result.get("need_reset"):
            error_msg = "ERREUR: Session invalide ou cookies manquants. Connexion manuelle requise (--login)."
            print(error_msg, file=sys.stderr, flush=True)
            jlog("session_needs_reset", details=repair_result, level="ERROR")
            failure_reason = "session_invalid"
            return 1

        # Modes spéciaux
        if args.login or args.debug_selectors:
            if args.login:
                print(
                    "\n" + "="*80 + "\n" +
                    " NAVIGATEUR OUVERT POUR CONNEXION MANUELLE ".center(80, "=") +
                    "\n" + "="*80 +
                    "\n1. Connectez-vous.\n2. Acceptez cookies/conditions.\n3. Allez sur gemini.google.com/app.\n4. Fermez le navigateur quand terminé.\n" +
                    "-"*80,
                    flush=True
                )
            else:
                print_debug_instructions()
                sys.stdout.flush()

            mode = "login" if args.login else "debug_selectors"
            jlog("entering_manual_mode", mode=mode, level="INFO")
            # Attendre réellement la fermeture par l'utilisateur
            await page.wait_for_event("close")
            jlog("browser_closed_by_user", mode=mode, level="INFO")
            context = None
            page = None
            exit_code = 0
            failure_reason = ""
            return exit_code

        # Mode normal
        if not args.prompt:
            error_msg = "Erreur CRITIQUE interne: --prompt requis mais absent."
            print(error_msg, file=sys.stderr, flush=True)
            jlog("internal_error", reason="no_prompt_in_normal_mode", level="CRITICAL")
            failure_reason = "no_prompt"
            return 1

        # 1) Upload
        upload_successful = False
        if args.file:
            # FIX: passer le bon dir
            upload_successful = await handle_file_upload(page, args.file, explicit_upload_selectors_str, Path(profile.profile_dir))
            if not upload_successful:
                error_msg = f"ERREUR: Échec de l'upload du fichier '{args.file}'. Raison: {failure_reason}"
                jlog("file_upload_failed_exiting_main", reason=failure_reason, level="ERROR")
                print(error_msg, file=sys.stderr, flush=True)
                if args.screenshot_on_fail:
                    await save_failure_artifacts(page, profile, failure_reason)
                return 1
            else:
                jlog("file_upload_successful_proceeding_with_prompt", level="INFO")

        # 2) Envoyer le prompt
        send_success = False
        try:
            jlog("calling_fast_send_prompt_v7", level="INFO")
            send_success = await fast_send_prompt(page, args.prompt, is_post_upload=upload_successful)
            jlog("fast_send_prompt_result_v7", success=send_success, level="INFO")
            if not send_success:
                failure_reason = "send_prompt_failed_v7"
                error_message = "ERREUR CRITIQUE: Échec de la soumission du prompt (fast_send_prompt v7 a retourné False). Abandon."
                jlog("fast_send_prompt_failed_exiting", reason=failure_reason, level="CRITICAL")
                print(error_message, file=sys.stderr, flush=True)
                await asyncio.sleep(1)
                if args.screenshot_on_fail:
                    await save_failure_artifacts(page, profile, failure_reason)
                return 1
            else:
                jlog("gemini_processing_acknowledged", source="input_submit", level="INFO")
        except Exception as send_err:
            failure_reason = "send_prompt_exception_v7"
            error_message = f"ERREUR CRITIQUE: Exception lors de la soumission du prompt (v7): {str(send_err).splitlines()[0]}"
            jlog("fast_send_prompt_exception", error=error_message, level="CRITICAL")
            print(error_message, file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            await asyncio.sleep(1)
            if args.screenshot_on_fail:
                await save_failure_artifacts(page, profile, failure_reason)
            return 1

        # 3) Orchestrateur + sonde
        jlog("starting_orchestrator_adaptative", level="INFO")
        stagnation_timeout_ms = config.get('orchestrator', {}).get('stagnation_timeout_ms', 120000)
        orch = Orchestrator(page, stagnation_timeout_ms=stagnation_timeout_ms)

        probe_task = asyncio.create_task(activity_probe_task(page, orch._done_evt), name="activity_probe")
        text, meta = await orch.run_fast_path()  # attend la fin

        # Analyse résultat
        final_len = len(text)
        source = meta.get("source_chosen", "unknown")
        total_ms = meta.get("total_ms", -1)
        jlog("orchestrator_finished", source=source, len=final_len, total_ms=total_ms, level="INFO")
        text_to_check = text.strip().lower()

        if meta.get("invalid_response") is True:
            error_msg = f"ERREUR: Réponse invalide détectée par l'observateur JS: '{text[:100]}...'"
            jlog("invalid_response_detected_by_observer", response_snippet=text[:100], source=source, total_ms=total_ms, level="ERROR")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "invalid_response_observer"
            exit_code = 2
        elif final_len > 0 and any(s in text_to_check for s in INVALID_GEMINI_RESPONSES):
            error_msg = f"ERREUR: Réponse invalide détectée (fallback check): '{text[:100]}...'"
            jlog("invalid_response_detected_post_orchestration", response_snippet=text[:100], source=source, total_ms=total_ms, level="ERROR")
            print(error_msg, file=sys.stderr, flush=True)
            failure_reason = "invalid_response_detected"
            exit_code = 2
        elif final_len == 0 and not meta.get("invalid_response"):
            warning_msg = "ERREUR: Aucune réponse textuelle reçue de Gemini après attente complète."
            jlog("empty_response_received", source=source, total_ms=total_ms, level="ERROR")
            print(warning_msg, file=sys.stderr, flush=True)
            failure_reason = "empty_response_final"
            exit_code = 1
        else:
            try:
                sys.stdout.buffer.write(text.encode('utf-8'))
                sys.stdout.flush()
                jlog("output_written_to_stdout_buffer", len_bytes=len(text.encode('utf-8')), len_chars=final_len, level="INFO")
                exit_code = 0
                failure_reason = ""
            except Exception as write_err:
                error_msg = f"ERREUR CRITIQUE: Échec de l'écriture de la sortie UTF-8: {write_err}"
                jlog("stdout_buffer_write_error", error=error_msg, level="CRITICAL")
                print(error_msg, file=sys.stderr, flush=True)
                failure_reason = "stdout_write_failed"
                exit_code = 1

        return exit_code

    except PlaywrightError as pw_err:
        error_msg = str(pw_err).split('\n')[0]
        full_error_msg = f"ERREUR CRITIQUE Playwright: {error_msg}"
        jlog("playwright_error_main", error=error_msg, error_type=type(pw_err).__name__, stack_preview=traceback.format_exc(limit=3), level="CRITICAL")
        print(full_error_msg, file=sys.stderr, flush=True)
        failure_reason = "playwright_error"
        exit_code = 1
    except Exception as e:
        error_msg = str(e).split('\n')[0]
        full_error_msg = f"ERREUR CRITIQUE Inattendue: {error_msg} ({type(e).__name__})"
        jlog("main_loop_unhandled_error", error=error_msg, error_type=type(e).__name__, stack_preview=traceback.format_exc(limit=3), level="CRITICAL")
        print(full_error_msg, file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        failure_reason = "unhandled_exception"
        exit_code = 1
    finally:
        sys.stderr.flush()

        # Annuler sonde si active
        if probe_task and not probe_task.done():
            jlog("cancelling_activity_probe_task_finally", level="INFO")
            probe_task.cancel()
            try:
                await asyncio.wait_for(probe_task, timeout=1.0)
            except asyncio.CancelledError:
                jlog("activity_probe_task_cancelled_ok", level="INFO")
            except asyncio.TimeoutError:
                jlog("activity_probe_task_cancel_timeout", level="WARN")
            except Exception as probe_cancel_err:
                jlog("activity_probe_task_cancel_error", error=str(probe_cancel_err), level="WARN")

        # Last Gasp DOM (utilise _GET_BEST_TEXT_JS V7.11)
        try:
            SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        snapshot_signal_file = SIGNAL_DIR / f"{pid}.snapshot_request"
        snapshot_text = ""
        if snapshot_signal_file.exists():
            jlog("last_gasp_signal_detected", pid=pid, file=str(snapshot_signal_file), level="INFO")
            try:
                snapshot_signal_file.unlink(missing_ok=True)
            except Exception:
                pass

            if orch and hasattr(orch, 'page') and orch.page and (not orch.page.is_closed()):
                jlog("last_gasp_attempting_snapshot_via_js_v7.11", level="INFO")
                snapshot_text_raw = ""
                try:
                    snap_js_expr = f"({_GET_BEST_TEXT_JS})()"
                    snapshot_text_raw = await asyncio.wait_for(
                        orch.page.evaluate(snap_js_expr),
                        timeout=SNAPSHOT_REQUEST_TIMEOUT_S
                    )
                    if snapshot_text_raw and isinstance(snapshot_text_raw, str):
                        snapshot_text = snapshot_text_raw.strip()
                        jlog("last_gasp_snapshot_captured_js_v7.11", raw_len=len(snapshot_text_raw), level="INFO")
                    else:
                        jlog("last_gasp_snapshot_js_empty_or_invalid", level="WARN", result_type=type(snapshot_text_raw).__name__)
                except asyncio.TimeoutError:
                    jlog("last_gasp_snapshot_timeout", level="ERROR")
                except asyncio.CancelledError:
                    jlog("last_gasp_snapshot_cancelled", level="WARN")
                except PlaywrightError as snap_pw_err:
                    jlog("last_gasp_snapshot_playwright_error", error=str(snap_pw_err).splitlines()[0], level="ERROR")
                except Exception as snap_err:
                    jlog("last_gasp_snapshot_generic_error", error=str(snap_err).splitlines()[0], error_type=type(snap_err).__name__, level="ERROR")
            else:
                jlog("last_gasp_snapshot_skipped", reason="orchestrator_or_dom_unavailable_or_no_snapshot_method", level="WARN")

            try:
                snapshot_bytes = snapshot_text.encode('utf-8')
                start_marker = f"\n{LAST_GASP_SNAPSHOT_START_MARKER}\n".encode('utf-8')
                end_marker = f"\n{LAST_GASP_SNAPSHOT_END_MARKER}\n".encode('utf-8')
                jlog("last_gasp_preparing_to_write_stdout", len_bytes=len(snapshot_bytes), len_chars=len(snapshot_text), level="DEBUG")
                sys.stdout.buffer.write(start_marker)
                sys.stdout.buffer.write(snapshot_bytes)
                sys.stdout.buffer.write(end_marker)
                sys.stdout.flush()
                jlog("last_gasp_snapshot_printed_to_stdout_buffer", len_bytes=len(snapshot_bytes), len_chars=len(snapshot_text), level="INFO" if snapshot_text else "WARN")
            except Exception as print_err:
                jlog("last_gasp_snapshot_print_error", error=str(print_err), level="CRITICAL")
                try:
                    print(f"\n{LAST_GASP_SNAPSHOT_START_MARKER}\n{snapshot_text}\n{LAST_GASP_SNAPSHOT_END_MARKER}", file=sys.stderr, flush=True)
                except Exception:
                    pass

        # Artefacts si échec
        if exit_code != 0 and args.screenshot_on_fail and page and profile and not page.is_closed():
            jlog("attempting_save_artifacts_before_context_close", reason=failure_reason, level="INFO")
            try:
                current_loop = asyncio.get_running_loop()
                if current_loop and not current_loop.is_closed():
                    await save_failure_artifacts(page, profile, failure_reason)
                else:
                    jlog("cannot_save_artifacts_event_loop_closed", level="WARN")
            except RuntimeError:
                jlog("cannot_save_artifacts_no_event_loop", level="WARN")
            except Exception as save_err:
                jlog("save_artifacts_before_close_exception", error=str(save_err), level="ERROR")

        # Fermeture context & Playwright
        if context:
            jlog("closing_context_finally", level="INFO")
            try:
                if not context.is_closed():
                    await context.close()
                    jlog("context_closed_finally", level="INFO")
                else:
                    jlog("context_already_closed_finally", level="DEBUG")
            except PlaywrightError as close_pw_err:
                if "Browser has been closed" in str(close_pw_err):
                    jlog("context_close_ignored_browser_already_closed", level="WARN")
                else:
                    jlog("context_close_playwright_error_finally", error=str(close_pw_err).split('\n')[0], level="WARN")
            except Exception as close_err:
                jlog("context_close_generic_error_finally", error=str(close_err).split('\n')[0], level="WARN")
            finally:
                context = None

        if playwright_instance:
            jlog("stopping_playwright_finally")
            try:
                await playwright_instance.stop()
                jlog("playwright_stopped_finally", level="INFO")
            except PlaywrightError as stop_pw_err:
                msg = str(stop_pw_err)
                if "Connection closed" in msg or "Browser has been closed" in msg:
                    jlog("playwright_stop_ignored_connection_already_closed", level="WARN")
                else:
                    jlog("playwright_stop_playwright_error_finally", error=str(stop_pw_err), level="WARN")
            except Exception as stop_err:
                jlog("playwright_stop_generic_error_finally", error=str(stop_err), level="WARN")

        final_log_reason = failure_reason if exit_code != 0 else "success"
        elapsed_s = time.time() - start_ts
        jlog("main_function_exit", code=exit_code, final_reason=final_log_reason, duration_s=round(elapsed_s, 2), level="INFO")
        return exit_code

# --- Point d'entrée ---
if __name__ == "__main__":
    final_exit_code = 1
    loop = None
    try:
        if sys.platform == 'win32':
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            if not isinstance(loop, asyncio.ProactorEventLoop):
                try:
                    loop.close()
                except Exception:
                    pass
                loop = asyncio.ProactorEventLoop()
                asyncio.set_event_loop(loop)
                jlog("proactor_event_loop_set_for_windows", level="DEBUG")
        else:
            loop = asyncio.get_event_loop_policy().get_event_loop()

        final_exit_code = loop.run_until_complete(main())
        jlog("script_exiting_normally", final_exit_code=final_exit_code, level="INFO")

    except KeyboardInterrupt:
        error_msg = "\nScript interrompu par l'utilisateur (Ctrl+C)."
        jlog("script_interrupted_by_user_main", level="WARN")
        print(error_msg, file=sys.stderr, flush=True)
        final_exit_code = 130
    except Exception as e:
        error_msg = f"ERREUR FATALE (bootstrap): {e}"
        jlog("bootstrap_fatal_error_main", error=str(e), error_type=type(e).__name__, stack=traceback.format_exc(), level="CRITICAL")
        print(error_msg, file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        final_exit_code = 1
    finally:
        jlog("script_exit_final", code=final_exit_code, level="INFO")
        try:
            sys.stdout.flush()
        except Exception:
            pass
        try:
            sys.stderr.flush()
        except Exception:
            pass
        if loop and not loop.is_closed() and not os.getenv("RUNNING_UNDER_SUPERVISOR"):
            try:
                loop.run_until_complete(asyncio.sleep(0.1))
                loop.run_until_complete(loop.shutdown_asyncgens())
            except RuntimeError as loop_err:
                if "Event loop is closed" not in str(loop_err):
                    jlog("event_loop_shutdown_error_finally", error=str(loop_err), level="WARN")
            finally:
                try:
                    if not loop.is_closed():
                        loop.close()
                        jlog("event_loop_closed_finally", level="DEBUG")
                except Exception as close_loop_err:
                    jlog("event_loop_close_error_finally", error=str(close_loop_err), level="WARN")
        time.sleep(0.2)
        sys.exit(final_exit_code)
