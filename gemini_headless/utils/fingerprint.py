# -*- coding: utf-8 -*-
import json, random
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass

UAS: List[str] = [
    # Chrome 123–128 Windows 10/11 — éventail plausible
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
]

COMMON_SCREENS: List[Tuple[int, int]] = [(1920,1080), (1366,768), (1536,864), (2560,1440)]
COMMON_TZS: List[str] = ["Europe/Paris","Europe/Madrid","Europe/Berlin","Europe/Rome"]

@dataclass
class Fingerprint:
    user_agent: str
    webgl_vendor: str = "Google Inc. (Intel)"
    renderer: str = "ANGLE (Intel, Intel(R) UHD Graphics 630, D3D11)"
    platform: str = "Win32"
    screen: Tuple[int, int] = (1920,1080)
    timezone: str = "Europe/Paris"
    fonts: List[str] = None  # type: ignore

    @staticmethod
    def load_or_seed(profile, policy: str = "stable"):
        """
        Charge un fingerprint stable si présent, sinon en génère un et l'écrit.
        """
        path = profile.dir / "fingerprint.json"
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists() and policy == "stable":
            return Fingerprint(**json.loads(path.read_text(encoding="utf-8")))

        ua = random.choice(UAS)
        screen = random.choice(COMMON_SCREENS)
        tz = random.choice(COMMON_TZS)
        fonts = ["Segoe UI", "Arial", "Times New Roman", "Calibri", "Monaco"]
        fp = Fingerprint(user_agent=ua, screen=screen, timezone=tz, fonts=fonts)
        path.write_text(json.dumps(fp.__dict__, ensure_ascii=False, indent=2), encoding="utf-8")
        return fp


def build_launch_args(fp: Fingerprint, proxy: Optional[Dict[str, str]] = None, timezone: Optional[str] = None):
    """
    Arguments Chromium furtifs et cohérents avec le fingerprint.
    """
    args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-features=Translate,IsolateOrigins,site-per-process",
        "--no-first-run",
        "--password-store=basic",
        "--lang=fr-FR",
        "--autoplay-policy=no-user-gesture-required",
        "--disable-dev-shm-usage",
    ]
    if timezone or fp.timezone:
        args += [f"--force-timezone={timezone or fp.timezone}"]
    if proxy and "server" in proxy:
        args += [f'--proxy-server={proxy["server"]}']
    # cohérence écran
    w, h = fp.screen
    args += [f"--window-size={w},{h}"]
    return args
