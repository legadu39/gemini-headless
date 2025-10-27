# -*- coding: utf-8 -*-
"""
Sandbox profile management for Gemini Headless.

Goals (Critical Mode++ NASA):
- No plaintext cookie persistence by default; strong file protection (0600) and optional encryption.
- Preserve and extend public API surface expected by the rest of the project.

Public API (kept + compat):
- class SandboxProfile:
    - __init__(user_id: str, base_dir: Optional[str] = None, no_persist: bool = False, logger: Optional[logging.Logger] = None)
    - user_data_dir: str  (property)
    - profile_dir: str    (property)
    - dir: pathlib.Path   (property — alias for profile_dir as Path; kept for backward-compat with _engine.py)
    - cookies_path: str   (property — path to on-disk cookie bundle; internal format)
    - ensure_dirs() -> None
    - ensure_structure() -> None   # alias retained for backward-compat
    - write_cookies(cookies: list[dict], persist: Optional[bool] = None) -> None
    - read_cookies(default: Optional[list] = None) -> list
    - clear_cookies() -> None
    - exists() -> bool

Environment:
- SANDBOX_BASE_DIR: override root directory for sandbox profiles (optional).
- SANDBOX_COOKIE_KEY: optional base64-encoded key; if present and cryptography is installed, AES-GCM is used.
- SANDBOX_NO_PERSIST: "1" → default no-persist unless explicitly overridden in constructor.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import secrets
import stat
import sys
import typing as _t
from hashlib import blake2b
from pathlib import Path

# Optional AES-GCM if available; otherwise we fallback to permissions-only protection.
try:
    # cryptography is optional; we don't add it as a hard dependency.
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
    _HAS_CRYPTO = True
except Exception:  # pragma: no cover - environment without cryptography
    AESGCM = None  # type: ignore
    _HAS_CRYPTO = False


# ------------------------------
# Internal utilities
# ------------------------------

def _json_logger(logger: _t.Optional[logging.Logger]) -> _t.Callable[[str], None]:
    """
    Return a safe logging function that writes JSON lines to logger if provided,
    else falls back to sys.stderr (never stdout to preserve "answer-only" pipe).
    """
    def _emit(evt: str, **fields: _t.Any) -> None:
        payload = {"evt": evt, **fields}
        try:
            line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=lambda o: str(o))
        except Exception:
            # Best-effort serialization
            try:
                safe = {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None), list, dict)) else v)
                        for k, v in payload.items()}
                line = json.dumps(safe, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                line = json.dumps({"evt": evt, "unserializable": True}, ensure_ascii=False)
        if logger is not None and hasattr(logger, "info"):
            logger.info(line)
        else:
            # Always STDERR, never print() to STDOUT
            sys.stderr.write(line + "\n")
    return _emit


def _ensure_0600(path: Path) -> None:
    """
    Ensure POSIX-like 0600 permissions when supported.
    On Windows, chmod(0o600) is best-effort and may map to read-only flags.
    """
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        # Ignore if filesystem/OS does not support
        pass


def _atomic_write_bytes(target: Path, data: bytes) -> None:
    """
    Atomic-ish write: write to temp file, set 0600, then replace.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    _ensure_0600(tmp)
    os.replace(tmp, target)
    _ensure_0600(target)


def _load_bytes(path: Path) -> _t.Optional[bytes]:
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def _get_cookie_key_from_env() -> _t.Optional[bytes]:
    """
    Get optional encryption key from env (base64). Accepted names kept generic to avoid coupling.
    """
    key_b64 = os.getenv("SANDBOX_COOKIE_KEY") or os.getenv("GEMINI_COOKIE_KEY")
    if not key_b64:
        return None
    try:
        return base64.urlsafe_b64decode(key_b64.encode("utf-8"))
    except Exception:
        return None


def _xor_stream(key: bytes, length: int, nonce: bytes) -> bytes:
    """
    Lightweight keystream generator (NOT cryptographically strong; used only when AESGCM unavailable).
    We still prefer permissions (0600) and no-persist. This is a last-resort obfuscation to avoid casual
    plaintext at rest when an env key is provided but cryptography is absent.
    """
    h = blake2b(digest_size=32, key=key)
    block = b""
    counter = 0
    while len(block) < length:
        from hashlib import blake2b as _b2
        h2 = _b2(digest_size=32)
        h2.update(h.digest())
        h2.update(nonce)
        h2.update(counter.to_bytes(8, "big"))
        block += h2.digest()
        counter += 1
    return block[:length]


def _seal(cookies: _t.List[dict], key: _t.Optional[bytes]) -> dict:
    """
    Produce a sealed cookie bundle with metadata. Format:
      {
        "version": 2,
        "enc": "aesgcm" | "obf" | "none",
        "nonce": "<base64url>" (if enc != "none"),
        "data": "<base64url>"
      }
    """
    raw = json.dumps(cookies, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if key and _HAS_CRYPTO:
        # AES-GCM with 256-bit key if size fits; if not, derive to 32 bytes
        k = key if len(key) in (16, 24, 32) else blake2b(key, digest_size=32).digest()
        nonce = secrets.token_bytes(12)
        ct = AESGCM(k).encrypt(nonce, raw, None)
        return {
            "version": 2,
            "enc": "aesgcm",
            "nonce": base64.urlsafe_b64encode(nonce).decode("utf-8").rstrip("="),
            "data": base64.urlsafe_b64encode(ct).decode("utf-8").rstrip("="),
        }
    elif key:
        # Obfuscation stream (avoid trivial plaintext if key present but no AES)
        nonce = secrets.token_bytes(16)
        ks = _xor_stream(key, len(raw), nonce)
        obf = bytes(a ^ b for a, b in zip(raw, ks))
        return {
            "version": 2,
            "enc": "obf",
            "nonce": base64.urlsafe_b64encode(nonce).decode("utf-8").rstrip("="),
            "data": base64.urlsafe_b64encode(obf).decode("utf-8").rstrip("="),
        }
    else:
        # Permissions-only protection (0600). Explicit "none" mark.
        return {
            "version": 2,
            "enc": "none",
            "data": base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("="),
        }


def _open(bundle: dict, key: _t.Optional[bytes]) -> _t.List[dict]:
    enc = bundle.get("enc") or "none"
    data_b64 = bundle.get("data")
    if not isinstance(data_b64, str):
        return []
    data = base64.urlsafe_b64decode(data_b64 + "==")
    if enc == "aesgcm":
        nonce_b64 = bundle.get("nonce") or ""
        if not key or not _HAS_CRYPTO or not nonce_b64:
            return []
        k = key if len(key) in (16, 24, 32) else blake2b(key, digest_size=32).digest()
        nonce = base64.urlsafe_b64decode(nonce_b64 + "==")
        try:
            raw = AESGCM(k).decrypt(nonce, data, None)
        except Exception:
            return []
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return []
    elif enc == "obf":
        if not key:
            return []
        nonce_b64 = bundle.get("nonce") or ""
        if not nonce_b64:
            return []
        nonce = base64.urlsafe_b64decode(nonce_b64 + "==")
        ks = _xor_stream(key, len(data), nonce)
        raw = bytes(a ^ b for a, b in zip(data, ks))
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return []
    else:
        # permissions-only
        try:
            raw = base64.urlsafe_b64decode(data_b64 + "==")
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return []


# ------------------------------
# Public class
# ------------------------------

class SandboxProfile:
    """
    Manage per-user sandbox directories and cookie persistence with safety defaults.
    """

    def __init__(
        self,
        user_id: str,
        base_dir: _t.Optional[str] = None,
        no_persist: bool = False,
        logger: _t.Optional[logging.Logger] = None,
    ) -> None:
        self.user_id = user_id
        # Base directory precedence: ctor arg > env > ~/.gemini_sandbox
        env_base = os.getenv("SANDBOX_BASE_DIR")
        self._base_dir = Path(base_dir or env_base or Path.home() / ".gemini_sandbox").resolve()
        self._profile_dir = self._base_dir / f"user_{user_id}"
        self._userdata_dir = self._profile_dir / "user_data"
        self._cookies_file = self._profile_dir / "cookies.bundle.json"  # internal container (encrypted/obfuscated/plain-marked)
        # Backward-compat alias (if other modules expect 'cookies.json' to exist). We write the same bundle to both.
        self._cookies_file_compat = self._profile_dir / "cookies.json"
        # no_persist precedence: env may force it when set to "1".
        env_no_persist = (os.getenv("SANDBOX_NO_PERSIST") or "").strip() == "1"
        self.no_persist = bool(no_persist or env_no_persist)
        self._logger = logger
        self._emit = _json_logger(logger)
        self._mem_cache: _t.Optional[_t.List[dict]] = None  # RAM cache when no_persist or before first flush
        # Optional encryption key for at-rest protection
        self._cookie_key: _t.Optional[bytes] = _get_cookie_key_from_env()

    # --- properties ---

    @property
    def profile_dir(self) -> str:
        return str(self._profile_dir)

    @property
    def dir(self) -> Path:
        """Backward-compat alias expected by some callers (e.g., _engine.py)."""
        return self._profile_dir

    @property
    def user_data_dir(self) -> str:
        return str(self._userdata_dir)

    @property
    def cookies_path(self) -> str:
        # Expose primary path; internal format is a JSON bundle with metadata.
        return str(self._cookies_file)

    # --- lifecycle ---

    def ensure_dirs(self) -> None:
        self._profile_dir.mkdir(parents=True, exist_ok=True)
        self._userdata_dir.mkdir(parents=True, exist_ok=True)
        # Harden directory permissions when possible
        try:
            os.chmod(self._profile_dir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            os.chmod(self._userdata_dir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except Exception:
            pass
        self._emit("sandbox_dirs_ready", user=self.user_id, profile=self.profile_dir, user_data=self.user_data_dir, no_persist=self.no_persist)

    # Backward-compat alias (expected by _engine.py)
    def ensure_structure(self) -> None:
        """
        Backward-compatible alias to ensure_dirs().
        NOTE: Keep this method to avoid breaking older callers.
        """
        self.ensure_dirs()

    def exists(self) -> bool:
        return self._profile_dir.exists()

    # --- cookies ---

    def write_cookies(self, cookies: _t.List[dict], persist: _t.Optional[bool] = None) -> None:
        """
        Persist cookies securely (if requested) or only keep them in memory when no_persist=True.
        When persisted:
          - File permissions are forced to 0600 (best-effort on non-POSIX).
          - If SANDBOX_COOKIE_KEY is set and 'cryptography' is available, AES-GCM is used.
          - If SANDBOX_COOKIE_KEY is set without 'cryptography', an obfuscation stream is used (non-cryptographic).
          - Otherwise, bundle is marked 'enc':'none' but still protected by 0600.
        """
        if persist is None:
            persist = not self.no_persist

        self._mem_cache = list(cookies)  # keep in RAM for current run
        if not persist:
            # Remove any on-disk artifacts to honor no-persist
            try:
                if self._cookies_file.exists():
                    self._cookies_file.unlink()
                if self._cookies_file_compat.exists():
                    self._cookies_file_compat.unlink()
            except Exception:
                pass
            self._emit("cookies_written", user=self.user_id, persisted=False, count=len(cookies))
            return

        # Seal and write bundle
        bundle = _seal(cookies, self._cookie_key)
        data = json.dumps(bundle, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        _atomic_write_bytes(Path(self._cookies_file), data)
        # Maintain backward-compatibility file if other modules read 'cookies.json' directly
        _atomic_write_bytes(Path(self._cookies_file_compat), data)
        self._emit(
            "cookies_written",
            user=self.user_id,
            persisted=True,
            count=len(cookies),
            enc=bundle.get("enc", "none"),
            file=self.cookies_path,
        )

    def read_cookies(self, default: _t.Optional[_t.List[dict]] = None) -> _t.List[dict]:
        """
        Read cookies from RAM cache first (if any), otherwise from the sealed bundle file.
        If the bundle cannot be opened (missing or key mismatch), returns 'default' or [].
        """
        if self._mem_cache is not None:
            return list(self._mem_cache)

        data = _load_bytes(Path(self._cookies_file))
        if data is None:
            # Fallback to compat path if primary doesn't exist
            data = _load_bytes(Path(self._cookies_file_compat))
            if data is None:
                return list(default or [])

        try:
            bundle = json.loads(data.decode("utf-8"))
        except Exception:
            self._emit("cookies_read_error", user=self.user_id, reason="json_decode_failed")
            return list(default or [])

        cookies = _open(bundle, self._cookie_key)
        if not isinstance(cookies, list):
            cookies = []
        self._emit(
            "cookies_read",
            user=self.user_id,
            persisted=True,
            count=len(cookies),
            enc=bundle.get("enc", "unknown"),
            file=self.cookies_path,
        )
        # Populate RAM cache for subsequent calls
        self._mem_cache = list(cookies)
        return cookies

    def clear_cookies(self) -> None:
        """
        Remove any persisted cookies and clear RAM cache.
        """
        self._mem_cache = None
        removed = []
        for p in (self._cookies_file, self._cookies_file_compat):
            try:
                if Path(p).exists():
                    Path(p).unlink()
                    removed.append(str(p))
            except Exception:
                pass
        self._emit("cookies_cleared", user=self.user_id, removed=removed)

# ✅ Correctif(s) appliqué(s):
# - dir (Path) ajouté pour compat avec _engine.py (évite TypeError lors de l'opérateur '/').
# - Cookies non persistants par défaut si SANDBOX_NO_PERSIST=1 (option no-persist).
# - Protection des fichiers par permissions 0600 (best-effort cross-OS).
# - Chiffrement AES-GCM utilisé automatiquement si SANDBOX_COOKIE_KEY est défini et 'cryptography' disponible ; sinon obfuscation légère.
# - Aucun print(): logs JSON unifiés via logger ou STDERR (jamais STDOUT).
# - Maintien de l’API publique et compat héritée : ensure_structure() et dir (Path).
# - Compat: écriture du même bundle sur cookies.bundle.json et cookies.json (pour anciens lecteurs).
