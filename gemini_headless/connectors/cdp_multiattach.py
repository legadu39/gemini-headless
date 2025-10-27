# gemini_headless/connectors/cdp_multiattach.py
# CORRIGÉ : Ajout log de diagnostic au début du wrapper callback CDP
from __future__ import annotations
import asyncio
import traceback # <-- Ajout pour tracebacks
from typing import Callable, Dict, List, Optional
from playwright.async_api import Page, Frame, CDPSession

try:
    from ..collect.utils.logs import jlog
except Exception:
    try: from ..utils.logs import jlog
    except Exception:
        try: from utils.logs import jlog
        except Exception:
            # Fallback logger minimaliste mais fonctionnel
            import sys, json, time
            _jlog_fallback_cache = {}
            def jlog(*_a, evt="unknown_cdpmt_fallback", level="INFO", **_k):
                if not _jlog_fallback_cache.get("used"):
                    print("WARNING: Using fallback jlog in cdp_multiattach.py", file=sys.stderr)
                    _jlog_fallback_cache["used"] = True
                try:
                    payload = {"evt": evt, "ts": time.time(), "level": level, "module": "cdp_multiattach.py", **_k}
                    print(json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr)
                    sys.stderr.flush()
                except Exception:
                    print(f'{{"evt": "jlog_cdpmt_fallback_error", "original_evt": "{evt}"}}', file=sys.stderr)
                    sys.stderr.flush()


class CDPMultiTarget:
    """
    Ecoute Network.* via CDP sessions sur la Page et toutes ses Frames.
    NASA++ V3: Logging événementiel détaillé et gestion d'erreurs robuste.
    """
    EVENTS = {
        "Network.requestWillBeSent",
        "Network.responseReceived",
        "Network.dataReceived",
        "Network.loadingFinished",
        "Network.webSocketFrameReceived",
        "Network.webSocketClosed",
        "Network.eventSourceMessageReceived", # <-- L'événement clé
    }

    def __init__(self, page: Page, poll_interval: float = 0.5) -> None:
        self.page = page
        # Vérifier si context existe et est valide
        try:
            self.ctx = page.context
            if not self.ctx: raise AttributeError("Page context is None")
        except Exception as e:
            jlog("cdpmt_init_error", error="Failed to get page context", details=str(e), level="ERROR")
            raise ValueError("Invalid page or context provided to CDPMultiTarget") from e

        self.poll_interval = poll_interval # Non utilisé actuellement
        self._sessions: Dict[str, CDPSession] = {} # Utiliser un dict pour gérer par ID de session ou frame ID
        self._listeners: Dict[str, List[Callable[[Dict], None]]] = {ev: [] for ev in self.EVENTS}
        self._watch_task: Optional[asyncio.Task] = None # Non utilisé actuellement
        self._closed = False
        self._on_frame_attached_handler = None # Stocker la référence pour .off()
        self._on_frame_detached_handler = None # Stocker la référence pour .off()
        self._session_tags: Dict[str, str] = {} # Associer un tag (page/frame_id) à une session
        jlog("cdpmt_init", object_id=id(self))


    async def start(self) -> None:
        if self._closed:
             jlog("cdpmt_start_ignored_closed", level="WARN")
             return
        if self._sessions:
             jlog("cdpmt_start_ignored_already_started", active_sessions=len(self._sessions), level="WARN")
             return

        jlog("cdpmt_start_begin")
        # Page principale
        try:
            page_session = await self.ctx.new_cdp_session(self.page)
            session_id = getattr(page_session, '_guid', f'page_{id(page_session)}') # Utiliser _guid si disponible
            tag = f"page_{self.page.main_frame.name or 'main'}"
            await self._prime_session(page_session, tag=tag, session_id=session_id)
            self._sessions[session_id] = page_session
            self._session_tags[session_id] = tag
            jlog("cdp_attach_page_ok", session_id=session_id, tag=tag)
        except Exception as e:
            jlog("cdp_attach_page_error", error=str(e), error_type=type(e).__name__, traceback=traceback.format_exc(limit=3), level="ERROR")
            # Ne pas s'arrêter ici, tenter les frames

        # Frames déjà présentes
        await self._attach_all_frames()

        # Watch frames dynamiques
        async def _on_frame_attached_async(frame: Frame) -> None:
            if self._closed or not frame or frame.is_detached(): return
            frame_id = getattr(frame, '_guid', f'frame_{id(frame)}')
            tag = f"frame_{frame.name or frame_id[-6:]}"
            jlog("cdpmt_frame_attached_event", frame_id=frame_id, url=frame.url, tag=tag)
            try:
                # Vérifier si une session existe déjà pour cette frame (peu probable mais possible)
                if frame_id in self._sessions:
                     jlog("cdpmt_attach_frame_skipped_exists", frame_id=frame_id, tag=tag, level="WARN")
                     return

                frame_session = await self.ctx.new_cdp_session(frame)
                session_id = getattr(frame_session, '_guid', f'session_{id(frame_session)}')
                await self._prime_session(frame_session, tag=tag, session_id=session_id)
                self._sessions[session_id] = frame_session
                self._session_tags[session_id] = tag
                jlog("cdp_attach_frame_ok", frame_id=frame_id, session_id=session_id, tag=tag, url=frame.url)
            except Exception as e:
                msg = str(e)
                # Playwright peut gérer certaines frames via la session parent
                if "part of the parent frame's session" in msg:
                    jlog("cdp_frame_shared_session", frame_id=frame_id, tag=tag, url=frame.url, level="INFO")
                else:
                    jlog("cdp_attach_frame_error", frame_id=frame_id, tag=tag, url=frame.url, error=msg, error_type=type(e).__name__, level="ERROR")

        # Wrapper synchrone pour le handler d'événement Playwright
        def on_frame_attached_sync(frame: Frame) -> None:
            # Lancer la coroutine sans l'attendre pour ne pas bloquer le handler
            if not self._closed:
                asyncio.create_task(_on_frame_attached_async(frame))

        def on_frame_detached_sync(frame: Frame) -> None:
            if self._closed: return
            frame_id = getattr(frame, '_guid', f'frame_{id(frame)}')
            tag = f"frame_{frame.name or frame_id[-6:]}"
            jlog("cdpmt_frame_detached_event", frame_id=frame_id, tag=tag, url=frame.url)
            # Tenter de détacher la session associée si on la trouve
            session_to_remove_id = None
            for s_id, s_tag in self._session_tags.items():
                if s_tag == tag: # Comparaison par tag (peut être imprécis si noms dupliqués)
                    session_to_remove_id = s_id
                    break
            if session_to_remove_id and session_to_remove_id in self._sessions:
                 session = self._sessions.pop(session_to_remove_id)
                 self._session_tags.pop(session_to_remove_id, None)
                 jlog("cdpmt_detaching_session_for_frame", session_id=session_to_remove_id, tag=tag)
                 try: asyncio.create_task(session.detach()) # Détacher en tâche de fond
                 except Exception as detach_err: jlog("cdpmt_detach_session_error", session_id=session_to_remove_id, tag=tag, error=str(detach_err), level="WARN")
            # else: jlog("cdpmt_session_not_found_for_detached_frame", frame_id=frame_id, tag=tag, level="DEBUG")


        # Stocker les références pour pouvoir les détacher plus tard
        self._on_frame_attached_handler = on_frame_attached_sync
        self._on_frame_detached_handler = on_frame_detached_sync

        try:
            self.page.on("frameattached", self._on_frame_attached_handler)
            self.page.on("framedetached", self._on_frame_detached_handler)
            jlog("cdpmt_frame_watchers_attached")
        except Exception as e:
            jlog("cdp_page_watch_frames_error", error=str(e), error_type=type(e).__name__, level="ERROR")

        # Service Workers ne sont pas gérés via CDP Session par Playwright standard
        jlog("cdp_attach_sw_skip", reason="Not supported by standard Playwright CDP sessions")
        jlog("cdpmt_start_complete", initial_session_count=len(self._sessions))

    async def stop(self) -> None:
        jlog("cdpmt_stop_begin", session_count=len(self._sessions))
        self._closed = True # Marquer comme fermé immédiatement

        # Détacher les listeners d'événements de frame
        try:
            if self._on_frame_attached_handler and hasattr(self.page, "remove_listener"):
                self.page.remove_listener("frameattached", self._on_frame_attached_handler)
            if self._on_frame_detached_handler and hasattr(self.page, "remove_listener"):
                self.page.remove_listener("framedetached", self._on_frame_detached_handler)
            jlog("cdpmt_frame_watchers_detached")
        except Exception as e:
            jlog("cdpmt_remove_frame_listeners_error", error=str(e), level="WARN")

        # Copier les IDs de session avant d'itérer car le dict peut changer
        session_ids_to_detach = list(self._sessions.keys())
        jlog("cdpmt_detaching_sessions", count=len(session_ids_to_detach))

        detach_tasks = []
        for session_id in session_ids_to_detach:
            session = self._sessions.pop(session_id, None)
            tag = self._session_tags.pop(session_id, "unknown")
            if session:
                async def detach_session(s, sid, t):
                    try: await s.detach()
                    except Exception as detach_e: jlog("cdpmt_session_detach_error", session_id=sid, tag=t, error=str(detach_e), level="WARN")
                detach_tasks.append(detach_session(session, session_id, tag))

        # Attendre la fin des détachements avec un timeout
        if detach_tasks:
            try: await asyncio.wait_for(asyncio.gather(*detach_tasks), timeout=5.0)
            except asyncio.TimeoutError: jlog("cdpmt_detach_gather_timeout", level="WARN")
            except Exception as gather_e: jlog("cdpmt_detach_gather_error", error=str(gather_e), level="WARN")

        self._sessions.clear() # Assurer la vidange finale
        self._session_tags.clear()
        jlog("cdpmt_stop_complete")

    def on(self, event_name: str, callback: Callable[[Dict], None]) -> None:
        if event_name not in self._listeners:
            # Logguer mais ne pas lever d'exception pour la robustesse
            jlog("cdpmt_unsupported_event_listener", event_name=event_name, level="ERROR")
            return
            # raise ValueError(f"Unsupported event: {event_name}")
        if callback not in self._listeners[event_name]:
             self._listeners[event_name].append(callback)
             jlog("cdpmt_listener_added", event_name=event_name, callback_name=getattr(callback, '__name__', 'unnamed'))


    async def _attach_all_frames(self) -> None:
        jlog("cdpmt_attach_all_frames_start")
        frames_to_process: List[Frame] = []
        try:
            # Récupérer frames de manière robuste
            if self.page and not self.page.is_closed():
                main_f = self.page.main_frame
                if main_f and not main_f.is_detached():
                     frames_to_process.append(main_f) # Ajouter main frame explicitement s'il n'y est pas déjà via page
                     try: frames_to_process.extend([cf for cf in main_f.child_frames if cf and not cf.is_detached()])
                     except Exception as child_e: jlog("cdpmt_attach_all_frames_child_error", error=str(child_e), level="WARN")
            jlog("cdpmt_attach_all_frames_found", count=len(frames_to_process))
        except Exception as e:
            jlog("cdpmt_attach_all_frames_list_error", error=str(e), error_type=type(e).__name__, level="ERROR")
            return

        for fr in frames_to_process:
            if self._closed or not fr or fr.is_detached(): continue # Vérifier état à chaque itération
            frame_id = getattr(fr, '_guid', f'frame_{id(fr)}')
            tag = f"frame_{fr.name or frame_id[-6:]}"
            # Ne pas recréer si déjà présent (ex: main frame ajoutée par start())
            session_exists = any(s_tag == tag for s_tag in self._session_tags.values())
            if session_exists:
                 jlog("cdpmt_attach_all_frames_skip_existing", tag=tag, url=fr.url, level="DEBUG")
                 continue

            try:
                frame_session = await self.ctx.new_cdp_session(fr)
                session_id = getattr(frame_session, '_guid', f'session_{id(frame_session)}')
                await self._prime_session(frame_session, tag=tag, session_id=session_id)
                self._sessions[session_id] = frame_session
                self._session_tags[session_id] = tag
                jlog("cdp_attach_frame_ok", frame_id=frame_id, session_id=session_id, tag=tag, url=fr.url)
            except Exception as e:
                msg = str(e)
                if "part of the parent frame's session" in msg: jlog("cdp_frame_shared_session", frame_id=frame_id, tag=tag, url=fr.url, level="INFO")
                else: jlog("cdp_attach_frame_error", frame_id=frame_id, tag=tag, url=fr.url, error=msg, error_type=type(e).__name__, level="ERROR")
        jlog("cdpmt_attach_all_frames_complete")

    async def _prime_session(self, session: CDPSession, *, tag: str, session_id: str) -> None:
        """Active Network domain and wires up listeners for a single session."""
        jlog("cdpmt_prime_session_start", session_id=session_id, tag=tag)
        if not session or getattr(session, '_connection', None) is None:
             jlog("cdpmt_prime_session_invalid_session", session_id=session_id, tag=tag, level="ERROR")
             return

        try:
            # Activer le domaine Network (essentiel)
            await session.send("Network.enable", {})
            jlog("cdpmt_network_enable_ok", session_id=session_id, tag=tag)
        except Exception as e:
            # Logguer l'erreur mais continuer d'attacher les listeners
            jlog("cdpmt_network_enable_error", session_id=session_id, tag=tag, error=str(e), error_type=type(e).__name__, level="ERROR")

        # --- Fonction interne pour créer le callback CDP avec logging et robustesse ---
        def _wire(ev: str) -> Callable[[Dict], None]:
            # Utiliser session_id et tag du scope externe
            current_session_id = session_id
            current_tag = tag

            def _cdp_callback_wrapper(params: Dict) -> None:
                # *** AJOUT LOG DIAGNOSTIC ICI ***
                # Loggue l'invocation brute avant tout traitement ou filtrage
                jlog("cdpmt_callback_invoked_raw", event=ev, session_id=current_session_id, tag=current_tag, params_head=str(params)[:150], level="DEBUG")

                # Appeler tous les listeners enregistrés pour cet événement
                listeners_for_event = list(self._listeners.get(ev, [])) # Copier la liste
                if not listeners_for_event:
                    # jlog("cdpmt_callback_no_listeners", event=ev, session_id=current_session_id, tag=current_tag, level="DEBUG")
                    return

                for fn in listeners_for_event:
                    try:
                        # Appeler le callback du producteur (ex: _on_sse_message)
                        res = fn(params)
                        # Gérer les coroutines retournées par le callback
                        if asyncio.iscoroutine(res):
                            # Lancer en tâche de fond pour ne pas bloquer
                            asyncio.create_task(res)
                            # jlog("cdpmt_callback_coroutine_started", event=ev, session_id=current_session_id, tag=current_tag, func_name=getattr(fn, '__name__', 'unnamed'), level="DEBUG")
                    # *** GESTION ROBUSTE DES ERREURS DANS LE CALLBACK DU PRODUCTEUR ***
                    except Exception as callback_err:
                        jlog("cdpmt_producer_callback_error", event=ev, session_id=current_session_id, tag=current_tag, func_name=getattr(fn, '__name__', 'unnamed'),
                             error=str(callback_err), error_type=type(callback_err).__name__, traceback=traceback.format_exc(limit=3), level="ERROR")
                        # Ne PAS arrêter le traitement des autres listeners ou événements

            return _cdp_callback_wrapper
        # --- Fin fonction interne ---

        # Attacher les listeners pour les événements définis
        for ev_name in self.EVENTS:
            try:
                # Créer le wrapper spécifique pour cet event/session/tag
                callback_wrapper = _wire(ev_name)
                # Utiliser session.on pour attacher
                session.on(ev_name, callback_wrapper) # type: ignore[attr-defined] # Playwright ajoute .on dynamiquement
                jlog("cdpmt_event_listener_wired", event=ev_name, session_id=session_id, tag=tag)
            except Exception as e_on:
                # Logguer l'échec de l'attachement mais continuer avec les autres
                jlog("cdpmt_wire_listener_error", event=ev_name, session_id=session_id, tag=tag, error=str(e_on), error_type=type(e_on).__name__, level="ERROR")

        jlog("cdpmt_prime_session_complete", session_id=session_id, tag=tag)