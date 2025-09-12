import aiohttp
import asyncio
from typing import Dict, Optional

import update_task_for_approval


class SessionManager:
    """Maintain reusable aiohttp ClientSession objects keyed by account.

    Sessions are created on demand and persist for the lifetime of the
    application. Call :func:`close_all` during application shutdown to ensure all
    sessions are properly closed.
    """

    def __init__(self) -> None:
        self._sessions: Dict[Optional[str], aiohttp.ClientSession] = {}
        self._lock = asyncio.Lock()

    async def get_session(self, account_id: Optional[str] = None) -> aiohttp.ClientSession:
        """Return an active ClientSession for *account_id*.

        If a session does not yet exist or has been closed it will be created
        using the timeout settings defined in ``update_task_for_approval``.
        """
        key = account_id or "shared"
        async with self._lock:
            session = self._sessions.get(key)
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=update_task_for_approval.HTTP_TIMEOUT)
                self._sessions[key] = session
            return session

    async def release_session(self, account_id: Optional[str] = None) -> None:
        """Placeholder for interface symmetry; sessions are reused."""
        # Currently sessions are long-lived so there is nothing to do here.
        # This hook exists to support future pooling or reference counting.
        return None

    async def close_all(self) -> None:
        """Close all managed ClientSession instances."""
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            if not session.closed:
                await session.close()


session_manager = SessionManager()
