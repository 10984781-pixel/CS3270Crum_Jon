"""Per-session in-memory dataset store."""

from __future__ import annotations

from threading import Lock


class SessionDataStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._store = {}

    def set_dataframe(self, session_id: str, dataframe) -> None:
        with self._lock:
            self._store[session_id] = dataframe.copy()

    def get_dataframe(self, session_id: str):
        with self._lock:
            dataframe = self._store.get(session_id)
            if dataframe is None:
                return None
            return dataframe.copy()

    def clear(self, session_id: str) -> None:
        with self._lock:
            self._store.pop(session_id, None)


session_data_store = SessionDataStore()
