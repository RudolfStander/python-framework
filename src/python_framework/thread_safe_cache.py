from threading import Lock
from typing import Dict, Generic, List, Tuple, TypeVar

K = TypeVar("K")
V = TypeVar("V")

# A thread-safe cache that acts like a normal dict object
class ThreadSafeCache(Generic[K, V]):

    _cache: Dict[K, V]
    _write_lock: Lock

    def __init__(self, init: Dict[K, V] = None):
        self._cache = {}
        self._write_lock = Lock()

        if init is not None:
            self._cache.update(init)

    def values(self) -> List[V]:
        return self._cache.values()

    def keys(self) -> List[K]:
        return self._cache.keys()

    def items(self) -> List[Tuple[K, V]]:
        return self._cache.items()

    def __getitem__(self, key: K) -> V:
        return self._cache[key]

    def __setitem__(self, key: K, value: V):
        with self._write_lock:
            self._cache[key] = value

    def __delitem__(self, key: K):
        with self._write_lock:
            del self._cache[key]

    def __contains__(self, key) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)

    def __repr__(self) -> str:
        return repr(self._cache)
