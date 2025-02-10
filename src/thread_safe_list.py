from threading import Lock
from typing import MutableSequence, TypeVar

T = TypeVar("T")

# A thread-safe cache that acts like a normal dict object
class ThreadSafeList(MutableSequence[T]):

    _write_lock: Lock

    def __init__(self, init=None):
        super(ThreadSafeList, self).__init__()

        self._write_lock = Lock()

        if init is not None:
            self._list = list(init)
        else:
            self._list = list()

    def __delitem__(self, ii):
        with self._write_lock:
            del self._list[ii]

    def __setitem__(self, ii, val):
        # optional: self._acl_check(val)
        with self._write_lock:
            self._list[ii] = val

    def append(self, val):
        self.insert(len(self._list), val)

    def insert(self, ii, val):
        # optional: self._acl_check(val)
        with self._write_lock:
            self._list.insert(ii, val)

    def clear(self) -> MutableSequence[T]:
        with self._write_lock:
            _items = list(self._list)
            self._list.clear()

            return _items

    def __repr__(self):
        return "<{0} {1}>".format(self.__class__.__name__, self._list)

    def __len__(self):
        """List length"""
        return len(self._list)

    def __getitem__(self, ii):
        """Get a list item"""
        return self._list[ii]

    def __str__(self):
        return str(self._list)
