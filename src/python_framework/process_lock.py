from advanced_threading import synchronized_method
from time import utc_now


class LockInstance(object):

    process_key: str
    lock_granted_at: str
    process_lock_instance = None

    lock_released_at: str
    releasing: bool

    def __init__(self, process_key: str, process_lock_instance):
        self.process_key = process_key
        self.lock_granted_at = utc_now()
        self.process_lock_instance = process_lock_instance

        self.lock_released_at = None
        self.releasing = False

    def release(self):
        if self.lock_released_at is not None or self.releasing:
            return

        self.releasing = True

        self.process_lock_instance.release_instance(self)
        self.lock_released_at = utc_now()
        self.releasing = False


# TODO: add "lock_or_wait" method


class ProcessLock(object):

    process_locks: dict
    lock_for: str

    def __init__(self, lock_for: str = None):
        self.lock_for = lock_for
        self.process_locks = {}

    def __str__(self):
        return "[ProcessLock%s]" % (
            "" if self.lock_for is None else " - %s" % self.lock_for
        )

    # TODO: synchronize on process_key argument
    @synchronized_method
    def lock(self, process_key: str):
        if (
            process_key in self.process_locks
            and self.process_locks[process_key] is not None
        ):
            lock_instance = self.process_locks[process_key]
            print(
                "WARN - %s lock already granted for process with key [%s] at [%s]"
                % (str(self), process_key, lock_instance.lock_granted_at)
            )
            return None

        lock_instance = LockInstance(process_key, self)
        self.process_locks[process_key] = lock_instance

        return lock_instance

    def release(self, process_key: str):
        if process_key not in self.process_locks:
            return

        if self.process_locks[process_key] is None:
            return

        self.process_locks[process_key].release()

    def release_instance(self, lock_instance: LockInstance):
        if lock_instance.process_key not in self.process_locks:
            return

        if self.process_locks[lock_instance.process_key] is None:
            return

        self.process_locks[lock_instance.process_key] = None
