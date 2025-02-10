import signal
import traceback
from sys import stdout
from typing import List


class KillInstance:
    def kill(self):
        pass


class GracefulKiller:

    _instance: "GracefulKiller" = None

    is_killed: bool
    kill_instances: List[KillInstance]

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.kill_instances = []
        self.is_killed = False

    def exit_gracefully(self, *args):
        if self.is_killed:
            print("already killed. ignoring additional kill signal")
            return

        self.is_killed = True

        for instance in self.kill_instances:
            try:
                instance.kill()
            except:
                print("Failed to kill instance. Continuing")
                traceback.print_exc(file=stdout)

    @staticmethod
    def instance() -> "GracefulKiller":
        return GracefulKiller._instance

    @staticmethod
    def initialize() -> "GracefulKiller":
        if GracefulKiller._instance is not None:
            return GracefulKiller._instance

        GracefulKiller._instance = GracefulKiller()

        return GracefulKiller._instance

    @staticmethod
    def register_kill_instance(kill_instance: KillInstance):
        GracefulKiller._instance.kill_instances.append(kill_instance)
