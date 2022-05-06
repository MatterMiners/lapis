from typing import Callable, Coroutine
from functools import wraps

from usim import run

from lapis.workernode import WorkerNode


def via_usim(test_case: Callable[..., Coroutine]):
    """
    Mark an ``async def`` test case to be run via ``usim.run``

    .. code:: python3

        @via_usim
        async def test_sleep():
            before = time.now
            await (time + 20)
            after = time.now
            assert after - before == 20
    """

    @wraps(test_case)
    def run_test(*args, **kwargs):
        # pytest currently ignores __tracebackhide__ if we re-raise
        # https://github.com/pytest-dev/pytest/issues/1904
        __tracebackhide__ = True
        # >>> This is not the frame you are looking for. Do read on. <<<
        return run(test_case(*args, **kwargs))

    return run_test


class DummyScheduler:
    @staticmethod
    def register_drone(drone: WorkerNode):
        pass

    @staticmethod
    def unregister_drone(drone: WorkerNode):
        pass

    @staticmethod
    def update_drone(drone: WorkerNode):
        pass


class DummyDrone:
    pass
