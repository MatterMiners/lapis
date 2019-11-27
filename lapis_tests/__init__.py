from typing import Callable, Coroutine
from functools import wraps

from usim import run

from lapis.drone import Drone


class UnfinishedTest(RuntimeError):
    """A test did never finish"""

    def __init__(self, test_case):
        self.test_case = test_case
        super().__init__(
            "Test case %r did not finish" % getattr(test_case, "__name__", test_case)
        )


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
        test_completed = False

        async def complete_test_case():
            nonlocal test_completed
            await test_case(*args, **kwargs)
            test_completed = True

        run(complete_test_case())
        if not test_completed:
            raise UnfinishedTest(test_case)

    return run_test


class DummyScheduler:
    @staticmethod
    def register_drone(drone: Drone):
        pass

    @staticmethod
    def unregister_drone(drone: Drone):
        pass

    @staticmethod
    def update_drone(drone: Drone):
        pass


class DummyDrone:
    connection = None
