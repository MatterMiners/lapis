from typing import Generator, Callable
from cobald import interfaces
from usim import eternity, Scope, interval

from .drone import Drone


class Pool(interfaces.Pool):
    """
    A pool encapsulating a number of pools or drones. Given a specific demand,
    allocation and utilisation, the pool is able to adapt in terms of number of
    drones providing the given resources.

    :param capacity: Maximum number of pools that can be instantiated within the pool
    :param init: Number of pools to instantiate at creation time of the pool
    :param name: Name of the pool
    :param make_drone: Callable to create a drone with specific properties for this pool
    """

    def __init__(
        self,
        make_drone: Callable,
        *,
        capacity: int = float("inf"),
        init: int = 0,
        name: str = None,
    ):
        super(Pool, self).__init__()
        assert init <= capacity
        self.make_drone = make_drone
        self._drones = []
        self.init_pool(init=init)
        self._demand = 1
        self._level = init
        self._capacity = capacity
        self._name = name

    def init_pool(self, init: int = 0):
        """
        Initialisation of existing drones at creation time of pool.

        :param init: Number of drones to create.
        """
        for _ in range(init):
            self._drones.append(self.make_drone(0))

    # TODO: the run method currently needs to be called manually
    async def run(self):
        """
        Pool periodically checks the current demand and provided drones.
        If demand is higher than the current level, the pool takes care of
        initialising new drones. Otherwise drones get removed.
        """
        async with Scope() as scope:
            async for _ in interval(1):
                drones_required = min(self._demand, self._capacity) - self._level
                while drones_required > 0:
                    drones_required -= 1
                    # start a new drone
                    drone = self.make_drone(10)
                    scope.do(drone.run())
                    self._drones.append(drone)
                    self._level += 1
                if drones_required < 0:
                    for drone in self.drones:
                        if drone.jobs == 0:
                            drones_required += 1
                            self._level -= 1
                            self._drones.remove(drone)
                            scope.do(drone.shutdown())
                            if drones_required == 0:
                                break

    @property
    def drones(self) -> Generator[Drone, None, None]:
        for drone in self._drones:
            if drone.supply > 0:
                yield drone

    def drone_demand(self) -> int:
        return len(self._drones)

    @property
    def allocation(self) -> float:
        allocations = []
        for drone in self._drones:
            allocations.append(drone.allocation)
        try:
            return sum(allocations) / len(allocations)
        except ZeroDivisionError:
            return 1

    @property
    def utilisation(self) -> float:
        utilisations = []
        for drone in self._drones:
            utilisations.append(drone.utilisation)
        try:
            return sum(utilisations) / len(utilisations)
        except ZeroDivisionError:
            return 1

    @property
    def supply(self) -> float:
        supply = 0
        for drone in self._drones:
            supply += drone.supply
        return supply

    @property
    def demand(self) -> float:
        return self._demand

    @demand.setter
    def demand(self, value: float):
        if value > 0:
            self._demand = value
        else:
            self._demand = 0

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self._name or id(self))


class StaticPool(Pool):
    """
    A static pool does not react on changing conditions regarding demand,
    allocation and utilisation but instead initialises the `capacity` of given
    drones with initialised `resources`.

    :param capacity: Maximum number of pools that can be instantiated within
                     the pool
    :param resources: Dictionary of resources available for each pool
                      instantiated within the pool
    """

    def __init__(self, make_drone: Callable, capacity: int = 0):
        assert capacity > 0, "Static pool was initialised without any resources..."
        super(StaticPool, self).__init__(
            capacity=capacity, init=capacity, make_drone=make_drone
        )
        self._demand = capacity

    async def run(self):
        """
        Pool runs forever and does not check if number of drones needs to be adapted.
        """
        while True:
            await eternity
