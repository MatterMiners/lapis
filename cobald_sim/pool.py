from simpy.resources import container
from cobald import interfaces

from .drone import Drone


class Pool(interfaces.Pool, container.Container):
    """
    A pool encapsulating a number of pools or drones. Given a specific demand, allocation and utilisation, the
    pool is able to adapt in terms of number of drones providing the given resources.

    :param env: Reference to the simulation env
    :param capacity: Maximum number of pools that can be instantiated within the pool
    :param init: Number of pools to instantiate at creation time of the pool
    :param resources: Dictionary of resources available for each pool instantiated within the pool
    """
    def __init__(self, env, capacity=float('inf'), init=0, resources={"memory": 8000, "cores": 1}, name=None):
        super(Pool, self).__init__(env, capacity, init)
        self._drones = []
        self.env = env
        self.resources = resources
        self.init_pool(init=init)
        self._demand = 1
        self.name = name or id(self)
        self.action = env.process(self.run())

    def init_pool(self, init=0):
        """
        Initialisation of existing drones at creation time of pool.

        :param init: Number of drones to create.
        """
        for _ in range(init):
            self._drones.append(Drone(self.env, self.resources, 0))

    def run(self):
        """
        Pool periodically checks the current demand and provided drones. If demand is higher than the current level,
        the pool takes care of initialising new drones. Otherwise drones get removed.
        """
        while True:
            drones_required = self._demand - self.level
            while drones_required > 0:
                drones_required -= 1
                # start a new drone
                self._drones.append(Drone(self.env, self.resources, 10))
                yield self.put(1)
            if self.level > self._demand:
                for drone in self._drones:
                    if drone.jobs == 0:
                        break
                else:
                    break
                yield self.get(1)
                self._drones.remove(drone)
                yield from drone.shutdown()
                del drone
            yield self.env.timeout(1)

    @property
    def drones(self):
        for drone in self._drones:
            if drone.supply > 0:
                yield drone

    def drone_demand(self):
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
            if drone.allocation > 0:
                utilisations.append(drone.utilisation)
        try:
            return sum(utilisations) / len(utilisations)
        except ZeroDivisionError:
            return 1

    @property
    def supply(self):
        supply = 0
        for drone in self._drones:
            supply += drone.supply
        return supply

    @property
    def demand(self):
        return self._demand

    @demand.setter
    def demand(self, value):
        if value > 0:
            self._demand = value
        else:
            self._demand = 0


class StaticPool(Pool):
    """
    A static pool does not react on changing conditions regarding demand, allocation and utilisation but instead
    initialises the `capacity` of given drones with initialised `resources`.

    :param env: Reference to the simulation env
    :param capacity: Maximum number of pools that can be instantiated within the pool
    :param resources: Dictionary of resources available for each pool instantiated within the pool
    """
    def __init__(self, env, capacity=0, resources={"memory": 8000, "cores": 1}):
        assert capacity > 0, "Static pool was initialised without any resources..."
        super(StaticPool, self).__init__(env, capacity=capacity, init=capacity, resources=resources)
        self._demand = capacity

    def run(self):
        """
        Pool runs forever and does not check if number of drones needs to be adapted.
        """
        while True:
            yield self.env.timeout(float("Inf"))
