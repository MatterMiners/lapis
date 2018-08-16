from simpy.resources import container
from cobald import interfaces

import globals
from drone import Drone


def pool_demands():
    result = 0
    for pool in globals.pools:
        result += pool.demand
    return result


def pool_utilisation():
    result = []
    for pool in globals.pools:
        for drone in pool.drones():
            result.append(drone.utilisation)
    return sum(result)


def pool_allocation():
    result = []
    for pool in globals.pools:
        for drone in pool.drones():
            result.append(drone.allocation)
    return sum(result)


def pool_unused():
    result = 0
    for pool in globals.pools:
        for drone in pool.drones():
            if drone.allocation == 0:
                result += 1
    return result


class Pool(interfaces.Pool, container.Container):
    def __init__(self, env, capacity=float('inf'), init=0, resources={"memory": 8, "cores": 1, "disk": 100}):
        super(Pool, self).__init__(env, capacity, init)
        self.resources = resources
        self._demand = 0
        self._drones = []
        self._drones_in_use = []
        self.env = env
        self.action = env.process(self.run())

    def run(self):
        while True:
            if self.drone_demand() < self._demand:
                # start a new drone
                self.add_drone(Drone(self.env, self, 10))
            elif self.drone_demand() > self._demand:
                yield self.get(1)
                drone = self._drones.pop(0)
                yield from drone.shutdown()
                del drone
            yield self.env.timeout(1)

    def drone_demand(self):
        return len(self._drones) + len(self._drones_in_use)

    @property
    def allocation(self) -> float:
        allocations = []
        for drone in self.drones():
            allocations.append(drone.allocation)
        try:
            return sum(allocations) / len(allocations)
        except ZeroDivisionError:
            return 1

    @property
    def utilisation(self) -> float:
        utilisations = []
        for drone in self.drones():
            utilisations.append(drone.utilisation)
        try:
            return sum(utilisations) / len(utilisations)
        except ZeroDivisionError:
            return 1

    @property
    def supply(self):
        supply = 0
        for drone in self.drones():
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

    def add_drone(self, drone):
        try:
            self._drones_in_use.remove(drone)
        except ValueError:
            pass

    def drone_ready(self, drone):
        # print("[drone %s] is ready at %d" % (drone, self.env.now))
        self._drones.append(drone)
        yield self.put(1)

    def get_drone(self, amount):
        yield self.get(amount)
        drone = self._drones.pop(0)
        self._drones_in_use.append(drone)
        return drone

    def drones(self):
        for drone in self._drones:
            yield drone
        for drone in self._drones_in_use:
            yield drone
