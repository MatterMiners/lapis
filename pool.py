from simpy.resources import container
from cobald import interfaces

from drone import Drone


class Pool(interfaces.Pool, container.Container):
    def __init__(self, env, capacity=float('inf'), init=0, resources={"memory": 8, "cores": 1}):
        super(Pool, self).__init__(env, capacity, init)
        self.resources = resources
        self._demand = 1
        self._drones = []
        self.env = env
        self.action = env.process(self.run())

    def run(self):
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
