from simpy.resources import container
from cobald import interfaces
import globals
from drone import Drone


def pool_supply():
    result = 0
    for pool in globals.pools:
        result += pool.supply
    return result


def pool_demand():
    result = 0
    for pool in globals.pools:
        result += pool.demand
    return result


class Pool(interfaces.Pool, container.Container):
    def __init__(self, env, capacity=float('inf'), init=0, memory=8, cores=1, disk=100):
        super(Pool, self).__init__(env, capacity, init)
        self.memory = memory
        self.cores = cores
        self.disk = disk
        self._demand = 0
        self._supply = 0
        self._drones = []
        self._drones_in_use = []
        self.env = env
        self.action = env.process(self.run())

    def run(self):
        while True:
            if self._supply < self._demand:
                # start a new drone
                self._supply += 1
                Drone(self.env, self, 10)
            elif self._supply > self._demand:
                self.get(1)
                drone = self._drones.pop(0)
                self._supply -= 1
                yield from drone.shutdown()
                del drone
            yield self.env.timeout(1)

    @property
    def allocation(self) -> float:
        return len(self._drones_in_use) / self._supply

    @property
    def utilisation(self) -> float:
        return 0

    @property
    def supply(self):
        return self._supply

    @property
    def demand(self):
        return self._demand

    @demand.setter
    def demand(self, value):
        self._demand = value

    def add_drone(self, drone):
        try:
            self._drones_in_use.remove(drone)
        except ValueError:
            # drone not already existent
            pass
        self._drones.append(drone)
        self.put(1)
        print("[supply] pool supply at %d / %d (available: %d, allocation: %.2f, utilisation: %.2f)"
              % (self._supply, self._demand, self.level, self.allocation, self.utilisation))

    def get_drone(self, amount):
        super(Pool, self).get(amount)
        drone = self._drones.pop(0)
        self._drones_in_use.append(drone)
        return drone
