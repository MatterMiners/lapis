from lapis.pool import Pool
from cobald.controller.linear import LinearController
from cobald.controller.relative_supply import RelativeSupplyController
from usim import time


class SimulatedLinearController(LinearController):
    def __init__(
        self,
        target: Pool,
        low_utilisation: float = 0.5,
        high_allocation: float = 0.5,
        rate: float = 1,
        interval: float = 1,
    ):
        super(SimulatedLinearController, self).__init__(
            target, low_utilisation, high_allocation, rate, interval
        )

    async def run(self):
        while True:
            self.regulate(interval=self.interval)
            await (time + self.interval)


class SimulatedRelativeSupplyController(RelativeSupplyController):
    def __init__(
        self,
        target: Pool,
        low_utilisation: float = 0.5,
        high_allocation: float = 0.5,
        low_scale: float = 0.9,
        high_scale: float = 1.1,
        interval: float = 1,
    ):
        super(SimulatedRelativeSupplyController, self).__init__(
            target=target,
            low_utilisation=low_utilisation,
            high_allocation=high_allocation,
            low_scale=low_scale,
            high_scale=high_scale,
            interval=interval,
        )

    async def run(self):
        while True:
            self.regulate(interval=self.interval)
            await (time + self.interval)


class SimulatedCostController(SimulatedLinearController):
    def __init__(
        self,
        target: Pool,
        low_utilisation: float = 0.5,
        high_allocation: float = 0.5,
        rate: float = 1,
        interval: float = 1,
    ):
        self.current_cost = 1
        super(SimulatedCostController, self).__init__(
            target, low_utilisation, high_allocation, rate, interval
        )

    def regulate(self, interval: float):
        allocation = 0
        for drone in self.target.drones:
            allocation += drone.allocation
        if self.target.supply - allocation <= 1:
            if self.target.utilisation >= 0.8:
                self.target.demand = int(allocation + self.current_cost)
                self.current_cost += 1
            else:
                self.target.demand = allocation
                if self.current_cost > 1:
                    self.current_cost -= 1
