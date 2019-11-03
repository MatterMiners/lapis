from cobald.controller.linear import LinearController
from cobald.controller.relative_supply import RelativeSupplyController
from cobald.interfaces import Pool
from usim import time


class SimulatedLinearController(LinearController):
    def __init__(
        self, target: Pool, low_utilisation=0.5, high_allocation=0.5, rate=1, interval=1
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
        low_utilisation=0.5,
        high_allocation=0.5,
        low_scale=0.9,
        high_scale=1.1,
        interval=1,
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
        self, target: Pool, low_utilisation=0.5, high_allocation=0.5, rate=1, interval=1
    ):
        self.current_cost = 1
        super(SimulatedCostController, self).__init__(
            target, low_utilisation, high_allocation, rate, interval
        )

    def regulate(self, interval):
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
                # self.target.demand = allocation + self.current_cost
        # else:
        #     if self.current_cost > 1:
        #         self.current_cost -= 1
        #     self.target.demand = allocation + self.current_cost
