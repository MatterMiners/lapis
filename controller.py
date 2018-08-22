from cobald.controller.linear import LinearController
from cobald.interfaces import Pool


class SimulatedLinearController(LinearController):
    def __init__(self, env, target: Pool, low_utilisation=0.5, high_allocation=0.5, rate=1, interval=1):
        super(SimulatedLinearController, self).__init__(target, low_utilisation, high_allocation, rate, interval)
        self.env = env
        self.action = env.process(self.run())

    def run(self):
        while True:
            self.regulate(interval=self.interval)
            # print("[controller] demand %d -> %d, supply %d (global %d), allocation %.2f, utilisation %.2f "
            #       "(available %d)" % (pre_demand, self.target.demand, self.target.supply, globals.global_demand.level,
            #                           self.target.allocation, self.target.utilisation, self.target.level))
            yield self.env.timeout(self.interval)


class SimulatedCostController(SimulatedLinearController):
    def __init__(self, env, target: Pool, low_utilisation=0.5, high_allocation=0.5, rate=1, interval=1):
        self.current_cost = 1
        super(SimulatedCostController, self).__init__(env, target, low_utilisation, high_allocation, rate, interval)

    def regulate(self, interval):
        allocation = 0
        for drone in self.target.drones:
            allocation += drone.allocation
        if self.target.supply - allocation <= 1:
            if self.target.utilisation >= .8:
                self.target.demand = int(allocation + self.current_cost)
                self.current_cost += 1
            else:
                self.target.demand = allocation
                if self.current_cost > 1:
                    self.current_cost -= 1
        # else:
        #     self.target.demand = allocation
