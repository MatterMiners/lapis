from cobald.controller.linear import LinearController
from cobald.interfaces import Pool

import globals

from cost import cobald_cost
from pool import pool_demands, pool_allocation, pool_utilisation, pool_unused


class SimulatedLinearController(LinearController):
    def __init__(self, env, target: Pool, low_utilisation=0.5, high_allocation=0.5, rate=1):
        super(SimulatedLinearController, self).__init__(target, low_utilisation, high_allocation, rate)
        self.env = env
        self.action = env.process(self.run())

    def run(self):
        while True:
            pre_demand = self.target.demand
            self.regulate_demand()
            # print("[controller] demand %d -> %d, supply %d (global %d), allocation %.2f, utilisation %.2f "
            #       "(available %d)" % (pre_demand, self.target.demand, self.target.supply, globals.global_demand.level,
            #                           self.target.allocation, self.target.utilisation, self.target.level))
            globals.monitoring_data[round(self.env.now)]["user_demand"] = globals.global_demand.level
            globals.monitoring_data[round(self.env.now)]["pool_demand"] = pool_demands()
            globals.monitoring_data[round(self.env.now)]["pool_utilisation"] = pool_utilisation()
            globals.monitoring_data[round(self.env.now)]["pool_allocation"] = pool_allocation()
            globals.monitoring_data[round(self.env.now)]["pool_unused"] = pool_unused() * -1
            current_cost = cobald_cost()
            globals.cost += current_cost
            globals.monitoring_data[round(self.env.now)]["cost"] = current_cost
            globals.monitoring_data[round(self.env.now)]["acc_cost"] =globals.cost
            yield self.env.timeout(self._interval)
