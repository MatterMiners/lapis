from cobald import interfaces

from job import Job


class Drone(interfaces.Pool):
    def __init__(self, env, pool, scheduling_duration):
        super(Drone, self).__init__()
        self.env = env
        self.pool = pool
        self.action = env.process(self.run(scheduling_duration))
        self.resources = {resource: 0 for resource in self.pool.resources}
        self._supply = 0
        self.jobs = 0
        self._allocation = None
        self._utilisation = None

    def run(self, scheduling_duration):
        yield self.env.timeout(scheduling_duration)
        self._supply = 1
        self.pool.drone_ready(self)

    @property
    def supply(self):
        return self._supply

    @property
    def demand(self):
        return 1

    @demand.setter
    def demand(self, value):
        pass  # demand is always defined as 1

    @property
    def utilisation(self):
        if self._utilisation is None:
            self._init_allocation_and_utilisation()
        return self._utilisation

    @property
    def allocation(self):
        if self._allocation is None:
            self._init_allocation_and_utilisation()
        return self._allocation

    def _init_allocation_and_utilisation(self):
        resources = []
        for resource_key, value in self.resources.items():
            resources.append(value / self.pool.resources[resource_key])
        self._allocation = max(resources)
        self._utilisation = min(resources)

    def shutdown(self):
        self._supply = 0
        yield self.env.timeout(1)
        # print("[drone %s] has been shut down" % self)

    def start_job(self, walltime, resources, used_resources=None):
        for resource_key in resources:
            if self.resources[resource_key] + resources[resource_key]:
                # TODO: kill job
                pass
        self._utilisation = None
        self._allocation = None
        for resource_key in resources:
            self.resources[resource_key] += resources[resource_key]
        self.jobs += 1
        yield from Job(self.env, walltime, resources)
        self.jobs -= 1
        self._utilisation = None
        self._allocation = None
        for resource_key in resources:
            self.resources[resource_key] -= resources[resource_key]
        # put drone back into pool queue
        # print("[drone %s] finished job at %d" % (self, self.env.now))
