from cobald import interfaces

from job import job


class Drone(interfaces.Pool):
    def __init__(self, env, pool, scheduling_duration):
        super(Drone, self).__init__()
        self.env = env
        self.pool = pool
        self.action = env.process(self.run(scheduling_duration))
        self._memory = 0
        self._disk = 0
        self._cores = 0
        self._supply = 0

    def run(self, scheduling_duration):
        yield self.env.timeout(scheduling_duration)
        self._supply = 1
        yield from self.pool.drone_ready(self)

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
        return ((self._memory / self.pool.memory) + (self._disk / self.pool.disk) + (self._cores / self.pool.cores)) / 3

    @property
    def allocation(self):
        return self._memory > 0 or self._disk > 0 or self._cores > 0

    def shutdown(self):
        self._supply = 0
        yield self.env.timeout(1)
        print("[drone %s] has been shut down" % self)

    def start_job(self, walltime, memory, cores, disk):
        print("[drone %s] starting job at %d" % (self, self.env.now))
        if (self._memory + memory > self.pool.memory or
                self._disk + disk > self.pool.disk or
                self._cores + cores > self.pool.cores):
            # TODO: kill job
            pass
        self._memory += memory
        self._disk += disk
        self._cores += cores
        yield self.env.process(job(self.env, walltime, memory, cores, disk))
        self._memory -= memory
        self._disk -= disk
        self._cores -= cores
        # put drone back into pool queue
        print("[drone %s] finished job at %d" % (self, self.env.now))
        self.pool.add_drone(self)
        yield from self.pool.drone_ready(self)
