from job import job


class Drone(object):
    def __init__(self, env, pool, scheduling_duration):
        self.env = env
        self.pool = pool
        self.action = env.process(self.run(scheduling_duration))
        self._memory = 0
        self._disk = 0
        self._cores = 0

    def run(self, scheduling_duration):
        yield self.env.timeout(scheduling_duration)
        print("drone is alive at", self.env.now)
        self.pool.add_drone(self)

    def shutdown(self):
        yield self.env.timeout(1)
        print("drone has been shut down")

    def start_job(self, walltime, memory, cores, disk):
        print("starting job at", self.env.now)
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
        self.pool.add_drone(self)
