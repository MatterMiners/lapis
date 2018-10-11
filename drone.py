from cobald import interfaces


class Drone(interfaces.Pool):
    def __init__(self, env, pool_resources, scheduling_duration):
        super(Drone, self).__init__()
        self.env = env
        self.pool_resources = pool_resources
        self.action = env.process(self.run(scheduling_duration))
        self.resources = {resource: 0 for resource in self.pool_resources}
        # shadowing requested resources to determine jobs to be killed
        self.used_resources = {resource: 0 for resource in self.pool_resources}
        self._supply = 0
        self.jobs = 0
        self._allocation = None
        self._utilisation = None

    def run(self, scheduling_duration):
        yield self.env.timeout(scheduling_duration)
        self._supply = 1

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
            resources.append(value / self.pool_resources[resource_key])
        self._allocation = max(resources)
        self._utilisation = min(resources)

    def shutdown(self):
        self._supply = 0
        yield self.env.timeout(1)
        # print("[drone %s] has been shut down" % self)

    def start_job(self, job, kill=False):
        """
        Method manages to start a job in the context of the given drone.
        The job is started independent of available resources. If resources of drone are exceeded, the job is killed.

        :param job: the job to start
        :param kill: if True, a job is killed when used resources exceed requested resources
        :return:
        """
        self._utilisation = None
        self._allocation = None
        self.jobs += 1
        job_execution = job.process()
        for resource_key in job.resources:
            if self.used_resources[resource_key] + job.used_resources[resource_key] > self.pool_resources[resource_key]:
                job.kill()
            if job.resources[resource_key] < job.used_resources[resource_key]:
                if kill:
                    job.kill()
                else:
                    pass
        for resource_key in job.resources:
            self.resources[resource_key] += job.resources[resource_key]
            self.used_resources[resource_key] += job.used_resources[resource_key]
        yield job_execution
        self.jobs -= 1
        self._utilisation = None
        self._allocation = None
        for resource_key in job.resources:
            self.resources[resource_key] -= job.resources[resource_key]
            self.used_resources[resource_key] -= job.used_resources[resource_key]
        # put drone back into pool queue
        # print("[drone %s] finished job at %d" % (self, self.env.now))
