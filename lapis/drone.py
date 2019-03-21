from cobald import interfaces
from usim import time, Scope


class Drone(interfaces.Pool):
    def __init__(self, scheduler, pool_resources: dict, scheduling_duration: float):
        super(Drone, self).__init__()
        self.scheduler = scheduler
        self.pool_resources = pool_resources
        self.resources = {resource: 0 for resource in self.pool_resources}
        self.scheduling_duration = scheduling_duration
        # shadowing requested resources to determine jobs to be killed
        self.used_resources = {resource: 0 for resource in self.pool_resources}
        if scheduling_duration == 0:
            self._supply = 1
            self.scheduler.register_drone(self)
        else:
            self._supply = 0
        self.jobs = 0
        self._allocation = None
        self._utilisation = None

    async def run(self):
        await (time + self.scheduling_duration)
        self._supply = 1
        self.scheduler.register_drone(self)

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
        for resource_key, value in self.used_resources.items():
            resources.append(value / self.pool_resources[resource_key])
        self._allocation = max(resources)
        self._utilisation = min(resources)

    async def shutdown(self):
        self._supply = 0
        self.scheduler.unregister_drone(self)
        await (time + 1)
        # print("[drone %s] has been shut down" % self)

    async def start_job(self, job, kill=False):
        """
        Method manages to start a job in the context of the given drone.
        The job is started independent of available resources. If resources of drone are exceeded, the job is killed.

        :param job: the job to start
        :param kill: if True, a job is killed when used resources exceed requested resources
        :return:
        """
        async with Scope() as scope:
            self._utilisation = None
            self._allocation = None
            self.jobs += 1
            job_execution = scope.do(job.run())
            # TODO: needs to be killed if resources are exceeding
            for resource_key in job.resources:
                try:
                    if self.used_resources[resource_key] + job.used_resources[resource_key] > self.pool_resources[resource_key]:
                        job.kill()
                except KeyError:
                    # we do not have data about how many resources the job used, so check with requested data
                    if self.used_resources[resource_key] + job.resources[resource_key] > self.pool_resources[resource_key]:
                        job.kill()
                try:
                    if job.resources[resource_key] < job.used_resources[resource_key]:
                        if kill:
                            job.kill()
                        else:
                            pass
                except KeyError:
                    # check is not relevant if the data is not stored
                    pass
            for resource_key in job.resources:
                self.resources[resource_key] += job.resources[resource_key]
            for resource_key in {*job.resources, *job.used_resources}:
                try:
                    self.used_resources[resource_key] += job.used_resources[resource_key]
                except KeyError:
                    self.used_resources[resource_key] += job.resources[resource_key]
            await job_execution
            self.jobs -= 1
            self._utilisation = None
            self._allocation = None
            for resource_key in job.resources:
                self.resources[resource_key] -= job.resources[resource_key]
            for resource_key in {*job.resources, *job.used_resources}:
                try:
                    self.used_resources[resource_key] -= job.used_resources[resource_key]
                except KeyError:
                    self.used_resources[resource_key] -= job.resources[resource_key]
        # put drone back into pool queue
        # print("[drone %s] finished job at %d" % (self, self.env.now))
