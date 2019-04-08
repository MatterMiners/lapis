import logging

from cobald import interfaces
from usim import time, Scope, ActivityCancelled, instant, ActivityState

from lapis.job import Job


class ResourcesExceeded(Exception):
    ...


class Drone(interfaces.Pool):
    def __init__(self, scheduler, pool_resources: dict, scheduling_duration: float, exclusive: bool=False,
                 ignore_resources: list=None):
        """
        :param scheduler:
        :param pool_resources:
        :param scheduling_duration:
        :param exclusive: Determines if the drone is used exclusively by jobs in sequential order
        """
        super(Drone, self).__init__()
        self.scheduler = scheduler
        self.pool_resources = pool_resources
        self.resources = {resource: 0 for resource in self.pool_resources}
        self._valid_resource_keys = [resource for resource in self.pool_resources if resource not in ignore_resources]
        # shadowing requested resources to determine jobs to be killed
        self.used_resources = {resource: 0 for resource in self.pool_resources}
        self.scheduling_duration = scheduling_duration
        if scheduling_duration == 0:
            self._supply = 1
            self.scheduler.register_drone(self)
        else:
            self._supply = 0
        self.exclusive = exclusive
        self.jobs = 0
        self._allocation = None
        self._utilisation = None

    async def run(self):
        from lapis.utility.monitor import sampling_required
        await (time + self.scheduling_duration)
        self._supply = 1
        self.scheduler.register_drone(self)
        await sampling_required.set(True)

    @property
    def supply(self) -> float:
        return self._supply

    @property
    def demand(self) -> float:
        return 1

    @demand.setter
    def demand(self, value: float):
        pass  # demand is always defined as 1

    @property
    def utilisation(self) -> float:
        if self._utilisation is None:
            self._init_allocation_and_utilisation()
        return self._utilisation

    @property
    def allocation(self) -> float:
        if self._allocation is None:
            self._init_allocation_and_utilisation()
        return self._allocation

    def _init_allocation_and_utilisation(self):
        resources = []
        for resource_key in self._valid_resource_keys:
            resources.append(self.resources[resource_key] / self.pool_resources[resource_key])
        self._allocation = max(resources)
        self._utilisation = min(resources)

    async def shutdown(self):
        from lapis.utility.monitor import sampling_required
        self._supply = 0
        self.scheduler.unregister_drone(self)
        await sampling_required.set(True)
        await (time + 1)
        # print("[drone %s] has been shut down" % self)

    def _add_resources(self, keys: list, target: dict, source: dict, alternative_source: dict):
        resources_exceeded = False
        for resource_key in keys:
            try:
                value = target[resource_key] + source[resource_key]
            except KeyError:
                value = target[resource_key] + alternative_source[resource_key]
            if value > self.pool_resources[resource_key]:
                resources_exceeded = True
            target[resource_key] = value
        if resources_exceeded:
            raise ResourcesExceeded()

    @staticmethod
    def _remove_resources(keys: list, target: dict, source: dict, alternative_source: dict):
        for resource_key in keys:
            try:
                target[resource_key] -= source[resource_key]
            except KeyError:
                target[resource_key] -= alternative_source[resource_key]

    async def start_job(self, job: Job, kill: bool=False):
        """
        Method manages to start a job in the context of the given drone.
        The job is started independent of available resources. If resources of drone are exceeded, the job is killed.

        :param job: the job to start
        :param kill: if True, a job is killed when used resources exceed requested resources
        :return:
        """
        # TODO: ensure that jobs cannot immediately started on the same drone until the jobs did not allocate resources
        async with Scope() as scope:
            from lapis.utility.monitor import sampling_required
            self._utilisation = self._allocation = None

            job_execution = scope.do(job.run())
            job_keys = {*job.resources, *job.used_resources}

            try:
                self._add_resources(job_keys, self.used_resources, job.used_resources, job.resources)
            except ResourcesExceeded:
                job_execution.cancel()
            try:
                # TODO: should we really kill the job if it is only about resources and not used resources?
                self._add_resources(job_keys, self.resources, job.resources, job.used_resources)
            except ResourcesExceeded:
                job_execution.cancel()

            for resource_key in job_keys:
                try:
                    if job.resources[resource_key] < job.used_resources[resource_key]:
                        if kill:
                            job_execution.cancel()
                        else:
                            pass
                except KeyError:
                    # check is not relevant if the data is not stored
                    pass
            await instant  # waiting just a moment to enable job to set parameters
            if job_execution.status != ActivityState.CANCELLED:
                self.jobs += 1
                await sampling_required.set(True)
            await job_execution
            if job_execution.status == ActivityState.CANCELLED:
                for resource_key in job_keys:
                    usage = job.used_resources.get(resource_key, None) or job.resources.get(resource_key, None)
                    value = usage / (job.resources.get(resource_key, None) or self.pool_resources[resource_key])
                    if value > 1:
                        logging.info(str(round(time.now)), {
                            "job_exceeds_%s" % resource_key: {
                                repr(job): value
                            }
                        })
            else:
                self.jobs -= 1
            self._remove_resources(job_keys, self.resources, job.resources, job.used_resources)
            self._remove_resources(job_keys, self.used_resources, job.used_resources, job.resources)
            self._utilisation = self._allocation = None
            await sampling_required.set(True)

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, id(self))


