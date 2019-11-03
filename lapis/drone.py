from cobald import interfaces
from usim import time, Scope, instant, Capacities, ResourcesUnavailable

from lapis.job import Job


class ResourcesExceeded(Exception):
    ...


class Drone(interfaces.Pool):
    def __init__(
        self,
        scheduler,
        pool_resources: dict,
        scheduling_duration: float,
        ignore_resources: list = None,
    ):
        """
        :param scheduler:
        :param pool_resources:
        :param scheduling_duration:
        """
        super(Drone, self).__init__()
        self.scheduler = scheduler
        self.pool_resources = pool_resources
        self.resources = Capacities(**pool_resources)
        # shadowing requested resources to determine jobs to be killed
        self.used_resources = Capacities(**pool_resources)
        if ignore_resources:
            self._valid_resource_keys = [
                resource
                for resource in self.pool_resources
                if resource not in ignore_resources
            ]
        else:
            self._valid_resource_keys = self.pool_resources.keys()
        self.scheduling_duration = scheduling_duration
        if scheduling_duration == 0:
            self._supply = 1
            self.scheduler.register_drone(self)
        else:
            self._supply = 0
        self.jobs = 0
        self._allocation = None
        self._utilisation = None

    @property
    def theoretical_available_resources(self):
        return dict(self.resources.levels)

    @property
    def available_resources(self):
        return dict(self.used_resources.levels)

    async def run(self):
        from lapis.monitor import sampling_required

        await (time + self.scheduling_duration)
        self._supply = 1
        self.scheduler.register_drone(self)
        await sampling_required.put(self)

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
        levels = self.resources.levels
        resources = []
        for resource_key in self._valid_resource_keys:
            resources.append(
                getattr(levels, resource_key) / self.pool_resources[resource_key]
            )
        self._allocation = max(resources)
        self._utilisation = min(resources)

    async def shutdown(self):
        from lapis.monitor import sampling_required

        self._supply = 0
        self.scheduler.unregister_drone(self)
        await sampling_required.put(self)  # TODO: introduce state of drone
        await (time + 1)

    async def start_job(self, job: Job, kill: bool = False):
        """
        Method manages to start a job in the context of the given drone.
        The job is started independent of available resources. If resources of
        drone are exceeded, the job is killed.

        :param job: the job to start
        :param kill: if True, a job is killed when used resources exceed
                     requested resources
        :return:
        """
        job.drone = self
        async with Scope() as scope:
            from lapis.monitor import sampling_required

            self._utilisation = self._allocation = None

            job_execution = scope.do(job.run())
            self.jobs += 1
            try:
                async with self.resources.claim(
                    **job.resources
                ), self.used_resources.claim(**job.used_resources):
                    await sampling_required.put(self)
                    if kill:
                        for resource_key in job.resources:
                            try:
                                if (
                                    job.resources[resource_key]
                                    < job.used_resources[resource_key]
                                ):
                                    job_execution.cancel()
                            except KeyError:
                                # check is not relevant if the data is not stored
                                pass
                    self.scheduler.update_drone(self)
                    await job_execution.done
            except ResourcesUnavailable:
                await instant
                job_execution.cancel()
            except AssertionError:
                await instant
                job_execution.cancel()
            self.jobs -= 1
            self._utilisation = self._allocation = None
            self.scheduler.update_drone(self)
            await sampling_required.put(self)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, id(self))
