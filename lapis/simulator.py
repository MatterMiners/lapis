import logging
import random
from functools import partial

from usim import run, time, until, Scope, Queue

from lapis.drone import Drone
from lapis.job import job_to_queue_scheduler
from lapis.monitor.general import (
    user_demand,
    job_statistics,
    resource_statistics,
    pool_status,
    configuration_information,
    job_events,
)
from lapis.monitor import Monitoring
from lapis.monitor.cobald import drone_statistics, pool_statistics


logging.getLogger("implementation").propagate = False


class Simulator(object):
    def __init__(self, seed=1234):
        random.seed(seed)
        self.job_queue = Queue()
        self.pools = []
        self.controllers = []
        self.job_scheduler = None
        self.job_generator = None
        self.cost = 0
        self._job_generators = []
        self.monitoring = None
        self.enable_monitoring()

    def enable_monitoring(self):
        self.monitoring = Monitoring()
        self.monitoring.register_statistic(user_demand)
        self.monitoring.register_statistic(job_statistics)
        self.monitoring.register_statistic(job_events)
        self.monitoring.register_statistic(pool_statistics)
        self.monitoring.register_statistic(drone_statistics)
        self.monitoring.register_statistic(resource_statistics)
        self.monitoring.register_statistic(pool_status)
        self.monitoring.register_statistic(configuration_information)

    def create_job_generator(self, job_input, job_reader):
        self._job_generators.append((job_input, job_reader))

    def create_pools(self, pool_input, pool_reader, pool_type, controller=None):
        assert self.job_scheduler, "Scheduler needs to be created before pools"
        for pool in pool_reader(
            iterable=pool_input,
            pool_type=pool_type,
            make_drone=partial(Drone, self.job_scheduler),
        ):
            self.pools.append(pool)
            if controller:
                self.controllers.append(controller(target=pool, rate=1))

    def create_scheduler(self, scheduler_type):
        self.job_scheduler = scheduler_type(job_queue=self.job_queue)

    def run(self, until=None):
        print(f"running until {until}")
        run(self._simulate(until))

    async def _simulate(self, end):
        print(f"Starting simulation at {time.now}")
        async with until(time == end) if end else Scope() as while_running:
            for pool in self.pools:
                while_running.do(pool.run(), volatile=True)
            for job_input, job_reader in self._job_generators:
                while_running.do(self._queue_jobs(job_input, job_reader))
            while_running.do(self.job_scheduler.run())
            for controller in self.controllers:
                while_running.do(controller.run(), volatile=True)
            while_running.do(self.monitoring.run(), volatile=True)
        print(f"Finished simulation at {time.now}")

    async def _queue_jobs(self, job_input, job_reader):
        await job_to_queue_scheduler(
            job_generator=job_reader(job_input), job_queue=self.job_queue
        )
