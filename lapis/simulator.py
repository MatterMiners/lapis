import logging
import random
import time as pytime
from functools import partial

from typing import List

from cobald.interfaces import Controller
from usim import run, time, until, Scope, Queue

import lapis.monitor as monitor
from lapis.drone import Drone
from lapis.job import job_to_queue_scheduler
from lapis.connection import Connection
from lapis.monitor.caching import storage_status, pipe_status, hitrate_evaluation
from lapis.monitor.general import (
    user_demand,
    job_statistics,
    resource_statistics,
    pool_status,
    configuration_information,
    job_events,
    drone_statistics_caching,
)
from lapis.monitor.cobald import drone_statistics, pool_statistics
from lapis.pool import Pool

logging.getLogger("implementation").propagate = False


class Simulator(object):
    def __init__(self, seed=1234):
        random.seed(seed)
        self.job_queue: Queue = Queue()
        self.pools: List[Pool] = []
        self.connection: Connection = None
        self.controllers: List[Controller] = []
        self.job_scheduler = None
        self.job_generator = None
        self._job_generators = []
        self.monitoring = monitor.Monitoring()
        self.duration = None

    def enable_monitoring(self):
        self.monitoring.register_statistic(user_demand)
        self.monitoring.register_statistic(job_statistics)
        self.monitoring.register_statistic(job_events)
        self.monitoring.register_statistic(pool_statistics)
        self.monitoring.register_statistic(drone_statistics)
        self.monitoring.register_statistic(resource_statistics)
        self.monitoring.register_statistic(pool_status)
        self.monitoring.register_statistic(configuration_information)
        self.monitoring.register_statistic(storage_status)
        self.monitoring.register_statistic(pipe_status)
        self.monitoring.register_statistic(drone_statistics_caching)
        self.monitoring.register_statistic(hitrate_evaluation)

    def create_job_generator(self, job_input, job_reader):
        self._job_generators.append((job_input, job_reader))

    def create_pools(self, pool_input, pool_reader, pool_type, controller=None):
        assert self.job_scheduler, "Scheduler needs to be created before pools"
        for pool in pool_reader(
            iterable=pool_input,
            pool_type=pool_type,
            make_drone=partial(Drone, self.job_scheduler),
            connection=self.connection,
        ):
            self.pools.append(pool)
            if controller:
                self.controllers.append(controller(target=pool, rate=1))

    def create_storage(
        self, storage_input, storage_reader, storage_type, storage_content_input=None
    ):
        assert self.connection, "Connection module needs to be created before storages"
        for storage in storage_reader(
            storage=storage_input,
            storage_content=storage_content_input,
            storage_type=storage_type,
        ):
            self.connection.add_storage_element(storage)

    def create_scheduler(self, scheduler_type):
        self.job_scheduler = scheduler_type(job_queue=self.job_queue)

    def create_connection_module(self, remote_throughput):
        self.connection = Connection(remote_throughput)

    def run(self, until=None):
        monitor.SIMULATION_START = pytime.time()
        print(f"[lapis-{monitor.SIMULATION_START}] running until {until}")
        run(self._simulate(until))

    async def _simulate(self, end):
        print(f"[lapis-{monitor.SIMULATION_START}] Starting simulation at {time.now}")
        async with until(time == end) if end else Scope() as while_running:
            for pool in self.pools:
                while_running.do(pool.run(), volatile=True)
            for job_input, job_reader in self._job_generators:
                while_running.do(self._queue_jobs(job_input, job_reader))
            while_running.do(self.job_scheduler.run())
            for controller in self.controllers:
                while_running.do(controller.run(), volatile=True)
            while_running.do(self.monitoring.run(), volatile=True)
            while_running.do(self.connection.run_pipemonitoring(), volatile=True)
        self.duration = time.now
        print(
            f"[lapis-{monitor.SIMULATION_START}] Finished simulation at {self.duration}"
        )

    async def _queue_jobs(self, job_input, job_reader):
        await job_to_queue_scheduler(
            job_generator=partial(job_reader, job_input)(), job_queue=self.job_queue
        )
