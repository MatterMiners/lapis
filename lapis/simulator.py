import random
from functools import partial

from usim import run, time, until, Scope
from usim.basics import Queue

from lapis.drone import Drone
from lapis.job import job_to_queue_scheduler
from lapis.utility.monitor import Monitoring, collect_pool_cobald_statistics, \
    collect_user_demand, collect_job_statistics, collect_drone_cobald_statistics


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
        self.monitoring = Monitoring(self)
        self.monitoring.register_statistic(collect_pool_cobald_statistics)
        self.monitoring.register_statistic(collect_user_demand)
        self.monitoring.register_statistic(collect_job_statistics)
        self.monitoring.register_statistic(collect_drone_cobald_statistics)

    def create_job_generator(self, job_input, job_reader):
        self._job_generators.append((job_input, job_reader))

    def create_pools(self, pool_input, pool_reader, pool_type, controller=None):
        assert self.job_scheduler, "Scheduler needs to be created before pools"
        for pool in pool_reader(iterable=pool_input, pool_type=pool_type,
                                make_drone=partial(Drone, self.job_scheduler)):
            self.pools.append(pool)
            if controller:
                self.controllers.append(controller(target=pool, rate=1))

    def create_scheduler(self, scheduler_type):
        self.job_scheduler = scheduler_type(job_queue=self.job_queue)

    def run(self, until=None):
        print("running until", until)
        run(self._simulate(until))

    async def _simulate(self, end):
        print("Starting simulation at %s" % time.now)
        async with until(time == end) if end else Scope() as while_running:
            for pool in self.pools:
                while_running.do(pool.run())
            for job_input, job_reader in self._job_generators:
                while_running.do(self._queue_jobs(job_input, job_reader))
            while_running.do(self.job_scheduler.run())
            for controller in self.controllers:
                while_running.do(controller.run())
            while_running.do(self.monitoring.run())
        print("Finished simulation at %s" % time.now)

    async def _queue_jobs(self, job_input, job_reader):
        await job_to_queue_scheduler(job_generator=job_reader(job_input),
                                     job_queue=self.job_queue)
