import random
from functools import partial

import simpy

from cobald_sim.job import job_to_queue_scheduler
from cobald_sim.utility.monitor import monitor, trace


class Simulator(object):
    def __init__(self, seed=1234):
        random.seed(seed)
        resource_normalisation = {"memory": 2000}
        monitor_data = partial(monitor, resource_normalisation)
        self.env = simpy.Environment()
        self.job_queue = []
        self.pools = []
        self.job_input = None
        self.job_scheduler = None
        self.cost = 0
        trace(self.env, monitor_data, resource_normalisation=resource_normalisation, simulator=self)

    def create_job_generator(self, filename, job_reader):
        self.job_input = open(filename, "r")
        job_generator = job_to_queue_scheduler(job_generator=job_reader(self.env, self.job_input),
                                               job_queue=self.job_queue,
                                               env=self.env)
        self.env.process(job_generator)

    def create_pools(self, filename, pool_reader, pool_type):
        with open(filename, "r") as pool_input:
            for pool in pool_reader(env=self.env, iterable=pool_input, pool_type=pool_type):
                self.pools.append(pool)

    def create_scheduler(self, scheduler_type):
        self.job_scheduler = scheduler_type(env=self.env, job_queue=self.job_queue, pools=self.pools)

    def run(self, until=2000):
        self.env.run(until=until)
