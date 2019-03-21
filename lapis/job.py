import random
import math
import simpy
import logging

from usim import time


# TODO: needs refactoring
def job_demand(simulator):
    """
    function randomly sets global user demand by using different strategies
    :param env:
    :return:
    """
    while True:
        delay = random.randint(0, 100)
        strategy = random.random()
        if strategy < 1/3:
            # linear amount
            # print("strategy: linear amount")
            amount = random.randint(0, int(random.random()*100))
        elif strategy < 2/3:
            # exponential amount
            # print("strategy: exponential amount")
            amount = (math.e**(random.random())-1)*random.random()*1000
        else:
            # sqrt
            # print("strategy: sqrt amount")
            amount = math.sqrt(random.random()*random.random()*100)
        value = yield simulator.env.timeout(delay=delay, value=amount)
        value = round(value)
        if value > 0:
            simulator.global_demand.put(value)
            logging.info(str(round(simulator.env.now)), {"user_demand_new": value})
            # print("[demand] raising user demand for %f at %d to %d" % (value, env.now, globals.global_demand.level))


# TODO: needs refactoring
class Job(object):
    def __init__(self, resources, used_resources=None, in_queue_since=0, queue_date=0, name=None):
        self.resources = resources
        self.used_resources = used_resources
        self.walltime = used_resources.pop("walltime", None)
        self.requested_walltime = resources.pop("walltime", None)
        assert self.walltime or self.requested_walltime, "Job does not provide any walltime"
        self.queue_date = queue_date
        self.in_queue_since = in_queue_since
        self.in_queue_until = None
        self.processing = None
        self.name = name or id(self)

    @property
    def waiting_time(self):
        if self.in_queue_until is not None:
            return self.in_queue_until - self.in_queue_since
        return float("Inf")

    async def run(self):
        self.in_queue_until = time.now
        # self.processing = self.env.process(self._process())
        # return self.processing
        await (time + self.walltime or self.requested_walltime)
        print("%s: job finished after %s" % (time.now, self.walltime or self.requested_walltime))

    # TODO: not needed anymore?
    def _process(self):
        try:
            yield self.env.timeout(0, value=self)
            yield self.env.timeout(self.requested_walltime or self.walltime)
        except simpy.exceptions.Interrupt:
            pass

    # TODO: interrupt should be integrated
    def kill(self):
        # job exceeds either own requested resources or resources provided by drone
        self.processing.interrupt(cause=self)


def job_property_generator(**kwargs):
    while True:
        yield 10, {"memory": 8, "cores": 1, "disk": 100}


async def job_to_queue_scheduler(job_generator, job_queue, **kwargs):
    job = next(job_generator)
    base_date = job.queue_date
    current_time = 0

    count = 0
    while True:
        if not job:
            job = next(job_generator)
            current_time = job.queue_date - base_date
        if time.now >= current_time:
            count += 1
            job.in_queue_since = time.now
            await job_queue.put(job)
            job = None
        else:
            if count > 0:
                logging.info(str(round(time.now)), {"user_demand_new": count})
                count = 0
            await (time == current_time)
