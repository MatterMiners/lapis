import random
import math
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


class Job(object):
    __slots__ = ("resources", "used_resources", "walltime", "requested_walltime", "queue_date", "in_queue_since",
                 "in_queue_until", "name")

    def __init__(self, resources: dict, used_resources: dict, in_queue_since: float=0, queue_date: float=0,
                 name: str=None):
        """
        Definition of a job that uses a specified amount of resources `used_resources` over a given amount of time,
        `walltime`. A job is described by its user via the parameter `resources`. This is a user prediction and is
        expected to deviate from `used_resources`.

        :param resources: Requested resources of the job
        :param used_resources: Resource usage of the job
        :param in_queue_since: Time when job was inserted into the queue of the simulation scheduler
        :param queue_date: Time when job was inserted into queue in real life
        :param name: Name of the job
        """
        self.resources = resources
        self.used_resources = used_resources
        self.walltime = used_resources.pop("walltime", None)
        self.requested_walltime = resources.pop("walltime", None)
        assert self.walltime or self.requested_walltime, "Job does not provide any walltime"
        self.queue_date = queue_date
        self.in_queue_since = in_queue_since
        self.in_queue_until = None
        self.name = name or id(self)

    @property
    def waiting_time(self) -> float:
        """
        The time the job spent in the simulators scheduling queue. `Inf` when the job is still waitiing.

        :return: Time in queue
        """
        if self.in_queue_until is not None:
            return self.in_queue_until - self.in_queue_since
        return float("Inf")

    async def run(self):
        self.in_queue_until = time.now
        logging.info(str(round(time.now)), {
            "job_queue_time": self.queue_date,
            "job_waiting_time": self.waiting_time
        })
        await (time + self.walltime or self.requested_walltime)
        logging.info(str(round(time.now)), {
            "job_wall_time": self.walltime or self.requested_walltime
        })


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
