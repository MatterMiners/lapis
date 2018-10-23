import random
import math
import simpy
import logging


def job_demand(env):
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
        value = yield env.timeout(delay=delay, value=amount)
        value = round(value)
        if value > 0:
            globals.global_demand.put(value)
            logging.getLogger("general").info(str(round(env.now)), {"user_demand_new": value})
            # print("[demand] raising user demand for %f at %d to %d" % (value, env.now, globals.global_demand.level))


class Job(object):
    def __init__(self, env, walltime, resources, used_resources=None, in_queue_since=0, schedule_date=0):
        self.env = env
        self.resources = resources
        self.used_resources = used_resources
        self.walltime = float(walltime)
        self.schedule_date = schedule_date
        self.in_queue_since = in_queue_since
        self.in_queue_until = None
        self.processing = None

    @property
    def waiting_time(self):
        if self.in_queue_until is not None:
            return self.in_queue_until - self.in_queue_since
        return float("Inf")

    def process(self):
        self.in_queue_until = self.env.now
        self.processing = self.env.process(self._process())
        return self.processing

    def _process(self):
        try:
            yield self.env.timeout(self.walltime, value=self)
        except simpy.exceptions.Interrupt:
            pass

    def kill(self):
        # job exceeds either own requested resources or resources provided by drone
        self.processing.interrupt(cause=self)


def job_property_generator(**kwargs):
    while True:
        yield 10, {"memory": 8, "cores": 1, "disk": 100}


def htcondor_export_job_generator(filename, job_queue, env=None, **kwargs):
    from .job_io.htcondor import htcondor_job_reader

    with open(filename, "r") as input_file:
        reader = htcondor_job_reader(env, input_file)
        job = next(reader)
        base_date = job.schedule_date
        current_time = 0

        count = 0
        while True:
            if not job:
                job = next(reader)
                current_time = job.schedule_date - base_date
            if env.now >= current_time:
                count += 1
                job.in_queue_since = env.now
                job_queue.append(job)
                job = None
            else:
                if count > 0:
                    logging.getLogger("general").info(str(round(env.now)), {"user_demand_new": count})
                    count = 0
                yield env.timeout(1)
