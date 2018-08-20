import random
import math
import csv

import globals


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
            globals.monitoring_data[round(env.now)]["user_demand_new"] = value
            # print("[demand] raising user demand for %f at %d to %d" % (value, env.now, globals.global_demand.level))


class Job(object):
    def __init__(self, env, walltime, resources, used_resources=None):
        self.env = env
        self.resources = resources
        self.walltime = walltime

    def __iter__(self):
        # print("starting job at", self.env.now)
        yield globals.global_demand.get(1)
        yield self.env.timeout(self.walltime)
        # print("job finished", self.env.now)


def job_property_generator():
    while True:
        yield 10, {"memory": 8, "cores": 1, "disk": 100}


def htcondor_export_job_generator(filename):
    with open(filename, "r") as input_file:
        htcondor_reader = csv.reader(input_file, delimiter=' ', quotechar="'")
        header = next(htcondor_reader)
        for row in htcondor_reader:
            yield 10, {
                "cores": int(row[header.index("RequestCpus")]),
                "disk": int(row[header.index("RequestDisk")]),
                "memory": float(row[header.index("RequestMemory")])
            }, {
                "memory": float(row[header.index("MemoryUsage")]),
                "disk": int(row[header.index("DiskUsage_RAW")])
            }
