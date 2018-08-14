import random
import math
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
            print("strategy: linear amount")
            amount = random.randint(0, int(random.random()*100))
        elif strategy < 2/3:
            # exponential amount
            print("strategy: exponential amount")
            amount = (math.e**(random.random())-1)*random.random()*1000
        else:
            # sqrt
            print("strategy: sqrt amount")
            amount = math.sqrt(random.random()*random.random()*100)
        value = yield env.timeout(delay=delay, value=amount)
        value = round(value)
        if value > 0:
            globals.global_demand.put(value)
            print("[demand] raising user demand for %f at %d to %d" % (value, env.now, globals.global_demand.level))


def job(env, walltime, memory, cores, disk):
    print("starting job at", env.now)
    globals.global_demand.get(1)
    yield env.timeout(walltime)
    print("job finished", env.now)


def job_property_generator():
    while True:
        yield 10, 8, 1, 100
