import simpy
import globals
from job import job_demand
from scheduler import drone_scheduler, job_scheduler
from pool import Pool


def main():
    env = simpy.Environment()
    for i in range(10):
        globals.pools.append(Pool(env))
    globals.global_demand = simpy.Container(env)
    env.process(job_demand(env))
    env.process(drone_scheduler(env))
    env.process(job_scheduler(env))
    env.run(until=100)


if __name__ == "__main__":
    main()
