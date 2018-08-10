import simpy
import globals
from job import job_demand, job_property_generator
from scheduler import drone_scheduler, job_scheduler
from pool import Pool


def main():
    env = simpy.Environment()
    for i in range(10):
        globals.pools.append(Pool(env))
    globals.global_demand = simpy.Container(env)
    globals.job_generator = job_property_generator()
    env.process(job_demand(env))
    env.process(drone_scheduler(env))
    env.process(job_scheduler(env))
    env.run(until=1000)


if __name__ == "__main__":
    main()
