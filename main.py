import simpy
import globals
from job import job_demand, job_property_generator
from scheduler import job_scheduler
from pool import Pool
from controller import SimulatedLinearController


def main():
    env = simpy.Environment()
    globals.job_generator = job_property_generator()
    for i in range(10):
        pool = Pool(env)
        globals.pools.append(pool)
        SimulatedLinearController(env, target=pool, rate=1)
    globals.global_demand = simpy.Container(env)
    env.process(job_demand(env))
    env.process(job_scheduler(env))
    env.run(until=1000)


if __name__ == "__main__":
    main()
