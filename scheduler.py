import globals
from pool import pool_demand


def drone_scheduler(env):
    while True:
        for pool in globals.pools:
            demand = pool_demand()
            if demand < globals.global_demand.level:
                # ask for another drone in the pool
                pool.demand += 1
            elif demand > globals.global_demand.level and pool.demand > 0:
                # lower demand and ask for stopping drones
                pool.demand -= 1
            print("[demand] pool %f vs user %f" % (demand, globals.global_demand.level))
            yield env.timeout(1)


def job_scheduler(env):
    while True:
        for pool in globals.pools:
            if pool.level > 0 and globals.global_demand.level > 0:
                drone = pool.get_drone(1)
                drone.start_job(walltime=10, memory=2, cores=1, disk=100)
            yield env.timeout(1)

