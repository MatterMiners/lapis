import globals


def job_scheduler(env):
    while True:
        for pool in globals.pools:
            while pool.level > 0 and globals.global_demand.level > 0:
                drone = yield from pool.get_drone(1)
                env.process(drone.start_job(*next(globals.job_generator)))
                yield env.timeout(0)
        yield env.timeout(1)
