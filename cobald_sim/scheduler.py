from cobald_sim import globals


# TODO: does not work anymore as there is no method get_drone at pool
def job_scheduler(env):
    while True:
        for pool in globals.pools:
            while pool.level > 0 and globals.global_demand.level > 0:
                drone = yield from pool.get_drone(1)
                env.process(drone.start_job(*next(globals.job_generator)))
                yield env.timeout(0)
        yield env.timeout(1)


class CondorJobScheduler(object):
    """
    Goal of the htcondor job scheduler is to have a scheduler that somehow mimics how htcondor does schedule jobs.
    Htcondor does scheduling based on a priority queue. The priorities itself are managed by operators of htcondor.
    So different instances can apparently behave very different.

    In my case I am going to try building a priority queue that sorts job slots by increasing cost. The cost itself
    is calculated based on the current strategy that is used at GridKa. The scheduler checks if a job either
    exactly fits a slot or if it does fit into it several times. The cost for putting a job at a given slot is
    given by the amount of resources that might remain unallocated.
    :param env:
    :return:
    """
    def __init__(self, env, job_queue):
        self.env = env
        self.job_queue = job_queue
        self.action = env.process(self.run())

    def run(self):
        # current_job = None
        # postponed_unmatched_job = False
        while True:
            for job in self.job_queue:
                best_match = self._schedule_job(job)
                if best_match:
                    self.env.process(best_match.start_job(job))
                    self.job_queue.remove(job)
                    yield self.env.timeout(0)
            yield self.env.timeout(60)

    def _schedule_job(self, job):
        priorities = {}
        for pool in globals.pools:
            for drone in pool.drones:
                cost = 0
                resource_types = {*drone.resources.keys(), *job.resources.keys()}
                for resource_type in resource_types:
                    if resource_type not in drone.resources.keys():
                        cost = float("Inf")
                    elif resource_type not in job.resources:
                        cost += drone.resources[resource_type] - drone.resources[resource_type]
                    elif (pool.resources[resource_type] - drone.resources[resource_type]) < \
                            job.resources[resource_type]:
                        cost = float("Inf")
                        break
                    else:
                        cost += (pool.resources[resource_type] - drone.resources[resource_type]) // \
                                job.resources[resource_type]
                cost /= len(resource_types)
                if cost <= 1:
                    # directly start job
                    return drone
                try:
                    priorities[cost].append(drone)
                except KeyError:
                    priorities[cost] = [drone]
        try:
            minimal_key = min(priorities)
            if minimal_key < float("Inf"):
                return priorities[minimal_key][0]
        except ValueError:
            pass
        return None
