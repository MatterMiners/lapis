from usim import time, Scope


# TODO: does not work anymore as there is no method get_drone at pool
from lapis.drone import Drone


def job_scheduler(simulator):
    while True:
        for pool in simulator.pools:
            while pool.level > 0 and simulator.global_demand.level > 0:
                drone = yield from pool.get_drone(1)
                simulator.env.process(drone.start_job(*next(simulator.job_generator)))
                yield simulator.env.timeout(0)
        yield simulator.env.timeout(1)


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
    def __init__(self, job_queue):
        self.job_queue = job_queue
        self.drone_list = []

    def register_drone(self, drone: Drone):
        self.drone_list.append(drone)

    def unregister_drone(self, drone: Drone):
        self.drone_list.remove(drone)

    async def run(self):
        # current_job = None
        # postponed_unmatched_job = False
        async with Scope() as scope:
            temp = []
            while True:
                async for job in self.job_queue:
                    best_match = self._schedule_job(job)
                    if best_match:
                        scope.do(best_match.start_job(job))
                    else:
                        temp.append(job)
                # put all the jobs that could not be scheduled back into the queue
                while temp:
                    job = temp.pop()
                    await self.job_queue.put(job)
                await (time + 60)

    def _schedule_job(self, job) -> Drone:
        priorities = {}
        for drone in self.drone_list:
            cost = 0
            resource_types = {*drone.resources.keys(), *job.resources.keys()}
            for resource_type in resource_types:
                if resource_type not in drone.resources.keys():
                    cost = float("Inf")
                elif resource_type not in job.resources:
                    cost += drone.resources[resource_type] - drone.resources[resource_type]
                elif (drone.pool_resources[resource_type] - drone.resources[resource_type]) < \
                        job.resources[resource_type]:
                    cost = float("Inf")
                    break
                else:
                    cost += (drone.pool_resources[resource_type] - drone.resources[resource_type]) // \
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
