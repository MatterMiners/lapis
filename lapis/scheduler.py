from usim import Scope, each, instant

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
        self._stream_queue = job_queue
        self.drone_cluster = []
        self.interval = 60
        self.job_queue = []

    @property
    def drone_list(self):
        for cluster in self.drone_cluster:
            for drone in cluster:
                yield drone

    def register_drone(self, drone: Drone):
        self._add_drone(drone)

    def unregister_drone(self, drone: Drone):
        for cluster in self.drone_cluster:
            try:
                cluster.remove(drone)
            except ValueError:
                pass
            else:
                if len(cluster) == 0:
                    self.drone_cluster.remove(cluster)

    def _add_drone(self, drone: Drone):
        minimum_distance_cluster = None
        distance = float("Inf")
        if len(self.drone_cluster) > 0:
            for cluster in self.drone_cluster:
                current_distance = 0
                for key in {*cluster[0].theoretical_available_resources, *drone.theoretical_available_resources}:
                    current_distance += abs(cluster[0].theoretical_available_resources.get(key, 0) -
                                            drone.theoretical_available_resources.get(key, 0))
                if current_distance < distance:
                    minimum_distance_cluster = cluster
                    distance = current_distance
            if distance < 1:
                minimum_distance_cluster.append(drone)
            else:
                self.drone_cluster.append([drone])
        else:
            self.drone_cluster.append([drone])

    def update_drone(self, drone: Drone):
        self.unregister_drone(drone)
        self._add_drone(drone)

    async def run(self):
        async with Scope() as scope:
            scope.do(self._collect_jobs())
            async for _ in each(interval=self.interval):
                for job in self.job_queue:
                    best_match = self._schedule_job(job)
                    if best_match:
                        scope.do(best_match.start_job(job))
                        await instant
                        self.job_queue.remove(job)

    async def _collect_jobs(self):
        async for job in self._stream_queue:
            self.job_queue.append(job)

    def _schedule_job(self, job) -> Drone:
        priorities = {}
        for cluster in self.drone_cluster:
            drone = cluster[0]
            cost = 0
            resource_types = {*drone.resources.keys(), *job.resources.keys()}
            for resource_type in resource_types:
                if resource_type not in drone.resources.keys():
                    cost = float("Inf")
                    break
                elif resource_type not in job.resources:
                    cost += drone.pool_resources[resource_type] - drone.resources[resource_type]
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
