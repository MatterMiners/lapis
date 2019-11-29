from typing import Dict

from classad._functions import quantize
from classad._primitives import HTCInt
from usim import Scope, interval, Resources

from lapis.drone import Drone
from lapis.monitor import sampling_required


class JobQueue(list):
    pass


quantization_defaults = {
    "memory": HTCInt(128 * 1024 * 1024),
    "disk": HTCInt(1024 * 1024),
    "cores": HTCInt(1),
}


class CondorJobScheduler(object):
    """
    Goal of the htcondor job scheduler is to have a scheduler that somehow
    mimics how htcondor does schedule jobs.
    Htcondor does scheduling based on a priority queue. The priorities itself
    are managed by operators of htcondor.
    So different instances can apparently behave very different.

    In my case I am going to try building a priority queue that sorts job slots
    by increasing cost. The cost itself is calculated based on the current
    strategy that is used at GridKa. The scheduler checks if a job either
    exactly fits a slot or if it does fit into it several times. The cost for
    putting a job at a given slot is given by the amount of resources that
    might remain unallocated.
    :return:
    """

    def __init__(self, job_queue):
        self._stream_queue = job_queue
        self.drone_cluster = {}
        self.interval = 60
        self.job_queue = JobQueue()
        self._collecting = True
        self._processing = Resources(jobs=0)

    @property
    def drone_list(self):
        for cluster in self.drone_cluster.values():
            for drone in cluster:
                yield drone

    def register_drone(self, drone: Drone):
        self._add_drone(drone)

    def unregister_drone(self, drone: Drone):
        for key in self.drone_cluster:
            try:
                self.drone_cluster[key].remove(drone)
            except ValueError:
                pass
            else:
                break
        else:
            # nothing was removed
            return
        if len(self.drone_cluster[key]) == 0:
            del self.drone_cluster[key]

    def _clustering_key(self, resource_dict: Dict):
        clustering_key = []
        for key, value in resource_dict.items():
            clustering_key.append(
                int(quantize(value, quantization_defaults.get(key, 1)))
            )
        return tuple(clustering_key)

    def _add_drone(self, drone: Drone, drone_resources: Dict = None):
        if drone_resources:
            clustering_key = self._clustering_key(drone_resources)
        else:
            clustering_key = self._clustering_key(drone.theoretical_available_resources)
        self.drone_cluster.setdefault(clustering_key, []).append(drone)

    def update_drone(self, drone: Drone):
        self.unregister_drone(drone)
        self._add_drone(drone)

    async def run(self):
        async with Scope() as scope:
            scope.do(self._collect_jobs())
            async for _ in interval(self.interval):
                job_drone_mapping = {}
                for job in self.job_queue:
                    job_key = self._clustering_key(job.resources)
                    try:
                        drone_key = job_drone_mapping[job_key]
                        if drone_key is None:
                            continue
                        best_match = self._schedule_job(
                            job, self.drone_cluster[drone_key]
                        )
                    except KeyError:
                        best_match = self._schedule_job(job)
                    if best_match:
                        job_drone_mapping[job_key] = self._clustering_key(
                            best_match.theoretical_available_resources
                        )
                        await self._execute_job(job, best_match)
                    else:
                        job_drone_mapping[job_key] = None
                if (
                    not self._collecting
                    and not self.job_queue
                    and self._processing.levels.jobs == 0
                ):
                    break
                await sampling_required.put(self)

    async def _execute_job(self, job, drone):
        await drone.schedule_job(job)
        self.job_queue.remove(job)
        await sampling_required.put(self.job_queue)
        self.unregister_drone(drone)
        left_resources = {
            key: value - job.resources.get(key, 0)
            for key, value in drone.theoretical_available_resources.items()
        }
        self._add_drone(drone, left_resources)

    async def _collect_jobs(self):
        async for job in self._stream_queue:
            self.job_queue.append(job)
            await self._processing.increase(jobs=1)
            # TODO: logging happens with each job
            await sampling_required.put(self.job_queue)
        self._collecting = False

    async def job_finished(self, job):
        if job.successful:
            await self._processing.decrease(jobs=1)
        else:
            self.job_queue.append(job)

    def _schedule_job(self, job, cluster=None) -> Drone:
        priorities = {}
        if cluster and len(cluster) > 0:
            return cluster[0]
        for cluster in self.drone_cluster.values():
            drone = cluster[0]
            cost = 0
            resources = drone.theoretical_available_resources
            for resource_type in job.resources:
                if resources.get(resource_type, 0) < job.resources[resource_type]:
                    # Inf for all job resources that a drone does not support
                    # and all resources that are too small to even be considered
                    cost = float("Inf")
                    break
                else:
                    try:
                        cost += 1 / (
                            resources[resource_type] // job.resources[resource_type]
                        )
                    except KeyError:
                        pass
            for additional_resource_type in [
                key for key in drone.pool_resources if key not in job.resources
            ]:
                cost += resources[additional_resource_type]
            cost /= len((*job.resources, *drone.pool_resources))
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
