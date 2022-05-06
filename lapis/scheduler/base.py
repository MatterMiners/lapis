from abc import ABC
from typing import Dict, Iterator, Optional
from usim import Scope, interval, Resources, Queue

from lapis.workernode import WorkerNode
from lapis.job import Job
from lapis.monitor.core import sampling_required


class JobQueue(list):
    pass


class JobScheduler(ABC):
    __slots__ = ()

    @property
    def drone_list(self) -> Iterator[WorkerNode]:
        """Yields the registered drones"""
        raise NotImplementedError

    def register_drone(self, drone: WorkerNode):
        """Register a drone at the scheduler"""
        raise NotImplementedError

    def unregister_drone(self, drone: WorkerNode):
        """Unregister a drone at the scheduler"""
        raise NotImplementedError

    def update_drone(self, drone: WorkerNode):
        """Update parameters of a drone"""
        raise NotImplementedError

    async def run(self):
        """Run method of the scheduler"""
        raise NotImplementedError

    async def job_finished(self, job):
        """
        Declare a job as finished by a drone. This might even mean, that the job
        has failed and that the scheduler needs to requeue the job for further
        processing.
        """
        raise NotImplementedError


class CondorJobScheduler(JobScheduler):
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

    def __init__(self, job_queue: Queue):
        self._stream_queue = job_queue
        self.drone_cluster = []
        self.interval = 60
        self.job_queue = JobQueue()
        self._collecting = True
        self._processing = Resources(jobs=0)

    @property
    def drones(self) -> Iterator[WorkerNode]:
        for cluster in self.drone_cluster:
            for drone in cluster:
                yield drone

    def register_drone(self, drone: WorkerNode):
        self._add_drone(drone)

    def unregister_drone(self, drone: WorkerNode):
        for cluster in self.drone_cluster:
            try:
                cluster.remove(drone)
            except ValueError:
                pass
            else:
                if len(cluster) == 0:
                    self.drone_cluster.remove(cluster)

    def _add_drone(self, drone: WorkerNode, drone_resources: Dict = None):
        minimum_distance_cluster = None
        distance = float("Inf")
        if len(self.drone_cluster) > 0:
            for cluster in self.drone_cluster:
                current_distance = 0
                for key in {*cluster[0].pool_resources, *drone.pool_resources}:
                    if drone_resources:
                        current_distance += abs(
                            cluster[0].unallocated_resources.get(key, 0)
                            - drone_resources.get(key, 0)
                        )
                    else:
                        current_distance += abs(
                            cluster[0].unallocated_resources.get(key, 0)
                            - drone.unallocated_resources.get(key, 0)
                        )
                if current_distance < distance:
                    minimum_distance_cluster = cluster
                    distance = current_distance
            if distance < 1:
                minimum_distance_cluster.append(drone)
            else:
                self.drone_cluster.append([drone])
        else:
            self.drone_cluster.append([drone])

    def update_drone(self, drone: WorkerNode):
        self.unregister_drone(drone)
        self._add_drone(drone)

    async def run(self):
        async with Scope() as scope:
            scope.do(self._collect_jobs())
            async for _ in interval(self.interval):
                for job in self.job_queue.copy():
                    best_match = self._schedule_job(job)
                    if best_match:
                        await best_match.schedule_job(job)
                        self.job_queue.remove(job)
                        await sampling_required.put(self.job_queue)
                        self.unregister_drone(best_match)
                        left_resources = best_match.unallocated_resources
                        left_resources = {
                            key: value - job.resources.get(key, 0)
                            for key, value in left_resources.items()
                        }
                        self._add_drone(best_match, left_resources)
                if (
                    not self._collecting
                    and not self.job_queue
                    and self._processing.levels.jobs == 0
                ):
                    break
                await sampling_required.put(self)

    async def _collect_jobs(self):
        async for job in self._stream_queue:
            self.job_queue.append(job)
            await self._processing.increase(jobs=1)
            # TODO: logging happens with each job
            await sampling_required.put(self.job_queue)
        self._collecting = False

    async def job_finished(self, job: Job):
        if job.successful:
            await self._processing.decrease(jobs=1)
        else:
            await self._stream_queue.put(job)

    def _schedule_job(self, job: Job) -> Optional[WorkerNode]:
        priorities = {}
        for cluster in self.drone_cluster:
            drone = cluster[0]
            cost = 0
            resources = drone.unallocated_resources
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
