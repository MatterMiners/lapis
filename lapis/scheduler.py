from abc import ABC
from typing import Dict, Iterator, Union, Tuple, List, TypeVar, Generic, Optional
from weakref import WeakKeyDictionary

from classad import parse
from classad._functions import quantize
from classad._primitives import HTCInt, Undefined
from classad._expression import ClassAd
from usim import Scope, interval, Resources

from lapis.drone import Drone
from lapis.job import Job
from lapis.monitor import sampling_required


class JobQueue(list):
    pass


quantization_defaults = {
    "memory": HTCInt(128 * 1024 * 1024),
    "disk": HTCInt(1024 * 1024),
    "cores": HTCInt(1),
}

DJ = TypeVar("DJ", Drone, Job)


class WrappedClassAd(ClassAd, Generic[DJ]):

    __slots__ = "_wrapped", "_temp"

    def __init__(self, classad: ClassAd, wrapped: DJ):
        super(WrappedClassAd, self).__init__()
        self._wrapped = wrapped
        self._data = classad._data
        self._temp = {}

    def __getitem__(self, item):
        def access_wrapped(name, requested=True):
            if isinstance(self._wrapped, Drone):
                return self._wrapped.theoretical_available_resources[name]
            if requested:
                return self._wrapped.resources[name]
            return self._wrapped.used_resources[name]

        if "target" not in item:
            if "requestcpus" in item:
                return access_wrapped("cores", requested=True)
            elif "requestmemory" in item:
                return (1 / 1024 / 1024) * access_wrapped("memory", requested=True)
            elif "requestdisk" in item:
                return (1 / 1024) * access_wrapped("disk", requested=True)
            elif "cpus" in item:
                try:
                    return self._temp["cores"]
                except KeyError:
                    return access_wrapped("cores", requested=False)
            elif "memory" in item:
                try:
                    return self._temp["memory"]
                except KeyError:
                    return (1 / 1000 / 1000) * access_wrapped("memory", requested=False)
            elif "disk" in item:
                try:
                    return self._temp["disk"]
                except KeyError:
                    return (1 / 1024) * access_wrapped("disk", requested=False)
        return super(WrappedClassAd, self).__getitem__(item)

    def clear_temporary_resources(self):
        self._temp.clear()

    def __repr__(self):
        return f"<{self.__class__.__name__}>: {self._wrapped}"

    def __eq__(self, other):
        return super().__eq__(other) and self._wrapped == other._wrapped


class Cluster(List[WrappedClassAd[DJ]], Generic[DJ]):
    pass


class Bucket(List[Cluster[DJ]], Generic[DJ]):
    pass


class JobScheduler(ABC):
    __slots__ = ()

    @property
    def drone_list(self) -> Iterator[Drone]:
        raise NotImplementedError

    def register_drone(self, drone: Drone):
        raise NotImplementedError

    def unregister_drone(self, drone: Drone):
        raise NotImplementedError

    def update_drone(self, drone: Drone):
        raise NotImplementedError

    async def run(self):
        raise NotImplementedError

    async def job_finished(self, job):
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

    def __init__(self, job_queue):
        self._stream_queue = job_queue
        self.drone_cluster = []
        self.interval = 60
        self.job_queue = JobQueue()
        self._collecting = True
        self._processing = Resources(jobs=0)

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

    def _add_drone(self, drone: Drone, drone_resources: Dict = None):
        minimum_distance_cluster = None
        distance = float("Inf")
        if len(self.drone_cluster) > 0:
            for cluster in self.drone_cluster:
                current_distance = 0
                for key in {*cluster[0].pool_resources, *drone.pool_resources}:
                    if drone_resources:
                        current_distance += abs(
                            cluster[0].theoretical_available_resources.get(key, 0)
                            - drone_resources.get(key, 0)
                        )
                    else:
                        current_distance += abs(
                            cluster[0].theoretical_available_resources.get(key, 0)
                            - drone.theoretical_available_resources.get(key, 0)
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

    def update_drone(self, drone: Drone):
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
                        left_resources = best_match.theoretical_available_resources
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

    async def job_finished(self, job):
        if job.successful:
            await self._processing.decrease(jobs=1)
        else:
            await self._stream_queue.put(job)

    def _schedule_job(self, job) -> Drone:
        priorities = {}
        for cluster in self.drone_cluster:
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


class CondorClassadJobScheduler(JobScheduler):
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
        self.drone_cluster: Dict[Tuple[float, ...], Cluster[WrappedClassAd[Drone]]] = {}
        self.job_cluster: Dict[Tuple[float, ...], Cluster[WrappedClassAd[Job]]] = {}
        self.interval = 60
        self.job_queue = JobQueue()
        self._collecting = True
        self._processing = Resources(jobs=0)

        # temporary solution
        self._wrapped_classads = WeakKeyDictionary()
        self._machine_classad = parse(
            """
        requirements = target.requestcpus <= my.cpus
        """
        )
        self._job_classad = parse(
            """
        requirements = my.requestcpus <= target.cpus && my.requestmemory <= target.memory
        """
        )

    @property
    def drone_list(self) -> Iterator[Drone]:
        for cluster in self.drone_cluster.values():
            for drone in cluster:
                yield drone._wrapped

    def register_drone(self, drone: Drone):
        wrapped_drone = WrappedClassAd(classad=self._machine_classad, wrapped=drone)
        self._wrapped_classads[drone] = wrapped_drone
        self._add_drone(wrapped_drone)

    def unregister_drone(self, drone: Drone):
        drone_wrapper = self._wrapped_classads[drone]
        for key in self.drone_cluster:
            try:
                self.drone_cluster[key].remove(drone_wrapper)
            except ValueError:
                pass
            else:
                break
        else:
            # nothing was removed
            return
        if len(self.drone_cluster[key]) == 0:
            del self.drone_cluster[key]

    @staticmethod
    def _clustering_key(resource_dict: Dict):
        clustering_key = []
        for key, value in resource_dict.items():
            clustering_key.append(
                int(quantize(value, quantization_defaults.get(key, 1)))
            )
        return tuple(clustering_key)

    def _add_drone(self, drone: WrappedClassAd, drone_resources: Dict = None):
        wrapped_drone = drone._wrapped
        if drone_resources:
            clustering_key = self._clustering_key(drone_resources)
        else:
            clustering_key = self._clustering_key(wrapped_drone.available_resources)
        self.drone_cluster.setdefault(clustering_key, Cluster()).append(drone)

    def update_drone(self, drone: Drone):
        self.unregister_drone(drone)
        self._add_drone(self._wrapped_classads[drone])

    def _sort_drone_cluster(self):
        return [Bucket(self.drone_cluster.values())]

    def _sort_job_cluster(self):
        return Bucket(self.job_cluster.values())

    async def run(self):
        def filter_drones(job: WrappedClassAd[Job], drone_bucket: Bucket[Drone]):
            result = {}  # type: Dict[Union[Undefined, float], Bucket[Drone]]
            for drones in drone_bucket:
                drone = drones[0]  # type: WrappedClassAd[Drone]
                if job.evaluate(
                    "requirements", my=job, target=drone
                ) and drone.evaluate("requirements", my=drone, target=job):
                    rank = job.evaluate("rank", my=job, target=drone)
                    result.setdefault(rank, Bucket()).append(drones)
            return result

        def pop_first(
            ranked_drones: Dict[Union[Undefined, float], Bucket[Drone]]
        ) -> Optional[WrappedClassAd[Drone]]:
            if not ranked_drones:
                return None
            key = sorted(ranked_drones)[0]
            values = ranked_drones[key]
            result = values[0]
            values.remove(result)
            if not values:
                del ranked_drones[key]
            try:
                return result[0]
            except IndexError:
                return pop_first(ranked_drones)

        async with Scope() as scope:
            scope.do(self._collect_jobs())
            async for _ in interval(self.interval):
                # TODO: get sorted job cluster [{Job, ...}, ...]
                # TODO: get set of drone cluster {{PSlot, ...}, ...}
                # TODO: get sorted drone clusters PreJob [{{PSlot, ...}, ...}, ...]
                # TODO: filter (Job.Requirements) and sort (Job.Rank) for job and drones => lazy

                all_drone_buckets = self._sort_drone_cluster()
                filtered_drones = {}
                for jobs in self._sort_job_cluster().copy():
                    current_drone_bucket = 0
                    for job in jobs:
                        best_match = pop_first(filtered_drones)
                        while best_match is None:
                            # lazily evaluate more PSlots
                            try:
                                # TODO: sort filtered_drones
                                filtered_drones = filter_drones(
                                    job, all_drone_buckets[current_drone_bucket]
                                )
                            except IndexError:
                                break
                            current_drone_bucket += 1
                            best_match = pop_first(filtered_drones)
                        else:
                            # TODO: update drone and check if it gets reinserted to filtered_drones
                            await self._execute_job(job=job, drone=best_match)
                if (
                    not self._collecting
                    and not self.job_queue
                    and self._processing.levels.jobs == 0
                ):
                    break
                await sampling_required.put(self)

    async def _execute_job(self, job: WrappedClassAd, drone: WrappedClassAd):
        wrapped_job = job._wrapped
        wrapped_drone = drone._wrapped
        await wrapped_drone.schedule_job(wrapped_job)
        self.job_queue.remove(job)
        cluster_key = self._clustering_key(wrapped_job.resources)
        self.job_cluster[cluster_key].remove(job)
        if len(self.job_cluster[cluster_key]) == 0:
            del self.job_cluster[cluster_key]
        await sampling_required.put(self.job_queue)
        self.unregister_drone(wrapped_drone)
        left_resources = {
            key: value - wrapped_job.resources.get(key, 0)
            for key, value in wrapped_drone.theoretical_available_resources.items()
        }
        self._add_drone(drone, left_resources)

    async def _collect_jobs(self):
        async for job in self._stream_queue:
            wrapped_job = WrappedClassAd(classad=self._job_classad, wrapped=job)
            self._wrapped_classads[job] = wrapped_job
            self.job_queue.append(wrapped_job)
            cluster_key = self._clustering_key(job.resources)
            self.job_cluster.setdefault(cluster_key, []).append(wrapped_job)
            await self._processing.increase(jobs=1)
            # TODO: logging happens with each job
            # TODO: job queue to the outside now contains wrapped classads...
            await sampling_required.put(self.job_queue)
        self._collecting = False

    async def job_finished(self, job):
        if job.successful:
            await self._processing.decrease(jobs=1)
        else:
            self.job_queue.append(self._wrapped_classads[job])
            cluster_key = self._clustering_key(job.resources)
            self.job_cluster.setdefault(cluster_key, []).append(
                self._wrapped_classads[job]
            )
