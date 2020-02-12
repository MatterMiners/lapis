from abc import ABC
from typing import Dict, Iterator, Tuple, List, TypeVar, Generic, Set, NamedTuple
from weakref import WeakKeyDictionary

from sortedcontainers import SortedDict

from classad import parse
from classad._base_expression import Expression
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

machine_ad_defaults = """
requirements = target.requestcpus <= my.cpus
""".strip()

job_ad_defaults = """
requirements = my.requestcpus <= target.cpus && my.requestmemory <= target.memory
"""

T = TypeVar("T")
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
                    return (1 / 1000 / 1000) * self._temp["memory"]
                except KeyError:
                    return (1 / 1000 / 1000) * access_wrapped("memory", requested=False)
            elif "disk" in item:
                try:
                    return (1 / 1024) * self._temp["disk"]
                except KeyError:
                    return (1 / 1024) * access_wrapped("disk", requested=False)
        return super(WrappedClassAd, self).__getitem__(item)

    def clear_temporary_resources(self):
        self._temp.clear()

    def __repr__(self):
        return f"<{self.__class__.__name__}>: {self._wrapped}"

    def __eq__(self, other):
        return super().__eq__(other) and self._wrapped == other._wrapped

    def __hash__(self):
        return id(self._wrapped)


class Cluster(List[WrappedClassAd[DJ]], Generic[DJ]):
    pass


class Bucket(List[Cluster[DJ]], Generic[DJ]):
    pass


class JobScheduler(ABC):
    __slots__ = ()

    @property
    def drone_list(self) -> Iterator[Drone]:
        """Yields the registered drones"""
        raise NotImplementedError

    def register_drone(self, drone: Drone):
        """Register a drone at the scheduler"""
        raise NotImplementedError

    def unregister_drone(self, drone: Drone):
        """Unregister a drone at the scheduler"""
        raise NotImplementedError

    def update_drone(self, drone: Drone):
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


# HTCondor ClassAd Scheduler


class NoMatch(Exception):
    """A job could not be matched to any drone"""


class RankedClusterKey(NamedTuple):
    rank: float
    key: Tuple[float, ...]


class RankedAutoClusters(Generic[DJ]):
    """Automatically cluster similar jobs or drones"""

    def __init__(self, quantization: Dict[str, HTCInt], ranking: Expression):
        self._quantization = quantization
        self._ranking = ranking
        self._clusters: Dict[RankedClusterKey, Set[WrappedClassAd[DJ]]] = SortedDict()
        self._inverse: Dict[WrappedClassAd[DJ], RankedClusterKey] = {}

    def copy(self) -> "RankedAutoClusters[DJ]":
        """Copy the entire ranked auto clusters"""
        clone = type(self)(quantization=self._quantization, ranking=self._ranking)
        clone._clusters = SortedDict(
            (key, value.copy()) for key, value in self._clusters.items()
        )
        clone._inverse = self._inverse.copy()
        return clone

    def add(self, item: WrappedClassAd[DJ]):
        """Add a new item"""
        if item in self._inverse:
            raise ValueError(f"{item!r} already stored; use `.update(item)` instead")
        item_key = self._clustering_key(item)
        try:
            self._clusters[item_key].add(item)
        except KeyError:
            self._clusters[item_key] = {item}
        self._inverse[item] = item_key

    def remove(self, item: WrappedClassAd[DJ]):
        """Remove an existing item"""
        item_key = self._inverse.pop(item)
        cluster = self._clusters[item_key]
        cluster.remove(item)
        if not cluster:
            del self._clusters[item_key]

    def update(self, item):
        """Update an existing item with its current state"""
        self.remove(item)
        self.add(item)

    def _clustering_key(self, item: WrappedClassAd[DJ]):
        # TODO: assert that order is consistent
        quantization = self._quantization
        return RankedClusterKey(
            rank=self._ranking.evaluate(my=item),
            key=tuple(
                int(quantize(value, quantization.get(key, 1)))
                for key, value in item._wrapped.available_resources.items()
            ),
        )

    def clusters(self) -> Iterator[Set[WrappedClassAd[DJ]]]:
        return iter(self._clusters.values())

    def items(self) -> Iterator[Tuple[RankedClusterKey, Set[WrappedClassAd[DJ]]]]:
        return iter(self._clusters.items())

    def cluster_groups(self) -> Iterator[List[Set[WrappedClassAd[Drone]]]]:
        """Group autoclusters by PreJobRank"""
        group = []
        current_rank = None
        for ranked_key, drones in self._clusters.items():
            if ranked_key.rank != current_rank:
                current_rank = ranked_key.rank
                if group:
                    yield group
                    group = []
            group.append(drones)
        if group:
            yield group


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

    def __init__(
        self,
        job_queue,
        machine_ad: str = machine_ad_defaults,
        job_ad: str = job_ad_defaults,
        pre_job_rank: str = "0",
        interval: float = 60,
    ):
        self._stream_queue = job_queue
        self._drones: RankedAutoClusters[Drone] = RankedAutoClusters(
            quantization=quantization_defaults, ranking=parse(pre_job_rank)
        )
        self.interval = interval
        self.job_queue = JobQueue()
        self._collecting = True
        self._processing = Resources(jobs=0)

        # temporary solution
        self._wrapped_classads = WeakKeyDictionary()
        self._machine_classad = parse(machine_ad)
        self._job_classad = parse(job_ad)

    @property
    def drone_list(self) -> Iterator[Drone]:
        for cluster in self._drones.clusters():
            for drone in cluster:
                yield drone._wrapped

    def register_drone(self, drone: Drone):
        wrapped_drone = WrappedClassAd(classad=self._machine_classad, wrapped=drone)
        self._wrapped_classads[drone] = wrapped_drone
        self._drones.add(wrapped_drone)

    def unregister_drone(self, drone: Drone):
        drone_wrapper = self._wrapped_classads[drone]
        self._drones.remove(drone_wrapper)

    def update_drone(self, drone: Drone):
        drone_wrapper = self._wrapped_classads[drone]
        self._drones.update(drone_wrapper)

    async def run(self):
        async with Scope() as scope:
            scope.do(self._collect_jobs())
            async for _ in interval(self.interval):
                await self._schedule_jobs()
                if (
                    not self._collecting
                    and not self.job_queue
                    and self._processing.levels.jobs == 0
                ):
                    break

    @staticmethod
    def _match_job(
        job: ClassAd, pre_job_clusters: Iterator[List[Set[WrappedClassAd[Drone]]]]
    ):
        if job["Requirements"] != Undefined():
            pre_job_clusters = (
                [
                    cluster
                    for cluster in cluster_group
                    if job.evaluate("Requirements", my=job, target=next(iter(cluster)))
                ]
                for cluster_group in pre_job_clusters
            )
        if job["Rank"] != Undefined():
            pre_job_clusters = (
                sorted(
                    cluster_group,
                    key=lambda cluster: job.evaluate(
                        "Rank", my=job, target=next(iter(cluster))
                    ),
                )
                for cluster_group in pre_job_clusters
            )
        for cluster_group in pre_job_clusters:
            # TODO: if we have POST_JOB_RANK, collect *all* matches of a group
            for cluster in cluster_group:
                for drone in cluster:
                    if drone["Requirements"] == Undefined() or drone.evaluate(
                        "Requirements", my=drone, target=job
                    ):
                        return drone
        raise NoMatch()

    async def _schedule_jobs(self):
        # Pre Job Rank is the same for all jobs
        # Use a copy to allow temporary "remainder after match" estimates
        pre_job_drones = self._drones.copy()
        matches: List[Tuple[int, WrappedClassAd[Job], WrappedClassAd[Drone]]] = []
        for queue_index, candidate_job in enumerate(self.job_queue):
            try:
                matched_drone = self._match_job(
                    candidate_job, pre_job_drones.cluster_groups()
                )
            except NoMatch:
                continue
            else:
                matches.append((queue_index, candidate_job, matched_drone))
                for key, value in enumerate(candidate_job._wrapped.resources):
                    matched_drone._temp[key] = (
                        matched_drone._temp.get(
                            key,
                            matched_drone._wrapped.theoretical_available_resources[key],
                        )
                        - value
                    )
                pre_job_drones.update(matched_drone)
        if not matches:
            return
        # TODO: optimize for few matches, many matches, all matches
        for queue_index, _, _ in reversed(matches):
            del self.job_queue[queue_index]
        for _, job, drone in matches:
            drone.clear_temporary_resources()
            await self._execute_job(job=job, drone=drone)
        await sampling_required.put(self)
        # NOTE: Is this correct? Triggers once instead of for each job
        await sampling_required.put(self.job_queue)

    async def _execute_job(self, job: WrappedClassAd, drone: WrappedClassAd):
        wrapped_job = job._wrapped
        wrapped_drone = drone._wrapped
        await wrapped_drone.schedule_job(wrapped_job)

    async def _collect_jobs(self):
        async for job in self._stream_queue:
            wrapped_job = WrappedClassAd(classad=self._job_classad, wrapped=job)
            self._wrapped_classads[job] = wrapped_job
            self.job_queue.append(wrapped_job)
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
