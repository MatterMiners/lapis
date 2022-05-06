from abc import abstractmethod
import random
from typing import Any, Dict, Generic, Iterator, List, NamedTuple, Set, Tuple, TypeVar
from weakref import WeakKeyDictionary

from sortedcontainers import SortedDict

from classad import parse
from classad._primitives import HTCInt, Undefined
from classad._expression import ClassAd
from classad._base_expression import Expression
from classad._functions import quantize
from usim import Scope, interval, Resources

from lapis.workernode import WorkerNode
from lapis.job import Job
from lapis.monitor.core import sampling_required
from lapis.scheduler.base import JobScheduler, JobQueue

quantization_defaults = {
    "memory": HTCInt(128 * 1024 * 1024),
    "disk": HTCInt(1024 * 1024),
    "cores": HTCInt(1),
}

# ClassAd attributes are not case sensitive
machine_ad_defaults = """
requirements = target.requestcpus <= my.cpus
""".strip()

job_ad_defaults = """
requirements = my.requestcpus <= target.cpus && my.requestmemory <= target.memory
"""

T = TypeVar("T")
DJ = TypeVar("DJ", WorkerNode, Job)


class WrappedClassAd(ClassAd, Generic[DJ]):
    """
    Combines the original job/drone object and the associated ClassAd.
    """

    __slots__ = "_wrapped", "_temp"

    def __init__(self, classad: ClassAd, wrapped: DJ):
        """
        Initialization for wrapped ClassAd

        :param classad: the wrapped objects ClassAd description
        :param wrapped: wrapped object, either job or drone
        """
        super(WrappedClassAd, self).__init__()
        self._wrapped = wrapped
        self._data = classad._data
        self._temp = {}

    def empty(self):
        """
        Only relevant for wrapped drones to determine whether there are no more
        resources left on a drone.

        :return: true if no CPU cores are available, false otherwise
        """
        try:
            return self._temp["cores"] < 1
        except KeyError:
            return self._wrapped.unallocated_resources["cores"] < 1

    def __getitem__(self, item):
        """
        This method is used when evaluating classad expressions.

        :param item: name of a quantity in the classad expression
        :return: current value of this item
        """

        def access_wrapped(name, requested=True):
            """
            Extracts the wrapped object's current quantity of a certain resource (
            cores, memory, disk)

            :param name: name of the resource that is to be accessed
            :param requested: false if name is a resource of the drone, true if name
            is a resource requested by a job
            :return: value of respective resource
            """
            if isinstance(self._wrapped, WorkerNode):
                return self._wrapped.unallocated_resources[name]
            if requested:
                return self._wrapped.resources[name]
            return self._wrapped.used_resources[name]

        if "target" not in item:
            if "requestcpus" == item:
                return access_wrapped("cores", requested=True)
            elif "requestmemory" == item:
                return (1 / 1024 / 1024) * access_wrapped("memory", requested=True)
            elif "requestdisk" == item:
                return (1 / 1024) * access_wrapped("disk", requested=True)
            elif "requestwalltime" == item:
                return self._wrapped.requested_walltime
            elif "cpus" == item:
                try:
                    return self._temp["cores"]
                except KeyError:
                    return access_wrapped("cores", requested=False)
            elif "memory" == item:
                try:
                    return (1 / 1000 / 1000) * self._temp["memory"]
                except KeyError:
                    return (1 / 1000 / 1000) * access_wrapped("memory", requested=False)
            elif "disk" == item:
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


class NoMatch(Exception):
    """A job could not be matched to any drone"""


class RankedClusterKey(NamedTuple):
    rank: float
    key: Tuple[float, ...]


RC = TypeVar("RC", bound="RankedClusters")


class RankedClusters(Generic[DJ]):
    """Automatically cluster drones by rank"""

    @abstractmethod
    def __init__(self, quantization: Dict[str, HTCInt], ranking: Expression):
        raise NotImplementedError

    @abstractmethod
    def empty(self) -> bool:
        """Whether there are no resources available"""
        raise NotImplementedError

    @abstractmethod
    def copy(self: "RankedAutoClusters[DJ]") -> "RankedAutoClusters[DJ]":
        """Copy the entire ranked auto clusters"""
        raise NotImplementedError

    @abstractmethod
    def add(self, item: WrappedClassAd[DJ]) -> None:
        """Add a new item"""
        raise NotImplementedError

    @abstractmethod
    def remove(self, item: WrappedClassAd[DJ]) -> None:
        """Remove an existing item"""
        raise NotImplementedError

    def update(self, item) -> None:
        """Update an existing item with its current state"""
        self.remove(item)
        self.add(item)

    @abstractmethod
    def clusters(self) -> Iterator[Set[WrappedClassAd[DJ]]]:
        raise NotImplementedError

    @abstractmethod
    def items(self) -> Iterator[Tuple[Any, Set[WrappedClassAd[DJ]]]]:
        raise NotImplementedError

    @abstractmethod
    def cluster_groups(self) -> Iterator[List[Set[WrappedClassAd[WorkerNode]]]]:
        """Group autoclusters by PreJobRank"""
        raise NotImplementedError


class RankedAutoClusters(RankedClusters[DJ]):
    """Automatically cluster similar jobs or drones"""

    def __init__(self, quantization: Dict[str, HTCInt], ranking: Expression):
        """
        :param quantization: factors to convert resources into HTCondor scaling
        :param ranking: prejobrank expression
        """
        self._quantization = quantization
        self._ranking = ranking
        self._clusters: Dict[RankedClusterKey, Set[WrappedClassAd[DJ]]] = SortedDict()
        self._inverse: Dict[WrappedClassAd[DJ], RankedClusterKey] = {}

    def empty(self) -> bool:
        """
        Checks whether all drones in the RankedCluster are empty and currently not
        running any jobs.

        :return:
        """
        for drones in self._clusters.values():
            if not next(iter(drones)).empty():
                return False
        return True

    def copy(self) -> "RankedAutoClusters[DJ]":
        clone = type(self)(quantization=self._quantization, ranking=self._ranking)
        clone._clusters = SortedDict(
            (key, value.copy()) for key, value in self._clusters.items()
        )
        clone._inverse = self._inverse.copy()
        return clone

    def add(self, item: WrappedClassAd[DJ]):
        """
        Add a new wrapped item, usually a drone, to the RankedAutoCluster.
        Unless the item is already contained, the item's key is generated and it is
        sorted in into the clusters accordingly. If there are already items with the
        same key, the new item is added to the existing cluster. If not,
        a new cluster is created.

        :param item:
        :return:
        """
        if item in self._inverse:
            raise ValueError(f"{item!r} already stored; use `.update(item)` instead")
        item_key = self._clustering_key(item)
        try:
            self._clusters[item_key].add(item)
        except KeyError:
            self._clusters[item_key] = {item}
        self._inverse[item] = item_key

    def remove(self, item: WrappedClassAd[DJ]):
        """
        Removes the item.

        :param item:
        :return:
        """
        item_key = self._inverse.pop(item)
        cluster = self._clusters[item_key]
        cluster.remove(item)
        if not cluster:
            del self._clusters[item_key]

    def _clustering_key(self, item: WrappedClassAd[DJ]):
        """
        Calculates an item's clustering key based on the specified ranking (in my use
        case the prejobrank) and the item's available resource. The resulting key's
        structure is (prejobrank value, (available cpus, available memory, available
        disk space)). The clustering key is negative as the SortedDict sorts its entries
        from low keys to high keys.

        :param item: drone for which the clustering key is calculated.
        :return: (prejobrank value, (available cpus, available memory, available
        disk space))
        """
        # TODO: assert that order is consistent
        quantization = self._quantization
        return RankedClusterKey(
            rank=-1.0 * self._ranking.evaluate(my=item),
            key=tuple(
                int(quantize(item[key], quantization.get(key, 1)))
                for key in ("cpus", "memory", "disk")
            ),
        )

    def clusters(self) -> Iterator[Set[WrappedClassAd[DJ]]]:
        """
        :return: iterator of all clusters
        """
        return iter(self._clusters.values())

    def items(self) -> Iterator[Tuple[RankedClusterKey, Set[WrappedClassAd[DJ]]]]:
        """
        :return: iterator of all clusters and corresponding keys
        """
        return iter(self._clusters.items())

    def cluster_groups(self) -> Iterator[List[Set[WrappedClassAd[WorkerNode]]]]:
        """
        Sort clusters by the ranking key and then by the amount of available
        resources into nested lists of sets.

        :return:
        """
        group = []
        current_rank = None
        for ranked_key, drones in self._clusters.items():
            if next(iter(drones)).empty():
                continue
            if ranked_key.rank != current_rank:
                current_rank = ranked_key.rank
                if group:
                    yield group
                    group = []
            group.append(drones)
        if group:
            yield group


class RankedNonClusters(RankedClusters[DJ]):
    """Automatically cluster jobs or drones by rank only"""

    def __init__(self, quantization: Dict[str, HTCInt], ranking: Expression):
        self._quantization = quantization
        self._ranking = ranking
        self._clusters: Dict[float, Set[WrappedClassAd[DJ]]] = SortedDict()
        self._inverse: Dict[WrappedClassAd[DJ], float] = {}

    def empty(self) -> bool:
        for drones in self._clusters.values():
            for drone in drones:
                if not drone.empty():
                    return False
        return True

    def copy(self) -> "RankedNonClusters[DJ]":
        clone = type(self)(quantization=self._quantization, ranking=self._ranking)
        clone._clusters = SortedDict(
            (key, value.copy()) for key, value in self._clusters.items()
        )
        clone._inverse = self._inverse.copy()
        return clone

    def add(self, item: WrappedClassAd[DJ]):
        if item in self._inverse:
            raise ValueError(f"{item!r} already stored; use `.update(item)` instead")
        item_key = self._clustering_key(item)
        try:
            self._clusters[item_key].add(item)
        except KeyError:
            self._clusters[item_key] = {item}
        self._inverse[item] = item_key

    def remove(self, item: WrappedClassAd[DJ]):
        item_key = self._inverse.pop(item)
        cluster = self._clusters[item_key]
        cluster.remove(item)
        if not cluster:
            del self._clusters[item_key]

    def update(self, item):
        self.remove(item)
        self.add(item)

    def _clustering_key(self, item: WrappedClassAd[DJ]):
        """
        For RankNonClusters there is only one clustering key, the objects defined
        ranking. The clustering key is negative as the SortedDict sorts its entries
        from low keys to high keys.
        """
        return -1.0 * self._ranking.evaluate(my=item)

    def clusters(self) -> Iterator[Set[WrappedClassAd[DJ]]]:
        return iter(self._clusters.values())

    def items(self) -> Iterator[Tuple[float, Set[WrappedClassAd[DJ]]]]:
        return iter(self._clusters.items())

    def cluster_groups(self) -> Iterator[List[Set[WrappedClassAd[WorkerNode]]]]:
        """
        Sorts cluster by the ranking key. As there is no autoclustering, every drone
        is in a dedicated set and drones of the same ranking are combined into a list.
        These lists are then sorted by increasing ranking.

        :return: iterator of the lists containing drones with identical key
        """
        for _ranked_key, drones in self._clusters.items():
            yield [{item} for item in drones]


class CondorClassadJobScheduler(JobScheduler):
    """
    Goal of the htcondor job scheduler is to have a scheduler that somehow
    mimics how htcondor does schedule jobs.
    Htcondor does scheduling based on a priority queue. The priorities itself
    are managed by operators of htcondor.
    So different instances can apparently behave very different.
    In this case a priority queue that sorts job slots
    by increasing cost is built. The scheduler checks if a job either
    exactly fits a slot or if it does fit into it several times. The cost for
    putting a job at a given slot is given by the amount of resources that
    might remain unallocated.
    """

    def __init__(
        self,
        job_queue,
        machine_ad: str = machine_ad_defaults,
        job_ad: str = job_ad_defaults,
        pre_job_rank: str = "0",
        interval: float = 60,
    ):
        """
        Initializes the CondorClassadJobScheduler

        :param job_queue: queue of jobs that are scheduled in the following simulation
        :param machine_ad: ClassAd that is used with every drone
        :param job_ad: ClassAd that is used with every job
        :param pre_job_rank: ClassAd attribute that all drones are sorted by
        :param interval: time between scheduling cycles
        """
        self._stream_queue = job_queue
        self._drones: RankedClusters[WorkerNode] = RankedNonClusters(
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
    def drone_list(self) -> Iterator[WorkerNode]:
        """
        Takes an iterator over the WrappedClassAd objects of drones known to the
        scheduler, extracts the drones and returns an iterator over the drone objects.

        :return:
        """
        for cluster in self._drones.clusters():
            for drone in cluster:
                yield drone._wrapped

    def register_drone(self, drone: WorkerNode):
        """
        Provides the drones with the drone ClassAd, combines both into one object and
        adds the resulting WrappedClassAd object to the drones known to the scheduler as
        well as the dictionary containing all WrappedClassAd objects the scheduler
        works with.

        :param drone:
        """
        wrapped_drone = WrappedClassAd(classad=self._machine_classad, wrapped=drone)
        self._drones.add(wrapped_drone)
        self._wrapped_classads[drone] = wrapped_drone

    def unregister_drone(self, drone: WorkerNode):
        """
        Remove a drone's representation from the scheduler's scope.

        :param drone:
        :return:
        """
        drone_wrapper = self._wrapped_classads[drone]
        self._drones.remove(drone_wrapper)

    def update_drone(self, drone: WorkerNode):
        """
        Update a drone's representation in the scheduler scope.

        :param drone:
        :return:
        """
        drone_wrapper = self._wrapped_classads[drone]
        self._drones.update(drone_wrapper)

    async def run(self):
        """
        Runs the scheduler's functionality. One executed, the scheduler starts up and
        begins to add the jobs that are

        :return:
        """
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
        job: ClassAd, pre_job_clusters: Iterator[List[Set[WrappedClassAd[WorkerNode]]]]
    ):
        """
        Tries to find a match for the transferred job among the available drones.

        :param job: job to match
        :param pre_job_clusters: list of clusters of wrapped drones that are
        presorted by a clustering mechanism of RankedAutoClusters/RankedNonClusters
        that mimics the HTCondor NEGOTIATOR_PRE_JOB_RANK, short prejobrank. The
        clusters contain drones that are considered to be equivalent with respect to all
        Requirements and Ranks
        that are used during the matchmaking process. This mimics the Autoclustering
        functionality of HTCondor.
        [[highest prejobrank {autocluster}, {autocluster}], ..., [lowest prejobrank {
        autocluster}, {autocluster}]
        :return: drone that is the best match for the job

        The matching is performed in several steps:
        1. The job's requirements are evaluted and only drones that meet them are
        considered further. A drone of every autocluster is extracted from
        pre_job_clusters and if it meets the job's requirements it is not removed
        from pre_job_clusters.
        2. The autoclusters that are equivalent with respect to the prejobrank are
        then sorted by the job's rank expression. The resulting format of
        pre_job_clusters is
        [[(highest prejobrank, highest jobrank) {autocluster} {autocluster},
        ...,  (highest prejobrank, lowest jobrank) {autocluster}], ...]
        3. The resulting pre_job_clusters are then iterated and the drone with the
        highest (prejobrank, jobrank) whose requirements are also compatible with the
        job is returned as best match.
        """

        def debug_evaluate(expr, my, target=None):
            """
            Reimplementation of the classad packages evaluate function. Having it
            here enables developers to inspect the ClassAd evaluation process more
            closely and to add debug output if necessary.

            :param expr:
            :param my:
            :param target:
            :return:
            """
            if type(expr) is str:
                expr = my[expr]
            result = expr.evaluate(my=my, target=target)
            return result

        if job["Requirements"] != Undefined():
            pre_job_clusters_tmp = []
            for cluster_group in pre_job_clusters:
                cluster_group_tmp = []
                for cluster in cluster_group:
                    if debug_evaluate(
                        "Requirements", my=job, target=next(iter(cluster))
                    ):
                        cluster_group_tmp.append(cluster)
                pre_job_clusters_tmp.append(cluster_group_tmp)
            pre_job_clusters = pre_job_clusters_tmp

        if job["Rank"] != Undefined():
            pre_job_clusters_tmp = []
            for cluster_group in pre_job_clusters:
                pre_job_clusters_tmp.append(
                    sorted(
                        cluster_group,
                        key=lambda cluster: (
                            debug_evaluate("Rank", my=job, target=next(iter(cluster))),
                            random.random(),
                        ),
                        reverse=True,
                    )
                )
            pre_job_clusters = pre_job_clusters_tmp

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
        """
        Handles the scheduling of jobs. Tried to match the jobs in the job queue to
        available resources. This occurs in several steps.
        1. The list of drones known to the scheduler is copied. The copy can then be
        used to keep track of the drones' available resources while matching jobs as
        the jobs allocate resources on the original drones before being processed but
        not during scheduling.
        2. The job in the job queue are matched to (the copied)resources iteratively.
        The actual matching is performed by the `_match_job` method that returns the
        most suitable drone unless no drone is compatible with the job's requirements.
        If a match was found, the resources requested by the job are allocated on the
        matched drone. If no resources remain unallocated after the last job's
        allocation, the matching process is ended for this scheduler interval.
        3. After the job matching is finished, the matched jobs are removed from the
        job queue as the index of a job in the job queue changes once a job with a
        lower index is removed from the queue.
        4. The matched jobs' execution is triggered.

        """
        # Pre CachingJob Rank is the same for all jobs
        # Use a copy to allow temporary "remainder after match" estimates
        if self._drones.empty():
            return  # early exit in case of no more available resources
        pre_job_drones = self._drones.copy()
        matches: List[Tuple[int, WrappedClassAd[Job], WrappedClassAd[WorkerNode]]] = []
        for queue_index, candidate_job in enumerate(self.job_queue):
            try:
                matched_drone = self._match_job(
                    candidate_job, pre_job_drones.cluster_groups()
                )
            except NoMatch:
                candidate_job._wrapped.failed_matches += 1
                continue
            else:
                matches.append((queue_index, candidate_job, matched_drone))
                for key, value in candidate_job._wrapped.resources.items():
                    matched_drone._temp[key] = (
                        matched_drone._temp.get(
                            key,
                            matched_drone._wrapped.unallocated_resources[key],
                        )
                        - value
                    )
                pre_job_drones.update(matched_drone)

                if pre_job_drones.empty():
                    break
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
        """
        Schedules a job on a drone by extracting both objects from the
        respective WrappedClassAd and using the drone's scheduling functionality

        :param job:
        :param drone:
        """
        wrapped_job = job._wrapped
        wrapped_drone = drone._wrapped
        await wrapped_drone.schedule_job(wrapped_job)

    async def _collect_jobs(self):
        """
        Combines jobs that are imported from the simulation's job config with a job
        ClassAd and adds the resulting WrappedClassAd objects to the scheduler's job
        queue.
        """
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
        """
        Handles the impact of finishing jobs on the scheduler. If the job is completed
        successfully, the amount of running jobs matched by the current scheduler
        instance is reduced. If the job is not finished successfully,
        it is resubmitted to the scheduler's job queue.
        :param job:
        """
        if job.successful:
            await self._processing.decrease(jobs=1)
        else:
            self.job_queue.append(self._wrapped_classads[job])
