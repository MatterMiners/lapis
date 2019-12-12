from typing import Dict, Union
from weakref import WeakKeyDictionary

from classad import parse
from classad._functions import quantize
from classad._primitives import HTCInt
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


class WrappedClassAd(ClassAd):

    __slots__ = "_wrapped"

    _wrapped: Union[Job, Drone]

    def __init__(self, classad: ClassAd, wrapped: Union[Job, Drone]):
        super(WrappedClassAd, self).__init__()
        self._wrapped = wrapped
        self._data = classad._data

    def __getitem__(self, item):
        def access_wrapped(name, requested=True):
            if isinstance(self._wrapped, Drone):
                if requested:
                    return self._wrapped.theoretical_available_resources[name]
                return self._wrapped.available_resources[name]
            if requested:
                return self._wrapped.resources[name]
            return self._wrapped.used_resources[name]

        if "target" not in item:
            if "requestcpus" in item:
                return access_wrapped("cores", requested=True)
            elif "requestmemory" in item:
                return 0.000000953674316 * access_wrapped("memory", requested=True)
            elif "requestdisk" in item:
                return 0.0009765625 * access_wrapped("disk", requested=True)
            elif "cpus" in item:
                return access_wrapped("cores", requested=False)
            elif "memory" in item:
                return 0.000001 * access_wrapped("memory", requested=False)
            elif "disk" in item:
                return 0.0009765625 * access_wrapped("disk", requested=False)
        return super(WrappedClassAd, self).__getitem__(item)

    def __repr__(self):
        return f"<{self.__class__.__name__}>: {self._wrapped}"

    def __eq__(self, other):
        return super().__eq__(other) and self._wrapped == other._wrapped


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
        self.job_cluster = {}  # TODO: should be sorted
        self.interval = 60
        self.job_queue = JobQueue()
        self._collecting = True
        self._processing = Resources(jobs=0)

        # temporary solution
        self._wrapped_classads = WeakKeyDictionary()
        self._machine_classad = parse(
            """
        requirements = target.requestcpus > my.cpus
        """
        )
        self._job_classad = parse(
            """
        requirements = my.requestcpus <= target.cpus && my.requestmemory <= target.memory
        """
        )

    @property
    def drone_list(self):
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

    def _clustering_key(self, resource_dict: Dict):
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
            # TODO: I think this should be available_resources
            clustering_key = self._clustering_key(
                wrapped_drone.theoretical_available_resources
            )
        self.drone_cluster.setdefault(clustering_key, []).append(drone)

    def update_drone(self, drone: Drone):
        self.unregister_drone(drone)
        self._add_drone(self._wrapped_classads[drone])

    def _sort_drone_cluster(self):
        return [[list(drones) for drones in self.drone_cluster.values()]]

    def _sort_job_cluster(self):
        return list(self.job_cluster.values())

    async def run(self):
        def filter_drones(job, drone_bucket):
            result = {}
            for drones in drone_bucket:
                drone = drones[0]
                filtered = job.evaluate("requirements", my=job, target=drone)
                if filtered:
                    rank = job.evaluate("rank", my=job, target=drone)
                    result.setdefault(rank, []).append(drones)
            return result

        def pop_first(ranked_drones: Dict):
            keys = sorted(ranked_drones.keys())
            if len(keys) == 0:
                return None
            values = ranked_drones.get(keys[0])
            result = values[0]
            values.remove(result)
            if len(values) == 0:
                del ranked_drones[keys[0]]
            return result[0]

        async with Scope() as scope:
            scope.do(self._collect_jobs())
            async for _ in interval(self.interval):
                # TODO: get sorted job cluster [{Job, ...}, ...]
                # TODO: get set of drone cluster {{PSlot, ...}, ...}
                # TODO: get sorted drone clusters PreJob [{{PSlot, ...}, ...}, ...]
                # TODO: filter (Job.Requirements) and sort (Job.Rank) for job and drones => lazy

                all_drone_buckets = self._sort_drone_cluster().copy()
                filtered_drones = {}
                current_drone_bucket = 0
                for jobs in self._sort_job_cluster().copy():
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
