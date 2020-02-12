import logging
from typing import Optional, TYPE_CHECKING

from usim import time, Scope, instant
from usim import CancelTask

from lapis.monitor import sampling_required

if TYPE_CHECKING:
    from lapis.drone import Drone


class Job(object):
    __slots__ = (
        "resources",
        "used_resources",
        "walltime",
        "requested_walltime",
        "queue_date",
        "requested_inputfiles",
        "used_inputfiles",
        "in_queue_since",
        "in_queue_until",
        "_name",
        "drone",
        "_success",
        "calculation_efficiency",
        "__weakref__",
        "_coordinated",
        "_used_cache",
        "_total_input_data",
    )

    def __init__(
        self,
        resources: dict,
        used_resources: dict,
        in_queue_since: float = 0,
        queue_date: float = 0,
        name: Optional[str] = None,
        drone: "Optional[Drone]" = None,
        calculation_efficiency: Optional[float] = None,
    ):
        """
        Definition of a job that uses a specified amount of resources `used_resources`
        over a given amount of time, `walltime`. A job is described by its user
        via the parameter `resources`. This is a user prediction and is expected
        to deviate from `used_resources`.

        :param resources: Requested resources of the job
        :param used_resources: Resource usage of the job
        :param in_queue_since: Time when job was inserted into the queue of the
                               simulation scheduler
        :param queue_date: Time when job was inserted into queue in real life
        :param name: Name of the job
        :param drone: Drone where the job is running on
        """
        self.resources = resources
        self.used_resources = used_resources
        for key in used_resources:
            if key not in resources:
                logging.getLogger("implementation").info(
                    "job uses different resources than specified, added %s: %s",
                    key,
                    self.used_resources[key],
                )
                self.resources[key] = self.used_resources[key]
        self.walltime: int = used_resources.pop("walltime")
        self.requested_walltime: Optional[int] = resources.pop("walltime", None)
        self.queue_date = queue_date
        assert in_queue_since >= 0, "Queue time cannot be negative"
        self.in_queue_since = in_queue_since
        self.in_queue_until: Optional[float] = None
        self.drone = drone
        self._name = name
        self._success: Optional[bool] = None
        self.calculation_efficiency = calculation_efficiency

        # caching-related
        self.requested_inputfiles = resources.pop("inputfiles", None)
        self.used_inputfiles = used_resources.pop("inputfiles", None)
        self._coordinated = 0
        self._used_cache = 0
        try:
            self._total_input_data = sum(
                [fileinfo["usedsize"] for fileinfo in self.used_inputfiles.values()]
            )
        except AttributeError:
            self._total_input_data = 0

    @property
    def name(self) -> str:
        return self._name or id(self)

    @property
    def successful(self) -> Optional[bool]:
        return self._success

    @property
    def waiting_time(self) -> float:
        """
        The time the job spent in the simulators scheduling queue. `Inf` when
        the job is still waiting.

        :return: Time in queue
        """
        if self.in_queue_until is not None:
            return self.in_queue_until - self.in_queue_since
        return float("Inf")

    async def _calculate(self):
        """
        Determines a jobs calculation time based on the jobs CPU time and a
        calculation efficiency representing inefficient programming.
        :param calculation_efficiency:
        :return:
        """
        # print(
        #     f"WALLTIME: Job {self} @ {time.now}, "
        #     f"{self.used_resources.get('cores', None)}, "
        #     f"{self.calculation_efficiency}"
        # )
        result = self.walltime
        try:
            result = (
                self.used_resources["cores"] / self.calculation_efficiency
            ) * self.walltime
        except (KeyError, TypeError):
            pass
        # start = time.now
        await (time + result)
        # print(f"finished calculation at {time.now - start}")

    async def _transfer_inputfiles(self):
        try:
            # start = time.now
            # print(f"TRANSFERING INPUTFILES: Job {self} @ {start}")
            await self.drone.connection.transfer_files(
                drone=self.drone,
                requested_files=self.used_inputfiles,
                job_repr=repr(self),
            )
            # print(
            #     f"streamed inputfiles {self.used_inputfiles.keys()} for job {self} "
            #     f"in {time.now - start} timeunits, finished @ {time.now}"
            # )
        except AttributeError:
            pass

    async def run(self, drone: "Drone"):
        assert drone, "Jobs cannot run without a drone being assigned"
        self.drone = drone
        self.in_queue_until = time.now
        self._success = None
        await sampling_required.put(self)
        # print("running job {}  in drone {}".format(repr(self), repr(self.drone)))
        try:
            start = time.now
            async with Scope() as scope:
                await instant
                scope.do(self._transfer_inputfiles())
                scope.do(self._calculate())
        except CancelTask:
            self.drone = None
            self._success = False
            # TODO: in_queue_until is still set
        except BaseException:
            self.drone = None
            self._success = False
            # TODO: in_queue_until is still set
            raise
        else:
            old_walltime = self.walltime
            self.walltime = time.now - start
            print(f"monitored walltime of {old_walltime} changed to {self.walltime}")
            self.drone = None
            self._success = True
            await sampling_required.put(self)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self._name or id(self))


async def job_to_queue_scheduler(job_generator, job_queue):
    base_date = None
    for job in job_generator:
        if base_date is None:
            base_date = job.queue_date
        current_time = job.queue_date - base_date
        if time.now < current_time:
            await (time >= current_time)
        job.in_queue_since = time.now
        await job_queue.put(job)
    await job_queue.close()
