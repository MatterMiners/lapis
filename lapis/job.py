import logging
from typing import Optional, TYPE_CHECKING, Dict

from usim import time, Queue
from usim import CancelTask

from lapis.monitor.core import sampling_required

if TYPE_CHECKING:
    from lapis.workernode import WorkerNode


class Job(object):
    __slots__ = (
        "resources",
        "used_resources",
        "walltime",
        "requested_walltime",
        "queue_date",
        "in_queue_since",
        "in_queue_until",
        "_name",
        "drone",
        "_success",
    )

    def __init__(
        self,
        resources: Dict[str, float],
        used_resources: Dict[str, float],
        in_queue_since: float = 0,
        queue_date: float = 0,
        name: str = None,
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
        self.walltime: float = used_resources.pop("walltime")
        """the job's runtime, in reality as well as in the simulation"""
        self.requested_walltime: Optional[float] = resources.pop("walltime", None)
        """estimate of the job's walltime"""
        self.queue_date = queue_date
        """ point in time when the job was submitted to the simulated job queue"""
        assert in_queue_since >= 0, "Queue time cannot be negative"
        self.in_queue_since = in_queue_since
        """Time when job was inserted into the queue of the simulation scheduler"""
        self.in_queue_until: Optional[float] = None
        """point in time when the job left the job queue"""
        self.drone = None
        self._name = name
        """identifier of the job"""
        self._success: Optional[bool] = None
        """flag indicating whether the job was completed successfully"""

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

    async def run(self, drone: "WorkerNode"):
        assert drone, "Jobs cannot run without a drone being assigned"
        self.drone = drone
        self.in_queue_until = time.now
        self._success = None
        await sampling_required.put(self)
        try:
            await (time + self.walltime)
        except CancelTask:
            self.drone = None
            self._success = False
        except BaseException:
            self.drone = None
            self._success = False
            raise
        else:
            self.drone = None
            self._success = True
        await sampling_required.put(self)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self._name or id(self))


async def job_to_queue_scheduler(job_generator, job_queue: Queue):
    """
    Handles reading the simulation's job input and puts the job's into the job queue

    :param job_generator: reader object that yields jobs from input
    :param job_queue: queue the jobs are added to
    """
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
