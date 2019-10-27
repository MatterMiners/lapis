import logging
from typing import Optional, TYPE_CHECKING

from usim import time
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
        "in_queue_since",
        "in_queue_until",
        "_name",
        "drone",
        "_success",
    )

    def __init__(
        self,
        resources: dict,
        used_resources: dict,
        in_queue_since: float = 0,
        queue_date: float = 0,
        name: str = None,
        drone: "Drone" = None,
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
        self.walltime = used_resources.pop("walltime")
        self.requested_walltime = resources.pop("walltime", None)
        self.queue_date = queue_date
        assert in_queue_since >= 0, "Queue time cannot be negative"
        self.in_queue_since = in_queue_since
        self.in_queue_until = None
        self.drone = drone
        self._name = name
        self._success: Optional[bool] = None

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
        the job is still waitiing.

        :return: Time in queue
        """
        if self.in_queue_until is not None:
            return self.in_queue_until - self.in_queue_since
        return float("Inf")

    async def run(self):
        self.in_queue_until = time.now
        self._success = None
        await sampling_required.put(self)
        try:
            await (time + self.walltime)
        except CancelTask:
            self._success = False
        except BaseException:
            self._success = False
            raise
        else:
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
