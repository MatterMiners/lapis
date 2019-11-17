import logging
from typing import Optional, TYPE_CHECKING

from usim import time
from usim import CancelTask

from lapis.monitor import sampling_required
from lapis.utilities.walltime_models import walltime_models

if TYPE_CHECKING:
    from lapis.drone import Drone


class Job(object):
    __slots__ = (
        "resources",
        "used_resources",
        "_walltime",
        "_streamtime",
        "requested_walltime",
        "queue_date",
        "requested_inputfiles",
        "used_inputfiles",
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
        self._walltime = used_resources.pop("walltime")
        self._streamtime = 0
        self.requested_walltime = resources.pop("walltime", None)
        self.requested_inputfiles = resources.pop("inputfiles", None)
        self.used_inputfiles = used_resources.pop("inputfiles", None)
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

    @property
    def walltime(self) -> float:
        """
        :return: Time that passes while job is running
        """
        return self._streamtime + self.calculation_time()

    def calculation_time(self):
        print("WALLTIME: Job {} @ {}".format(repr(self), time.now))
        return walltime_models["maxeff"](self, self._walltime)

    async def transfer_inputfiles(self):
        print("TRANSFERING INPUTFILES: Job {} @ {}".format(repr(self), time.now))
        if self.drone.fileprovider and self.used_inputfiles:
            self._streamtime = await self.drone.fileprovider.transfer_inputfiles(
                self.drone, self.requested_inputfiles, repr(self)
            )

        print(
            "streamed inputfiles {} for job {} in {} timeunits, finished @ {}".format(
                self.requested_inputfiles.keys(), repr(self), self._streamtime, time.now
            )
        )

    async def run(self):
        self.in_queue_until = time.now
        self._success = None
        await sampling_required.put(self)
        if self.drone:
            print(
                "running job {} on site {} in drone {}".format(
                    repr(self), self.drone.sitename, repr(self.drone)
                )
            )
        try:
            await self.transfer_inputfiles()
            await (time + self.calculation_time())
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
