from typing import TYPE_CHECKING

import logging.handlers

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.drone import Drone
from lapis.job import Job
from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler
from lapis.pool import Pool
from lapis.scheduler import CondorJobScheduler, JobQueue

if TYPE_CHECKING:
    from lapis.simulator import Simulator


def resource_statistics(drone: Drone) -> list:
    """
    Log ratio of used and requested resources for drones.

    :param drone: the drone
    :return: list of records for logging
    """
    results = []
    resources = drone.theoretical_available_resources
    used_resources = drone.available_resources
    for resource_type in resources:
        results.append(
            {
                "resource_type": resource_type,
                "pool_configuration": "None",
                "pool_type": "drone",
                "pool": repr(drone),
                "used_ratio": 1
                - used_resources[resource_type] / drone.pool_resources[resource_type],
                "requested_ratio": 1
                - resources[resource_type] / drone.pool_resources[resource_type],
            }
        )
    return results


resource_statistics.name = "resource_status"
resource_statistics.whitelist = (Drone,)
resource_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "resource_type", "pool_configuration", "pool_type", "pool"},
        resolution=1,
    ),
}


def user_demand(job_queue: JobQueue) -> list:
    """
    Log global user demand.

    :param scheduler: the scheduler
    :return: list of records for logging
    """
    result = [{"value": len(job_queue)}]
    return result


user_demand.name = "user_demand"
user_demand.whitelist = (JobQueue,)
user_demand.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1
    ),
}


def job_statistics(scheduler: CondorJobScheduler) -> list:
    """
    Log number of jobs running in all drones.

    .. Note::

        The logging is currently synchronised with the frequency of the
        scheduler. If a finer resolution is required, the update of drones
        can be considered additionally.

    :param scheduler: the scheduler
    :return: list of records for logging
    """
    result = 0
    for cluster in scheduler.drone_cluster.copy():
        for drone in cluster:
            result += drone.jobs
    return [
        {
            "pool_configuration": "None",
            "pool_type": "obs",
            "pool": repr(scheduler),
            "job_count": result,
        }
    ]


job_statistics.name = "cobald_status"
job_statistics.whitelist = (CondorJobScheduler,)
job_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool"}, resolution=1
    ),
}


def job_events(job: Job) -> list:
    """
    Log relevant events for jobs. Relevant events are

    * start of a job,
    * finishing of a job, either successful or not.

    Information about the start of a job are relevant to enable timely analysis
    of waiting times. For finishing of jobs information about the success itself,
    but also additional information on exceeded resources or refusal by the
    drone are added.

    .. Warning::

        The logging format includes the name / identifier of a job. This might
        result in a huge index of the grafana database. The job is currently
        included to enable better lookup and analysis of related events.

    :param job: the job to log information for
    :return: list of records for logging
    """
    result = {
        "pool_configuration": "None",
        "pool_type": "drone",
        "pool": repr(job.drone),
        "job": repr(job),
    }
    if job.successful is None:
        result["queue_time"] = job.queue_date
        result["waiting_time"] = job.waiting_time
    elif job.successful:
        result["wall_time"] = job.walltime
        result["success"] = 1
    else:
        result["success"] = 0
        error_logged = False
        for resource_key in job.resources:
            usage = job.used_resources.get(
                resource_key, job.resources.get(resource_key, None)
            )
            value = usage / job.resources.get(
                resource_key, job.drone.pool_resources[resource_key]
            )
            if value > 1:
                result[f"exceeded_{resource_key}"] = value
                error_logged = True
        if not error_logged:
            result["refused_by"] = repr(job.drone)
    return [result]


job_events.name = "job_event"
job_events.whitelist = (Job,)
job_events.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool", "job"}, resolution=1
    ),
}


def pool_status(pool: Pool) -> list:
    """
    Log state changes of pools and drones.

    :param simulator: the simulator
    :return: list of records for logging
    """
    return []


pool_status.name = "pool_status"
pool_status.whitelist = (Pool,)
pool_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "parent_pool", "pool_configuration", "pool_type", "pool"},
        resolution=1,
    ),
}


def configuration_information(simulator: "Simulator") -> list:
    """
    Log information how pools and drones are configured, e.g. provided resources.

    :param simulator: the simulator
    :return: list of records for logging
    """
    return []


configuration_information.name = "configuration"
configuration_information.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "resource_type"}, resolution=1
    ),
}
