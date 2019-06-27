import copy
from typing import Callable, TYPE_CHECKING

import logging

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter
from usim import each, Flag, time

from lapis.cost import cobald_cost

if TYPE_CHECKING:
    from lapis.simulator import Simulator

sampling_required = Flag()


class LoggingSocketHandler(logging.handlers.SocketHandler):
    def makePickle(self, record):
        return self.format(record).encode()


class LoggingUDPSocketHandler(logging.handlers.DatagramHandler):
    def makePickle(self, record):
        return self.format(record).encode()


class TimeFilter(logging.Filter):
    """
    py:class:`TimeFilter` takes care to modify the created timestamp of a log
    record to be set to the current simulation time.
    """
    def filter(self, record) -> bool:
        record.created = time.now
        return True


class Monitoring(object):
    def __init__(self, simulator: "Simulator"):
        self.simulator = simulator
        self._statistics = []

    async def run(self):
        async for _ in each(delay=1):
            await sampling_required
            await sampling_required.set(False)
            for statistic in self._statistics:
                # do the logging
                for record in statistic(self.simulator):
                    logging.getLogger(statistic.name).info(
                        statistic.name, record
                    )

    def register_statistic(self, statistic: Callable):
        assert hasattr(statistic, "name") and hasattr(statistic, "logging_formatter")
        self._statistics.append(statistic)

        # prepare the logger
        logger = logging.getLogger(statistic.name)
        if len(logger.handlers) == 0:
            # append handlers of default logger and add required formatters
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                new_handler = copy.copy(handler)
                new_handler.setFormatter(statistic.logging_formatter.get(
                    type(handler).__name__, JsonFormatter()))


def collect_resource_statistics(simulator: "Simulator") -> list:
    results = []
    for drone in simulator.job_scheduler.drone_list:
        for resource_type in {*drone.resources, *drone.used_resources}:
            results.append({
                "resource_type": resource_type,
                "pool_configuration": None,
                "pool_type": "drone",
                "pool": repr(drone),
                "used_ratio": drone.used_resources.get(resource_type, 0) /
                              drone.resources.get(resource_type, 0)
            })
    return results


collect_resource_statistics.logging_formatter = {
    LoggingSocketHandler.__class__.__name__: JsonFormatter(),
    logging.StreamHandler.__class__.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__class__.__name__: LineProtocolFormatter(
        tags={"tardis", "resource_type", "pool_configuration", "pool_type"},
        resolution=1
    )
}
collect_resource_statistics.name = "resource_status"


def collect_cobald_cost(simulator: "Simulator") -> dict:
    current_cost = cobald_cost(simulator)
    simulator.cost += current_cost
    return {
        "cobald_cost": {
            "current": current_cost,
            "accumulated": simulator.cost
        }
    }


def collect_user_demand(simulator: "Simulator") -> list:
    return [{
        "value": len(simulator.job_scheduler.job_queue)
    }]


collect_user_demand.logging_formatter = {
    LoggingSocketHandler.__class__.__name__: JsonFormatter(),
    logging.StreamHandler.__class__.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__class__.__name__: LineProtocolFormatter(
        resolution=1
    )
}
collect_user_demand.name = "user_demand"


def collect_job_statistics(simulator: "Simulator") -> list:
    result = 0
    for drone in simulator.job_scheduler.drone_list:
        result += drone.jobs
    return [{
        "job_count": result
    }]


collect_job_statistics.logging_formatter = {
    LoggingSocketHandler.__class__.__name__: JsonFormatter(),
    logging.StreamHandler.__class__.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__class__.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type"},
        resolution=1
    )
}
collect_job_statistics.name = "cobald_status"


def collect_drone_cobald_statistics(simulator: "Simulator") -> list:
    results = []
    for drone in simulator.job_scheduler.drone_list:
        results.append({
            "pool_configuration": None,
            "pool_type": "drone",
            "pool": repr(drone),
            "allocation": drone.allocation,
            "utilisation": drone.utilisation,
            "demand": drone.demand,
            "supply": drone.supply,
            "job_count": drone.jobs
        })
    return results


collect_drone_cobald_statistics.logging_formatter = {
    LoggingSocketHandler.__class__.__name__: JsonFormatter(),
    logging.StreamHandler.__class__.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__class__.__name__: LineProtocolFormatter(
        tags={"tardis", "resource_type", "pool_configuration", "pool_type"},
        resolution=1
    )
}
collect_drone_cobald_statistics.name = "cobald_status"


def collect_pool_cobald_statistics(simulator: "Simulator") -> list:
    results = []
    for pool in simulator.pools:
        results.append({
            "pool_configuration": None,
            "pool_type": "pool",
            "pool": repr(pool),
            "allocation": pool.allocation,
            "utilisation": pool.utilisation,
            "demand": pool.demand,
            "supply": pool.supply,
        })
    return results


collect_pool_cobald_statistics.logging_formatter = {
    LoggingSocketHandler.__class__.__name__: JsonFormatter(),
    logging.StreamHandler.__class__.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__class__.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type"},
        resolution=1
    )
}
collect_pool_cobald_statistics.name = "cobald_status"

