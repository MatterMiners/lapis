import logging.handlers
from typing import NamedTuple, List, Dict
from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter


class UserDemand(NamedTuple):
    value: int


class DroneStatusCaching(NamedTuple):
    drone: str
    slots_tot: int
    slots_free: int
    slots_caching: int


def user_demand_tmp(user_demand: UserDemand) -> List[Dict]:
    """
    Log global user demand.

    :param scheduler: the scheduler
    :return: list of records for logging
    """
    # print("user_demand", job_queue)
    result = [{"value": user_demand.value}]
    return result


user_demand_tmp.name = "user_demand_tmp"
user_demand_tmp.whitelist = (UserDemand,)
user_demand_tmp.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1e-9
    ),
}


def drone_statistics_caching_tmp(dronestatus: DroneStatusCaching) -> List[Dict]:
    """


    :param drone: the drone
    :return: list of records for logging
    """

    results = [
        {
            "pool_type": "drone",
            "pool": dronestatus.drone,
            "claimed_slots": dronestatus.slots_tot - dronestatus.slots_free,
            "free_slots": dronestatus.slots_free,
            "slots_with_caching": dronestatus.slots_caching,
        }
    ]
    return results


drone_statistics_caching_tmp.name = "drone_status_caching_tmp"
drone_statistics_caching_tmp.whitelist = (DroneStatusCaching,)
drone_statistics_caching_tmp.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_type", "pool"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_type", "pool"}, resolution=1e-9
    ),
}
