from functools import wraps
from typing import Callable

import logging

from usim import each, Flag, time

from lapis.cost import cobald_cost
from lapis.simulator import Simulator

sampling_required = Flag()


class Monitoring(object):
    # TODO: we need to check how to integrate the normalization factor
    def __init__(self, simulator: Simulator):
        self.simulator = simulator
        self._statistics = []

    async def run(self):
        async for _ in each(delay=1):
            await sampling_required
            await sampling_required.set(False)
            result = {}
            for statistic in self._statistics:
                # do the logging
                result.update(statistic(self.simulator))
            logging.info(str(round(time.now)), result)

    def register_statistic(self, statistic: Callable):
        self._statistics.append(statistic)


def collect_resource_statistics(simulator: Simulator) -> dict:
    empty_drones = 0
    drone_resources = {}
    for drone in simulator.job_scheduler.drone_list:
        if drone.allocation == 0:
            empty_drones += 1
        for resource_key in {*drone.resources, *drone.used_resources}:
            drone_resources.setdefault(resource_key, {})
            try:
                drone_resources[resource_key]["reserved"] += drone.resources[resource_key]
            except KeyError:
                drone_resources[resource_key]["reserved"] = drone.resources[resource_key]
            try:
                drone_resources[resource_key]["used"] += drone.used_resources[resource_key]
            except KeyError:
                drone_resources[resource_key]["used"] = drone.used_resources[resource_key]
            try:
                drone_resources[resource_key]["available"] += drone.pool_resources[resource_key] - drone.resources[resource_key]
            except KeyError:
                drone_resources[resource_key]["available"] = drone.pool_resources[resource_key] - drone.resources[resource_key]
            try:
                drone_resources[resource_key]["total"] += drone.pool_resources[resource_key]
            except KeyError:
                drone_resources[resource_key]["total"] = drone.pool_resources[resource_key]
    return {
        "empty_drones": empty_drones,
        "drone_resources": drone_resources
    }


def collect_cobald_cost(simulator: Simulator) -> dict:
    current_cost = cobald_cost(simulator)
    simulator.cost += current_cost
    return {
        "cobald_cost": {
            "current": current_cost,
            "accumulated": simulator.cost
        }
    }


def collect_user_demand(simulator: Simulator) -> dict:
    return {
        "user_demand": len(simulator.job_scheduler.job_queue)
    }


def collect_job_statistics(simulator: Simulator) -> dict:
    result = 0
    for drone in simulator.job_scheduler.drone_list:
        result += drone.jobs
    return {
        "running_jobs": result
    }


def collect_pool_statistics(simulator: Simulator) -> dict:
    pool_demand = {}
    pool_supply = {}
    pool_utilisation = {}
    pool_allocation = {}
    for pool in simulator.pools:
        pool_demand[repr(pool)] = pool.demand
        pool_supply[repr(pool)] = pool.supply
        pool_utilisation[repr(pool)] = pool.utilisation
        pool_allocation[repr(pool)] = pool.allocation
    return {
        "pool": {
            "demand": pool_demand,
            "supply": pool_supply,
            "allocation": pool_allocation,
            "utilisation": pool_utilisation
        }
    }
