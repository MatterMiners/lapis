from functools import wraps

import simpy
import logging

from cobald_sim.cost import cobald_cost
from cobald_sim.job import Job

last_step = 0


def trace(env, callback, resource_normalisation, simulator):
    def get_wrapper(env_step, callback):
        @wraps(env_step)
        def tracing_step():
            if len(env._queue):
                t, prio, eid, event = env._queue[0]
                callback(t, prio, eid, event, resource_normalisation, simulator)
            return env_step()
        return tracing_step
    env.step = get_wrapper(env.step, callback)


def monitor(data, t, prio, eid, event, resource_normalisation, simulator):
    if event.value:
        if isinstance(event.value, simpy.exceptions.Interrupt):
            job = event.value.cause
            for resource_key, usage in job.used_resources.items():
                value = usage / job.resources[resource_key]
                if value > 1:
                    logging.info(str(round(t)), {"job_exceeds_%s" % resource_key: value})
        if isinstance(event.value, Job):
            logging.info(str(round(t)), {"job_waiting_times": event.value.waiting_time})
    global last_step
    if t > last_step:
        # new data to be recorded
        tmp = round(t)
        last_step = tmp
        pool_demand = 0
        pool_supply = 0
        pool_utilisation = 0
        pool_allocation = 0
        running_jobs = 0
        used_resources = 0
        unused_resources = 0
        available_resources = 0
        empty_drones = 0
        result = {}
        for pool in simulator.pools:
            pool_demand += pool.demand
            pool_supply += pool.supply
            result["pool_%s_supply" % id(pool)] = pool.supply
            pool_utilisation += pool.utilisation
            pool_allocation += pool.allocation
            for drone in pool.drones:
                running_jobs += drone.jobs
                if drone.allocation == 0:
                    empty_drones += 1
                for resource_key, usage in drone.resources.items():
                    normalisation_factor = resource_normalisation.get(resource_key, 1)
                    used_resources += usage / normalisation_factor
                    unused_resources += (pool.resources[resource_key] - usage) / normalisation_factor
                    available_resources += pool.resources[resource_key] / normalisation_factor
        result["user_demand"] = len(simulator.job_queue)
        result["pool_demand"] = pool_demand
        result["pool_supply"] = pool_supply
        result["pool_utilisation"] = pool_utilisation
        result["pool_allocation"] = pool_allocation
        result["running_jobs"] = running_jobs
        result["empty_drones"] = empty_drones
        result["used_resources"] = used_resources
        result["unused_resources"] = unused_resources
        result["available_resources"] = available_resources
        current_cost = cobald_cost(simulator)
        result["cost"] = current_cost
        simulator.cost += current_cost
        result["acc_cost"] = simulator.cost
        logging.info(str(tmp), result)
