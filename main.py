from functools import partial, wraps
import simpy
import random
import logging.handlers

from cobald.monitor.format_json import JsonFormatter

import matplotlib.pyplot as plt

import globals
from cost import cobald_cost
from job import job_demand, htcondor_export_job_generator, Job
from scheduler import CondorJobScheduler
from pool import Pool, StaticPool
from controller import SimulatedCostController


class JSONSocketHandler(logging.handlers.SocketHandler):
    def makePickle(self, record):
        return self.format(record).encode()


monitoring_logger = logging.getLogger("general")
monitoring_logger.setLevel(logging.DEBUG)
socketHandler = JSONSocketHandler(
    'localhost',
    logging.handlers.DEFAULT_TCP_LOGGING_PORT)
streamHandler = logging.StreamHandler()
socketHandler.setFormatter(JsonFormatter())
streamHandler.setFormatter(JsonFormatter())
monitoring_logger.addHandler(socketHandler)
monitoring_logger.addHandler(streamHandler)


def trace(env, callback, resource_normalisation):
    def get_wrapper(env_step, callback):
        @wraps(env_step)
        def tracing_step():
            if len(env._queue):
                t, prio, eid, event = env._queue[0]
                callback(t, prio, eid, event, resource_normalisation)
            return env_step()
        return tracing_step
    env.step = get_wrapper(env.step, callback)


last_step = 0


def monitor(data, t, prio, eid, event, resource_normalisation):
    if event.value:
        if isinstance(event.value, simpy.exceptions.Interrupt):
            job = event.value.cause
            for resource_key, usage in job.used_resources.items():
                value = usage / job.resources[resource_key]
                if value > 1:
                    monitoring_logger.info(str(round(t)), {"job_exceeds_%s" % resource_key: value})
        if isinstance(event.value, Job):
            monitoring_logger.info(str(round(t)), {"job_waiting_times": event.value.waiting_time})
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
        for pool in globals.pools:
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
        result["user_demand"] = len(globals.job_queue)
        result["pool_demand"] = pool_demand
        result["pool_supply"] = pool_supply
        result["pool_utilisation"] = pool_utilisation
        result["pool_allocation"] = pool_allocation
        result["running_jobs"] = running_jobs
        result["empty_drones"] = empty_drones
        result["used_resources"] = used_resources
        result["unused_resources"] = unused_resources
        result["available_resources"] = available_resources
        cost = cobald_cost()
        result["cost"] = cost
        globals.cost += cost
        result["acc_cost"] = globals.cost
        monitoring_logger.info(str(tmp), result)


def generate_plots():
    # Plotting some first results
    keys = globals.monitoring_data["timesteps"].keys()
    values = globals.monitoring_data["timesteps"].values()
    plt.plot(keys,
             [value.get("user_demand", None) for value in values],
             label="Accumulated demand")
    plt.plot(keys,
             [value.get("user_demand_new", None) for value in values],
             'ro',
             label="Current demand")
    plt.plot(keys,
             [value.get("pool_demand", None) for value in values],
             label="Pool demand")
    plt.plot(keys,
             [value.get("pool_supply", None) for value in values],
             label="Pool supply")
    plt.plot(keys,
             [value.get("running_jobs", None) for value in values],
             label="Running jobs")
    plt.legend()
    plt.show()
    plt.plot(keys,
             [value.get("pool_utilisation", None) for value in values],
             label="Pool utilisation")
    plt.plot(keys,
             [value.get("pool_allocation", None) for value in values],
             label="Pool allocation")
    plt.plot(keys,
             [value.get("empty_drones", None) for value in values],
             label="Unallocated drones")
    plt.legend()
    plt.show()

    for index, pool in enumerate(globals.pools):
        print("pool", index, "has", pool.resources)
        plt.plot(keys,
                 [value.get("pool_%s_supply" % pool, None) for value in values],
                 label="Pool %d supply" % index)
    plt.legend()
    plt.show()

    fig, ax1 = plt.subplots()
    ax1.plot(keys,
             [value.get("cost", None) for value in values], 'b-')
    ax1.set_xlabel('Time')
    # Make the y-axis label, ticks and tick labels match the line color.
    ax1.set_ylabel('Cost', color='b')
    ax1.tick_params('y', colors='b')

    ax2 = ax1.twinx()
    ax2.plot(keys,
             [value.get("acc_cost", None) for value in values], 'r.')
    ax2.set_ylabel('Accumulated Cost', color='r')
    ax2.tick_params('y', colors='r')

    fig.tight_layout()
    plt.show()

    # resource plot for max
    fig, ax = plt.subplots(2, sharex=True)
    ax[0].plot(keys,
               [value.get("unused_resources", None) for value in values],
               label="Unused")
    ax[0].plot(keys,
               [value.get("used_resources", None) for value in values],
               label="Used")
    ax[0].set_title("Resource utilisation")
    ax[0].legend()
    percentages = []
    percentage_means = []
    for value in values:
        try:
            percentages.append(value.get("unused_resources", 0) / value.get("available_resources", 0))
        except ZeroDivisionError:
            percentages.append(1)
        percentage_means.append(sum(percentages) / len(percentages))
    ax[1].plot(keys, percentages)
    ax[1].plot(keys, percentage_means, label="mean")
    ax[1].set_title("Percentage of unused resources")
    fig.show()

    # waiting time histogram
    plt.hist(globals.monitoring_data["job_waiting_times"], label="Job waiting times")
    plt.legend()
    plt.show()

    for resource_key in [key for key in globals.monitoring_data.keys() if
                         isinstance(key, str) and key.startswith("job_exceeds_")]:
        plt.hist(globals.monitoring_data[resource_key], label="Job exceeding %s" %
                                                              resource_key.replace("job_exceeds_", ""))
        plt.legend()
        plt.show()


def main(filename="condor_usage_sorted_filtered.csv", until=2000):
    resource_normalisation = {"memory": 2000}
    monitor_data = partial(monitor, resource_normalisation)

    random.seed(1234)
    env = simpy.Environment()
    trace(env, monitor_data, resource_normalisation=resource_normalisation)
    globals.job_generator = htcondor_export_job_generator(filename=filename,
                                                          job_queue=globals.job_queue,
                                                          env=env)
    env.process(globals.job_generator)
    for resources in [{"memory": 5000, "cores": 1}, {"memory": 24000, "cores": 8}, {"memory": 16000, "cores": 4}]:
        pool = Pool(env, resources=resources)
        globals.pools.append(pool)
        SimulatedCostController(env, target=pool, rate=1)
    globals.pools.append(StaticPool(env, init=2))
    globals.job_scheduler = CondorJobScheduler(env=env, job_queue=globals.job_queue)
    env.run(until=until)


if __name__ == "__main__":
    main()
