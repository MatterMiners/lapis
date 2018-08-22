from functools import partial, wraps
import simpy
import random

import matplotlib.pyplot as plt

import globals
from cost import cobald_cost
from job import job_demand, job_property_generator
from scheduler import job_scheduler, htcondor_job_scheduler
from pool import Pool
from controller import SimulatedLinearController


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
        empty_drones = 0
        result = {}
        for pool in globals.pools:
            pool_demand += pool.demand
            pool_supply += pool.supply
            result["pool_%s_supply" % pool] = pool.supply
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
        result["user_demand"] = globals.global_demand.level
        result["pool_demand"] = pool_demand
        result["pool_supply"] = pool_supply
        result["pool_utilisation"] = pool_utilisation
        result["pool_allocation"] = pool_allocation
        result["running_jobs"] = running_jobs
        result["empty_drones"] = empty_drones
        result["used_resources"] = used_resources
        result["unused_resources"] = unused_resources
        cost = cobald_cost()
        result["cost"] = cost
        globals.cost += cost
        result["acc_cost"] = globals.cost
        globals.monitoring_data[tmp].update(result)
        #     print("%s [Pool %s] drones %d, demand %d, supply %d (%d); allocation %.2f, utilisation %.2f" % (
        #         tmp, pool, len(pool.drones), pool.demand, pool.supply, pool.level, pool.allocation, pool.utilisation))


def main():
    monitor_data = partial(monitor, globals.monitoring_data)

    random.seed(1234)
    env = simpy.Environment()
    trace(env, monitor_data, resource_normalisation={"memory": 2000})
    #globals.job_generator = job_property_generator()
    globals.job_generator = htcondor_export_job_generator("condor_usage.csv")
    for resources in [{"memory": 5000, "cores": 1}, {"memory": 24000, "cores": 8}, {"memory": 16000, "cores": 4}]:
        pool = Pool(env, resources=resources)
        globals.pools.append(pool)
        SimulatedCostController(env, target=pool, rate=1)
    globals.global_demand = simpy.Container(env)
    env.process(job_demand(env))
    env.process(htcondor_job_scheduler(env))
    env.run(until=1000)

    # Plotting some first results
    plt.plot(globals.monitoring_data.keys(),
             [value.get("user_demand", None) for value in globals.monitoring_data.values()],
             label="Accumulated demand")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("user_demand_new", None) for value in globals.monitoring_data.values()],
             'ro',
             label="Current demand")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("pool_demand", None) for value in globals.monitoring_data.values()],
             label="Pool demand")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("pool_supply", None) for value in globals.monitoring_data.values()],
             label="Pool supply")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("running_jobs", None) for value in globals.monitoring_data.values()],
             label="Running jobs")
    plt.legend()
    plt.show()
    plt.plot(globals.monitoring_data.keys(),
             [value.get("pool_utilisation", None) for value in globals.monitoring_data.values()],
             label="Pool utilisation")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("pool_allocation", None) for value in globals.monitoring_data.values()],
             label="Pool allocation")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("empty_drones", None) for value in globals.monitoring_data.values()],
             label="Unallocated drones")
    plt.legend()
    plt.show()

    for index, pool in enumerate(globals.pools):
        plt.plot(globals.monitoring_data.keys(),
                 [value.get("pool_%s_supply" % pool, None) for value in globals.monitoring_data.values()],
                 label="Pool %d supply" % index)
    plt.legend()
    plt.show()

    fig, ax1 = plt.subplots()
    ax1.plot(globals.monitoring_data.keys(),
         [value.get("cost", None) for value in globals.monitoring_data.values()], 'b-')
    ax1.set_xlabel('Time')
    # Make the y-axis label, ticks and tick labels match the line color.
    ax1.set_ylabel('Cost', color='b')
    ax1.tick_params('y', colors='b')

    ax2 = ax1.twinx()
    ax2.plot(globals.monitoring_data.keys(),
         [value.get("acc_cost", None) for value in globals.monitoring_data.values()], 'r.')
    ax2.set_ylabel('Accumulated Cost', color='r')
    ax2.tick_params('y', colors='r')

    fig.tight_layout()
    plt.show()

    # resource plot for max
    plt.plot(globals.monitoring_data.keys(),
             [value.get("unused_resources", None) for value in globals.monitoring_data.values()],
             label="Unused")
    plt.plot(globals.monitoring_data.keys(),
             [value.get("used_resources", None) for value in globals.monitoring_data.values()],
             label="used")
    plt.legend()
    plt.show()
    print("final cost: %.2f" % globals.monitoring_data[sorted(globals.monitoring_data.keys())[-1]]["acc_cost"])


if __name__ == "__main__":
    main()
