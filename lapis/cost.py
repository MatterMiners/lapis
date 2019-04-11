def cobald_cost(simulator):
    result = len(list(simulator.job_scheduler.drone_list))
    for drone in simulator.job_scheduler.drone_list:
        result += 1
        tmp = 0
        for resource_key in drone.pool_resources:
            tmp += drone.resources[resource_key] / drone.pool_resources[resource_key]
        tmp /= len(drone.pool_resources)
        result -= tmp
    return result


def local_cobald_cost(pool):
    result = 0
    for drone in pool.drones:
        result += 1
        tmp = 0
        for resource_key in pool.resources:
            tmp += drone.resources[resource_key] / pool.resources[resource_key]
        tmp /= len(pool.resources)
        result -= tmp
    return result
