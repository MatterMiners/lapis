def cobald_cost(simulator):
    result = len(simulator.job_queue)
    for pool in simulator.pools:
        for drone in pool.drones:
            result += 1
            tmp = 0
            for resource_key in pool.resources:
                tmp += drone.resources[resource_key] / pool.resources[resource_key]
            tmp /= len(pool.resources)
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
