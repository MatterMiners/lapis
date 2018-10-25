import random
from functools import partial, wraps

import click
import simpy
import logging.handlers

from cobald.monitor.format_json import JsonFormatter

from cobald_sim.controller import SimulatedCostController
from cobald_sim.cost import cobald_cost
from cobald_sim.job import job_to_queue_scheduler
from cobald_sim.job_io.htcondor import htcondor_job_reader
from cobald_sim.pool import StaticPool, Pool
from cobald_sim.pool_io.htcondor import htcondor_pool_reader
from cobald_sim.job_io.swf import swf_job_reader

from cobald_sim import globals
from cobald_sim.scheduler import CondorJobScheduler


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

last_step = 0

job_import_mapper = {
    "htcondor": htcondor_job_reader,
    "swf": swf_job_reader
}

pool_import_mapper = {
    "htcondor": htcondor_pool_reader
}


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
        current_cost = cobald_cost()
        result["cost"] = current_cost
        globals.cost += current_cost
        result["acc_cost"] = globals.cost
        monitoring_logger.info(str(tmp), result)


@click.group()
@click.option("--seed", type=int, default=1234)
@click.option("--until", default=2000)
@click.pass_context
def cli(ctx, seed, until):
    ctx.ensure_object(dict)
    ctx.obj['seed'] = seed
    ctx.obj['until'] = until


@cli.command()
@click.option("--job_file", type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))))
@click.option("--pool_file", type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))), multiple=True)
@click.pass_context
def static(ctx, job_file, pool_file):
    click.echo("starting static environment")
    random.seed(ctx.obj["seed"])
    resource_normalisation = {"memory": 2000}
    monitor_data = partial(monitor, resource_normalisation)

    env = simpy.Environment()
    trace(env, monitor_data, resource_normalisation=resource_normalisation)
    file, file_type = job_file
    globals.job_generator = job_to_queue_scheduler(job_generator=job_import_mapper[file_type](env, file),
                                                   job_queue=globals.job_queue,
                                                   env=env)
    for current_pool in pool_file:
        file, file_type = current_pool
        for pool in pool_import_mapper[file_type](env=env, iterable=file, pool_type=StaticPool):
            globals.pools.append(pool)
    env.process(globals.job_generator)
    globals.job_scheduler = CondorJobScheduler(env=env, job_queue=globals.job_queue)
    env.run(until=ctx.obj["until"])


@cli.command()
@click.option("--job_file", type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))))
@click.option("--pool_file", type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))), multiple=True)
@click.pass_context
def dynamic(ctx, job_file, pool_file):
    click.echo("starting dynamic environment")
    random.seed(ctx.obj["seed"])
    resource_normalisation = {"memory": 2000}
    monitor_data = partial(monitor, resource_normalisation)

    env = simpy.Environment()
    trace(env, monitor_data, resource_normalisation=resource_normalisation)
    file, file_type = job_file
    globals.job_generator = job_to_queue_scheduler(job_generator=job_import_mapper[file_type](env, file),
                                                   job_queue=globals.job_queue,
                                                   env=env)
    for current_pool in pool_file:
        file, file_type = current_pool
        for pool in pool_import_mapper[file_type](env=env, iterable=file, pool_type=Pool):
            globals.pools.append(pool)
            SimulatedCostController(env, target=pool, rate=1)
    env.process(globals.job_generator)
    globals.job_scheduler = CondorJobScheduler(env=env, job_queue=globals.job_queue)
    env.run(until=ctx.obj["until"])


@cli.command()
@click.option("--job_file", type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))))
@click.option("--static_pool_file", type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))), multiple=True)
@click.option("--dynamic_pool_file", type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))), multiple=True)
@click.pass_context
def hybrid(ctx, job_file, static_pool_file, dynamic_pool_file):
    click.echo("starting hybrid environment")
    random.seed(ctx.obj["seed"])
    resource_normalisation = {"memory": 2000}
    monitor_data = partial(monitor, resource_normalisation)

    env = simpy.Environment()
    trace(env, monitor_data, resource_normalisation=resource_normalisation)
    file, file_type = job_file
    globals.job_generator = job_to_queue_scheduler(job_generator=job_import_mapper[file_type](env, file),
                                                   job_queue=globals.job_queue,
                                                   env=env)
    for current_pool in static_pool_file:
        file, file_type = current_pool
        for pool in pool_import_mapper[file_type](env=env, iterable=file, pool_type=StaticPool):
            globals.pools.append(pool)
    for current_pool in dynamic_pool_file:
        file, file_type = current_pool
        for pool in pool_import_mapper[file_type](env=env, iterable=file, pool_type=Pool):
            globals.pools.append(pool)
            SimulatedCostController(env, target=pool, rate=1)
    env.process(globals.job_generator)
    globals.job_scheduler = CondorJobScheduler(env=env, job_queue=globals.job_queue)
    env.run(until=ctx.obj["until"])


if __name__ == '__main__':
    cli()
