import click
import logging.handlers

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.controller import SimulatedLinearController
from lapis.job_io.htcondor import htcondor_job_reader
from lapis.pool import StaticPool, Pool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.job_io.swf import swf_job_reader

from lapis.scheduler import CondorJobScheduler
from lapis.simulator import Simulator

from lapis.monitor import (
    LoggingSocketHandler,
    LoggingUDPSocketHandler,
    SimulationTimeFilter,
)

last_step = 0

job_import_mapper = {"htcondor": htcondor_job_reader, "swf": swf_job_reader}

pool_import_mapper = {"htcondor": htcondor_pool_reader}


@click.group()
@click.option("--seed", type=int, default=1234)
@click.option("--until", type=float)
@click.option("--log-tcp", "log_tcp", is_flag=True)
@click.option("--log-file", "log_file", type=click.File("w"))
@click.option("--log-telegraf", "log_telegraf", is_flag=True)
@click.pass_context
def cli(ctx, seed, until, log_tcp, log_file, log_telegraf):
    ctx.ensure_object(dict)
    ctx.obj["seed"] = seed
    ctx.obj["until"] = until
    monitoring_logger = logging.getLogger()
    monitoring_logger.setLevel(logging.DEBUG)
    time_filter = SimulationTimeFilter()
    monitoring_logger.addFilter(time_filter)
    if log_tcp:
        socketHandler = LoggingSocketHandler(
            "localhost", logging.handlers.DEFAULT_TCP_LOGGING_PORT
        )
        socketHandler.setFormatter(JsonFormatter())
        monitoring_logger.addHandler(socketHandler)
    if log_file:
        streamHandler = logging.StreamHandler(stream=log_file)
        streamHandler.setFormatter(JsonFormatter())
        monitoring_logger.addHandler(streamHandler)
    if log_telegraf:
        telegrafHandler = LoggingUDPSocketHandler(
            "localhost", logging.handlers.DEFAULT_UDP_LOGGING_PORT
        )
        telegrafHandler.setFormatter(LineProtocolFormatter(resolution=1))
        monitoring_logger.addHandler(telegrafHandler)


@cli.command()
@click.option(
    "--job-file",
    "job_file",
    type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))),
)
@click.option(
    "--pool-file",
    "pool_file",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
)
@click.pass_context
def static(ctx, job_file, pool_file):
    click.echo("starting static environment")
    simulator = Simulator(seed=ctx.obj["seed"])
    file, file_type = job_file
    simulator.create_job_generator(
        job_input=file, job_reader=job_import_mapper[file_type]
    )
    simulator.create_scheduler(scheduler_type=CondorJobScheduler)
    for current_pool in pool_file:
        pool_file, pool_file_type = current_pool
        simulator.create_pools(
            pool_input=pool_file,
            pool_reader=pool_import_mapper[pool_file_type],
            pool_type=StaticPool,
        )
    simulator.run(until=ctx.obj["until"])


@cli.command()
@click.option(
    "--job-file",
    "job_file",
    type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))),
)
@click.option(
    "--pool-file",
    "pool_file",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
)
@click.pass_context
def dynamic(ctx, job_file, pool_file):
    click.echo("starting dynamic environment")
    simulator = Simulator(seed=ctx.obj["seed"])
    file, file_type = job_file
    simulator.create_job_generator(
        job_input=file, job_reader=job_import_mapper[file_type]
    )
    simulator.create_scheduler(scheduler_type=CondorJobScheduler)
    for current_pool in pool_file:
        file, file_type = current_pool
        simulator.create_pools(
            pool_input=file,
            pool_reader=pool_import_mapper[file_type],
            pool_type=Pool,
            controller=SimulatedLinearController,
        )
    simulator.run(until=ctx.obj["until"])


@cli.command()
@click.option(
    "--job-file",
    "job_file",
    type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))),
)
@click.option(
    "--static-pool-file",
    "static_pool_file",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
)
@click.option(
    "--dynamic-pool-file",
    "dynamic_pool_file",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
)
@click.pass_context
def hybrid(ctx, job_file, static_pool_file, dynamic_pool_file):
    click.echo("starting hybrid environment")
    simulator = Simulator(seed=ctx.obj["seed"])
    file, file_type = job_file
    simulator.create_job_generator(
        job_input=file, job_reader=job_import_mapper[file_type]
    )
    simulator.create_scheduler(scheduler_type=CondorJobScheduler)
    for current_pool in static_pool_file:
        file, file_type = current_pool
        simulator.create_pools(
            pool_input=file,
            pool_reader=pool_import_mapper[file_type],
            pool_type=StaticPool,
        )
    for current_pool in dynamic_pool_file:
        file, file_type = current_pool
        simulator.create_pools(
            pool_input=file,
            pool_reader=pool_import_mapper[file_type],
            pool_type=Pool,
            controller=SimulatedLinearController,
        )
    simulator.run(until=ctx.obj["until"])


if __name__ == "__main__":
    cli()
