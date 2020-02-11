from functools import partial

import logging.handlers

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.job_io.htcondor import htcondor_job_reader
from lapis.pool import StaticPool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.job_io.swf import swf_job_reader
from lapis.storageelement import FileBasedHitrateStorage
from lapis.storage_io.storage import (
    storage_reader,
    storage_reader_filebased_hitrate_caching,
)

from lapis.scheduler import CondorJobScheduler
from lapis.simulator import Simulator

import sys

from lapis.monitor import LoggingUDPSocketHandler, SimulationTimeFilter


last_step = 0

job_import_mapper = {"htcondor": htcondor_job_reader, "swf": swf_job_reader}

pool_import_mapper = {"htcondor": htcondor_pool_reader}

storage_import_mapper = {
    "standard": storage_reader,
    "filehitrate": storage_reader_filebased_hitrate_caching,
}


def ini_and_run(
    job_file,
    pool_files,
    storage_file,
    storage_type,
    log_file="test.log",
    remote_throughput=1.0,
    seed=1234,
    until=None,
    calculation_efficiency=1.0,
    log_telegraf=False,
):
    logging.getLogger("implementation").info(
        job_file, pool_files, storage_file, log_file
    )
    # ini logging to file
    monitoring_logger = logging.getLogger()
    monitoring_logger.setLevel(logging.DEBUG)
    time_filter = SimulationTimeFilter()
    monitoring_logger.addFilter(time_filter)
    streamHandler = logging.StreamHandler(stream=open(log_file, "w"))
    streamHandler.setFormatter(JsonFormatter())
    monitoring_logger.addHandler(streamHandler)

    if log_telegraf:
        telegrafHandler = LoggingUDPSocketHandler(
            "localhost", logging.handlers.DEFAULT_UDP_LOGGING_PORT
        )
        telegrafHandler.setFormatter(LineProtocolFormatter(resolution=1))
        monitoring_logger.addHandler(telegrafHandler)

    # ini simulation
    print("starting static environment")
    simulator = Simulator(seed=seed)
    file_type = "htcondor"
    file = job_file
    # print()
    # input()
    simulator.create_job_generator(
        job_input=open(file, "r"),
        job_reader=partial(
            job_import_mapper[file_type], calculation_efficiency=calculation_efficiency
        ),
    )
    simulator.create_scheduler(scheduler_type=CondorJobScheduler)

    simulator.create_connection_module(remote_throughput * 1024 * 1024 * 1024)
    with open(storage_file, "r") as storage_file:
        simulator.create_storage(
            storage_input=storage_file,
            storage_content_input=None,
            storage_reader=storage_import_mapper[storage_type],
            storage_type=FileBasedHitrateStorage,
        )

    for pool_file in pool_files:
        with open(pool_file, "r") as pool_file:
            pool_file_type = "htcondor"
            simulator.create_pools(
                pool_input=pool_file,
                pool_reader=pool_import_mapper[pool_file_type],
                pool_type=StaticPool,
            )
    simulator.enable_monitoring()

    # run simulation
    simulator.run(until=until)


ini_and_run(
    job_file=sys.argv[1],
    pool_files=[sys.argv[2], sys.argv[3]],
    storage_file=sys.argv[4],
    storage_type="filehitrate",
    log_file=sys.argv[5],
    remote_throughput=sys.argv[6],
    calculation_efficiency=sys.argv[7],
    log_telegraf=False,
)
