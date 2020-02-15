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

from lapis.scheduler import CondorClassadJobScheduler
from lapis.simulator import Simulator


from lapis.monitor import LoggingUDPSocketHandler, SimulationTimeFilter

from time import time

machine_ad_defaults = """
    requirements = target.requestcpus <= my.cpus
    rank = 0
    """.strip()

job_ad_defaults = """
requirements = my.requestcpus <= target.cpus && my.requestmemory <= target.memory
rank = 1
"""
pre_job_rank_defaults = "0"

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
    log_file="test_{}.log".format(time()),
    remote_throughput=1.0,
    seed=1234,
    until=None,
    calculation_efficiency=1.0,
    log_telegraf=False,
    pre_job_rank=pre_job_rank_defaults,
    machine_ads=machine_ad_defaults,
    job_ads=job_ad_defaults,
):
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
    simulator.create_job_generator(
        job_input=open(file, "r"),
        job_reader=partial(
            job_import_mapper[file_type], calculation_efficiency=calculation_efficiency
        ),
    )

    simulator.job_scheduler = CondorClassadJobScheduler(
        job_queue=simulator.job_queue,
        pre_job_rank=pre_job_rank,
        machine_ad=machine_ads,
        job_ad=job_ads,
    )

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


# job_file = "/home/tabea/work/testdata/hitratebased/job_list_minimal.json"
# job_file = "/home/tabea/work/testdata/hitratebased/job_list_minimal_only_cpu.json"
job_file = "/home/tabea/work/testdata/fullsim/test_24h_jobinput.json"
# job_file = "/home/tabea/work/testdata/fullsim/resampled_reduced_025week_16_jobinput" \
#            ".json"
# pool_files = ["/home/tabea/work/testdata/hitratebased/sg_machines.csv",
#              "/home/tabea/work/testdata/hitratebased/dummycluster.csv"]
# storage_file = "/home/tabea/work/testdata/hitratebased/sg_caches.csv"
# storage_type = "filehitrate"
#
# ini_and_run(job_file=job_file, pool_files=pool_files, storage_file=storage_file,
#             storage_type=storage_type, log_file="minimal_hitratebased_test.log",
#             log_telegraf=True)

# job_file = "/home/tabea/work/testdata/hitratebased/testjobs.json"
# job_file = "/home/tabea/work/testdata/hitratebased/week.json"
# job_file = "/home/tabea/work/testdata/hitratebased/day_jobinput.json"
# job_file = "/home/tabea/work/testdata/hitratebased/week_1_sample_time_jobinput.json"
pool_files = [
    "/home/tabea/work/testdata/fullsim/sg_machines_shared_cache.csv",
    "/home/tabea/work/testdata/fullsim/dummycluster.csv",
]
storage_file = "/home/tabea/work/testdata/fullsim/sg_caches_shared.csv"
storage_type = "filehitrate"
ini_and_run(
    job_file=job_file,
    pool_files=pool_files,
    storage_file=storage_file,
    storage_type=storage_type,
    log_file="test_new_scheduler.log",
    log_telegraf=True,
    # pre_job_rank="100000 * my.cpus + my.memory - 1000000 - 10000000 * my.rank "
    pre_job_rank="1",
)
