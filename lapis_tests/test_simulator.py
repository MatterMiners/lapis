from tempfile import NamedTemporaryFile

from lapis.job_io.htcondor import htcondor_job_reader
from lapis.pool import StaticPool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.scheduler import CondorJobScheduler
from lapis.simulator import Simulator


class TestSimulator(object):
    def test_simulation_exit(self):
        simulator = Simulator()
        with NamedTemporaryFile(suffix=".csv") as machine_config, NamedTemporaryFile(
            suffix=".csv"
        ) as job_config:
            with open(machine_config.name, "w") as write_stream:
                write_stream.write(
                    "TotalSlotCPUs TotalSlotDisk TotalSlotMemory Count\n"
                    "1 44624348.0 8000 1"
                )
            with open(job_config.name, "w") as write_stream:
                write_stream.write(
                    "QDate RequestCpus RequestWalltime RequestMemory RequestDisk "
                    "RemoteWallClockTime MemoryUsage DiskUsage_RAW RemoteSysCpu "
                    "RemoteUserCpu\n"
                    "1567155456 1 60 2000 6000000 100.0 2867 41898 10.0 40.0"
                )
            job_input = open(job_config.name, "r+")
            machine_input = open(machine_config.name, "r+")
            simulator.create_job_generator(
                job_input=job_input, job_reader=htcondor_job_reader
            )
            simulator.create_scheduler(scheduler_type=CondorJobScheduler)
            simulator.create_pools(
                pool_input=machine_input,
                pool_reader=htcondor_pool_reader,
                pool_type=StaticPool,
            )
            simulator.run()
            assert 180 == simulator.duration
