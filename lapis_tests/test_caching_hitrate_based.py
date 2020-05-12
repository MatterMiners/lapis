from usim import time
from tempfile import NamedTemporaryFile
import json
from functools import partial

from lapis_tests import via_usim, DummyDrone
from lapis.connection import Connection
from lapis.storageelement import HitrateStorage
from lapis.storage_io.storage import storage_reader
from lapis.files import RequestedFile
from lapis.simulator import Simulator
from lapis.job_io.htcondor import htcondor_job_reader
from lapis.pool import StaticPool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.scheduler import CondorJobScheduler


class TestHitrateCaching(object):
    def test_hitratestorage(self):
        size = 1000
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        requested_file = RequestedFile(filename="testfile", filesize=100)
        looked_up_file = hitratestorage.find(requested_file, job_repr=None)

        assert size == hitratestorage.available
        assert 0 == hitratestorage.used
        assert 100 == looked_up_file.cached_filesize
        assert hitratestorage == looked_up_file.storage

    @via_usim
    async def test_add_storage_to_connection(self):
        throughput = 10
        size = 1000
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        connection = Connection(throughput=throughput)
        connection.add_storage_element(hitratestorage)
        assert hitratestorage in connection.storages[hitratestorage.sitename]

    @via_usim
    async def test_determine_inputfile_source(self):
        throughput = 10
        size = 1000
        requested_file = RequestedFile(filename="testfile", filesize=100)
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        connection = Connection(throughput=throughput)
        connection.add_storage_element(hitratestorage)
        cache = await connection._determine_inputfile_source(
            requested_file=requested_file, dronesite=None
        )
        assert cache is hitratestorage

    @via_usim
    async def test_stream_file(self):
        throughput = 10
        size = 1000
        requested_file = RequestedFile(filename="testfile", filesize=100)
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        connection = Connection(throughput=throughput)
        connection.add_storage_element(hitratestorage)
        assert 0 == time.now
        await connection.stream_file(requested_file=requested_file, dronesite=None)
        assert 5 == time.now

    @via_usim
    async def test_single_transfer_files(self):
        throughput = 10
        size = 1000
        drone = DummyDrone(throughput)
        requested_files = dict(test=dict(usedsize=100))
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        drone.connection.add_storage_element(hitratestorage)
        stream_time = await drone.connection.transfer_files(
            drone=drone, requested_files=requested_files, job_repr="test"
        )

        assert time.now == 5
        assert stream_time == 5

    @via_usim
    async def test_simultaneous_transfer(self):
        throughput = 10
        size = 1000
        drone = DummyDrone(throughput)
        requested_files = dict(test1=dict(usedsize=100), test2=dict(usedsize=200))
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        drone.connection.add_storage_element(hitratestorage)
        stream_time = await drone.connection.transfer_files(
            drone=drone, requested_files=requested_files
        )
        assert time.now == 15
        assert stream_time == 15

    @via_usim
    async def test_caching_simulation_duration_short_jobs(self):
        simulator = Simulator()
        with NamedTemporaryFile(suffix=".csv") as machine_config, NamedTemporaryFile(
            suffix=".csv"
        ) as storage_config, NamedTemporaryFile(suffix=".json") as job_config:
            with open(machine_config.name, "w") as write_stream:
                write_stream.write(
                    "TotalSlotCPUs TotalSlotDisk TotalSlotMemory Count sitename\n"
                    "1 44624348.0 8000 1 site1"
                )
            with open(job_config.name, "w") as write_stream:
                job_description = [
                    {
                        "QDate": 0,
                        "RequestCpus": 1,
                        "RequestWalltime": 60,
                        "RequestMemory": 1024,
                        "RequestDisk": 1024,
                        "RemoteWallClockTime": 1.0,
                        "MemoryUsage": 1024,
                        "DiskUsage_RAW": 1024,
                        "RemoteSysCpu": 1.0,
                        "RemoteUserCpu": 0.0,
                        "Inputfiles": dict(
                            file1=dict(usedsize=10), file2=dict(usedsize=5)
                        ),
                    }
                ] * 2
                json.dump(job_description, write_stream)
            with open(storage_config.name, "w") as write_stream:
                write_stream.write(
                    "name sitename cachesizeGB throughput_limit\n"
                    "cache1 site1 1000 1.0"
                )

            job_input = open(job_config.name, "r+")
            machine_input = open(machine_config.name, "r+")
            storage_input = open(storage_config.name, "r+")
            storage_content_input = None
            cache_hitrate = 0.5
            simulator.create_job_generator(
                job_input=job_input, job_reader=htcondor_job_reader
            )
            simulator.create_scheduler(scheduler_type=CondorJobScheduler)
            simulator.create_connection_module(remote_throughput=1.0)
            simulator.create_pools(
                pool_input=machine_input,
                pool_reader=htcondor_pool_reader,
                pool_type=StaticPool,
            )
            simulator.create_storage(
                storage_input=storage_input,
                storage_content_input=storage_content_input,
                storage_reader=storage_reader,
                storage_type=partial(HitrateStorage, cache_hitrate),
            )
            simulator.run()
            assert 180 == simulator.duration

    @via_usim
    async def test_caching_simulation_duration_long_jobs(self):
        simulator = Simulator()
        with NamedTemporaryFile(suffix=".csv") as machine_config, NamedTemporaryFile(
            suffix=".csv"
        ) as storage_config, NamedTemporaryFile(suffix=".json") as job_config:
            with open(machine_config.name, "w") as write_stream:
                write_stream.write(
                    "TotalSlotCPUs TotalSlotDisk TotalSlotMemory Count sitename\n"
                    "1 44624348.0 8000 1 site1"
                )
            with open(job_config.name, "w") as write_stream:
                job_description = [
                    {
                        "QDate": 0,
                        "RequestCpus": 1,
                        "RequestWalltime": 60,
                        "RequestMemory": 1024,
                        "RequestDisk": 1024,
                        "RemoteWallClockTime": 1.0,
                        "MemoryUsage": 1024,
                        "DiskUsage_RAW": 1024,
                        "RemoteSysCpu": 1.0,
                        "RemoteUserCpu": 0.0,
                        "Inputfiles": dict(
                            file1=dict(usedsize=60), file2=dict(usedsize=60)
                        ),
                    }
                ] * 2
                json.dump(job_description, write_stream)
            with open(storage_config.name, "w") as write_stream:
                write_stream.write(
                    "name sitename cachesizeGB throughput_limit\n"
                    "cache1 site1 1000 1.0"
                )

            job_input = open(job_config.name, "r+")
            machine_input = open(machine_config.name, "r+")
            storage_input = open(storage_config.name, "r+")
            storage_content_input = None
            cache_hitrate = 0.5
            simulator.create_job_generator(
                job_input=job_input, job_reader=htcondor_job_reader
            )
            simulator.create_scheduler(scheduler_type=CondorJobScheduler)
            simulator.create_connection_module(remote_throughput=1.0)
            simulator.create_pools(
                pool_input=machine_input,
                pool_reader=htcondor_pool_reader,
                pool_type=StaticPool,
            )
            simulator.create_storage(
                storage_input=storage_input,
                storage_content_input=storage_content_input,
                storage_reader=storage_reader,
                storage_type=partial(HitrateStorage, cache_hitrate),
            )
            simulator.run()
            assert 300 == simulator.duration
