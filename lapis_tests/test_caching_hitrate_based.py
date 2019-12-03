from usim import time

from lapis_tests import via_usim, DummyDrone
from lapis.connection import Connection
from lapis.storageelement import HitrateStorage
from lapis.files import RequestedFile


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
