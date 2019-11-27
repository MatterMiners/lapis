import logging

from lapis.storage import Storage
from lapis.files import RequestedFile
from lapis.monitor import sampling_required

from usim import Scope, time, Pipe
import random


class Connection(object):

    __slots__ = ("storages", "remote_connection", "cachehitrate")

    def __init__(self, throughput=100, cache_hitrate=None):
        self.storages = dict()
        self.remote_connection = Pipe(throughput=throughput)
        self.cachehitrate = cache_hitrate

    def add_storage_element(self, storage_element: Storage):
        """
        Register storage element in Connetion module clustering storage elements by
        sitename
        :param storage_element:
        :return:
        """
        try:
            self.storages[storage_element.sitename].append(storage_element)
        except KeyError:
            self.storages[storage_element.sitename] = [storage_element]

    async def _determine_inputfile_source(
        self, requested_file: RequestedFile, dronesite: str, job_repr: str
    ):
        """
        Collects NamedTuples containing the amount of data of the requested file
        cached in a storage element and the storage element for all reachable storage
        objects on the drone's site. The tuples are sorted by amount of cached data
        and the storage object where the biggest part of the file is cached is
        returned. If the file is not cached in any storage object the connection module
        remote connection is returned.
        :param requested_file:
        :param dronesite:
        :param job_repr:
        :return:
        """
        provided_storages = self.storages.get(dronesite, None)
        if provided_storages is not None:
            look_up_list = []
            for storage in provided_storages:
                look_up_list.append(storage.look_up_file(requested_file, job_repr))
            storage_list = sorted(
                [entry async for entry in look_up_list],
                key=lambda x: x[0],
                reverse=True,
            )
            for entry in storage_list:
                # TODO: check should better check that size is bigger than requested
                if entry.cached_filesize > 0:
                    return entry.storage
        return self.remote_connection

    async def stream_file(self, requested_file: RequestedFile, dronesite, job_repr):
        """
        Determines which storage object is used to provide the requested file and
        startes the files transfer. For files transfered via remote connection a
        potential cache decides whether to cache the file and handles the caching
        process.
        :param requested_file:
        :param dronesite:
        :param job_repr:
        :return:
        """
        used_connection = await self._determine_inputfile_source(
            requested_file, dronesite, job_repr
        )

        await sampling_required.put(used_connection)
        if used_connection == self.remote_connection and self.storages.get(
            dronesite, None
        ):
            try:
                potential_cache = random.choice(self.storages[dronesite])
                await potential_cache.apply_caching_decision(requested_file, job_repr)
            except KeyError:
                pass
        print(f"now transfering {requested_file.filesize} from {used_connection}")
        await used_connection.transfer(requested_file.filesize, job_repr)
        print(
            "Job {}: finished transfering of file {}: {}GB @ {}".format(
                job_repr, requested_file.filename, requested_file.filesize, time.now
            )
        )

    async def transfer_files(self, drone, requested_files: dict, job_repr):
        """
        Converts dict information about requested files to RequestedFile object and
        parallely launches streaming for all files
        :param drone:
        :param requested_files:
        :param job_repr:
        :return:
        """
        print("registered caches", self.storages)
        start_time = time.now
        async with Scope() as scope:
            for inputfilename, inputfilespecs in requested_files.items():
                requested_file = RequestedFile(
                    inputfilename, inputfilespecs["filesize"]
                )
                if self.cachehitrate is not None:

                    scope.do(
                        self.transfer_by_cache_hitrate(
                            self.storages.get(drone.sitename, None), requested_file
                        )
                    )
                else:
                    scope.do(self.stream_file(requested_file, drone.sitename, job_repr))
        stream_time = time.now - start_time
        print(
            "STREAMED files {} in {}".format(list(requested_files.keys()), stream_time)
        )
        return stream_time

    async def transfer_by_cache_hitrate(
        self, available_storages: Storage, requested_file: RequestedFile
    ):
        if not available_storages and self.cachehitrate:
            logging.getLogger("implementation").error(
                "no available caches for drone "
                " requested cachehitrate was "
                "{}".format(self.cachehitrate)
            )
        else:
            if 0 < self.cachehitrate < 1:
                async with Scope() as scope:
                    scope.do(
                        self.transfer_wrapper(
                            self.remote_connection,
                            (1.0 - self.cachehitrate) * requested_file.filesize,
                        )
                    )
                    scope.do(
                        self.transfer_wrapper(
                            available_storages[0].connection,
                            self.cachehitrate * requested_file.filesize,
                        )
                    )
            elif self.cachehitrate == 1:
                await available_storages[0].connection.transfer(requested_file.filesize)
            elif self.cachehitrate == 0:
                await self.remote_connection.transfer(requested_file.filesize)

    async def transfer_wrapper(self, connection, total):
        print(
            "transfering {} with {}, start @ {}".format(
                total, connection.throughput, time.now
            )
        )
        await sampling_required.put(connection)
        await connection.transfer(total=total)
        print(
            "transfering {} with {}, stop @ {}".format(
                total, connection.throughput, time.now
            )
        )
