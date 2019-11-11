from lapis.storage import Storage
from lapis.files import RequestedFile
from usim import Queue, Scope, time, Pipe
import random


class FileProvider(object):

    __slots__ = ("storages", "remote_connection")

    def __init__(self, throughput=20):
        self.storages = dict()
        self.remote_connection = Pipe(throughput=throughput)

    def add_storage_element(self, storage_element: Storage):
        """
        Register storage element in FileProvider clustering storage elements by sitename
        :param storage_element:
        :return:
        """
        try:
            self.storages[storage_element.sitename].append(storage_element)
        except KeyError:
            self.storages[storage_element.sitename] = [storage_element]

    async def determine_inputfile_source(
        self, requested_file: RequestedFile, dronesite: str, job_repr: str
    ):
        """
        Collects NamedTuples containing the amount of data of the requested file
        cached in a storage element and the storage element for all reachable storage
        objects on the drone's site. The tuples are sorted by amount of cached data
        and the storage object where the biggest part of the file is cached is
        returned. If the file is not cached in any storage object the fileproviders
        remote connection is returned.
        :param requested_file:
        :param dronesite:
        :param job_repr:
        :return:
        """
        provided_storages = self.storages.get(dronesite, None)
        if provided_storages:
            look_up_queue = Queue()
            async with Scope() as scope:
                for storage in provided_storages:
                    scope.do(
                        storage.look_up_file(requested_file, look_up_queue, job_repr)
                    )
            await look_up_queue.close()
            storage_list = sorted(
                [entry async for entry in look_up_queue],
                key=lambda x: x[0],
                reverse=True,
            )
            if storage_list[0].cached_filesize > 0:
                return storage_list[0].storage
            else:
                return self.remote_connection
        else:
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
        used_connection = await self.determine_inputfile_source(
            requested_file, dronesite, job_repr
        )
        print(used_connection)
        if used_connection == self.remote_connection:
            potential_cache = random.choice(self.storages.get(dronesite, None))
            await used_connection.transfer(requested_file.filesize)
            await potential_cache.apply_caching_decision(requested_file, job_repr)

        else:
            print("now transfering", requested_file.filesize)
            await used_connection.transfer(requested_file, job_repr)
            print(
                "Job {}: finished transfering of file {}: {}GB @ {}".format(
                    job_repr, requested_file.filename, requested_file.filesize, time.now
                )
            )

    async def transfer_inputfiles(self, drone, requested_files: dict, job_repr):
        """
        Converts dict information about requested files to RequestedFile object and
        parallely launches streaming for all files
        :param drone:
        :param requested_files:
        :param job_repr:
        :return:
        """
        start_time = time.now
        async with Scope() as scope:
            for inputfilename, inputfilespecs in requested_files.items():
                requested_file = RequestedFile(inputfilename, inputfilespecs)
                scope.do(self.stream_file(requested_file, drone.sitename, job_repr))
        stream_time = time.now - start_time
        print(
            "STREAMED files {} in {}".format(list(requested_files.keys()), stream_time)
        )
        return stream_time
