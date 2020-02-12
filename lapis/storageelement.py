from typing import Optional

from usim import time, Resources, Scope
from monitoredpipe import MonitoredPipe

from lapis.files import StoredFile, RequestedFile, RequestedFile_HitrateBased
from lapis.interfaces._storage import Storage, LookUpInformation

import logging


class RemoteStorage(Storage):
    def __init__(self, pipe: MonitoredPipe):
        self.connection = pipe
        pipe.storage = repr(self)

    @property
    def size(self):
        return float("Inf")

    @property
    def available(self):
        return float("Inf")

    @property
    def used(self):
        return 0

    async def transfer(self, file: RequestedFile, **kwargs):
        await self.connection.transfer(total=file.filesize)

    async def add(self, file: StoredFile, **kwargs):
        raise NotImplementedError

    async def remove(self, file: StoredFile, **kwargs):
        raise NotImplementedError

    def find(self, file: RequestedFile, **kwargs) -> LookUpInformation:
        raise NotImplementedError


class StorageElement(Storage):

    __slots__ = (
        "name",
        "sitename",
        "_size",
        "deletion_duration",
        "update_duration",
        "_usedstorage",
        "files",
        "filenames",
        "connection",
        "remote_storage",
    )

    def __init__(
        self,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1000 * 1000 * 1000,
        throughput_limit: int = 10 * 1000 * 1000 * 1000,
        files: Optional[dict] = None,
    ):
        self.name = name
        self.sitename = sitename
        self.deletion_duration = 5
        self.update_duration = 1
        self._size = size
        self.files = files
        self._usedstorage = Resources(
            size=sum(file.storedsize for file in files.values())
        )
        self.connection = MonitoredPipe(throughput_limit)
        self.connection.storage = repr(self)

        self.remote_storage = None

    @property
    def size(self):
        return self._size

    @property
    def used(self):
        return self._usedstorage.levels.size

    @property
    def available(self):
        return self.size - self.used

    async def remove(self, file: StoredFile, job_repr=None):
        """
        Deletes file from storage object. The time this operation takes is defined
        by the storages deletion_duration attribute.
        :param file:
        :param job_repr: Needed for debug output, will be replaced
        :return:
        """
        print(
            "REMOVE FROM STORAGE: Job {}, File {} @ {}".format(
                job_repr, file.filename, time.now
            )
        )
        await (time + self.deletion_duration)
        await self._usedstorage.decrease(size=file.filesize)
        self.files.pop(file.filename)

    async def add(self, file: RequestedFile, job_repr=None):
        """
        Adds file to storage object transfering it through the storage objects
        connection. This should be sufficient for now because files are only added
        to the storage when they are also transfered through the Connections remote
        connection. If this simulator is extended to include any kind of
        direct file placement this has to be adapted.
        :param file:
        :param job_repr: Needed for debug output, will be replaced
        :return:
        """
        print(
            "ADD TO STORAGE: Job {}, File {} @ {}".format(
                job_repr, file.filename, time.now
            )
        )
        file = file.convert_to_stored_file_object(time.now)
        await self._usedstorage.increase(size=file.filesize)
        self.files[file.filename] = file
        await self.connection.transfer(file.filesize)

    async def _update(self, stored_file: StoredFile, job_repr):
        """
        Updates a stored files information upon access.
        :param stored_file:
        :param job_repr: Needed for debug output, will be replaced
        :return:
        """
        await (time + self.update_duration)
        stored_file.lastaccessed = time.now
        stored_file.increment_accesses()
        print(
            "UPDATE: Job {}, File {} @ {}".format(
                job_repr, stored_file.filename, time.now
            )
        )

    async def transfer(self, file: RequestedFile, job_repr=None):
        """
        Manages file transfer via the storage elements connection and updates file
        information. If the file should have been deleted since it was originally
        looked up the resulting error is not raised.
        :param file:
        :param job_repr:  Needed for debug output, will be replaced
        :return:
        """
        await self.connection.transfer(file.filesize)
        try:
            # TODO: needs handling of KeyError
            await self._update(self.files[file.filename], job_repr)
        except AttributeError:
            pass

    def find(self, requested_file: RequestedFile, job_repr=None):
        """
        Searches storage object for the requested_file and sends result (amount of
        cached data, storage object) to the queue.
        :param requested_file:
        :param job_repr: Needed for debug output, will be replaced
        :return: (amount of cached data, storage object)
        """
        # print(
        #     "LOOK UP FILE: Job {}, File {}, Storage {} @ {}".format(
        #         job_repr, requested_file.filename, repr(self), time.now
        #     )
        # )
        try:
            result = LookUpInformation(
                self.files[requested_file.filename].filesize, self
            )
        except KeyError:
            # print(
            #     "File {} not cached on any reachable storage".format(
            #         requested_file.filename
            #     )
            # )
            result = LookUpInformation(0, self)
        return result

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.name or id(self))


class HitrateStorage(StorageElement):
    def __init__(
        self,
        hitrate,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1000 * 1000 * 1000,
        throughput_limit: int = 10 * 1000 * 1000 * 1000,
        files: Optional[dict] = None,
    ):
        super(HitrateStorage, self).__init__(
            name=name,
            sitename=sitename,
            size=size,
            throughput_limit=throughput_limit,
            files=files,
        )
        self._hitrate = hitrate

    @property
    def available(self):
        return self.size

    @property
    def used(self):
        return 0

    async def transfer(self, file: RequestedFile, job_repr=None):
        # print(
        #     "TRANSFER: {}, filesize {}, remote: {}/{}, cache: {}/{}".format(
        #         self._hitrate,
        #         file.filesize,
        #         (1 - self._hitrate) * file.filesize,
        #         self.remote_storage.connection.throughput,
        #         self._hitrate * file.filesize,
        #         self.connection.throughput,
        #     )
        # )
        async with Scope() as scope:
            logging.getLogger("implementation").warning(
                "{} {} @ {} in {}".format(
                    self._hitrate * file.filesize,
                    (1 - self._hitrate) * file.filesize,
                    time.now,
                    file.filename[-30:],
                )
            )
            scope.do(self.connection.transfer(total=self._hitrate * file.filesize))
            scope.do(
                self.remote_storage.connection.transfer(
                    total=(1 - self._hitrate) * file.filesize
                )
            )

    def find(self, requested_file: RequestedFile, job_repr=None):
        return LookUpInformation(requested_file.filesize, self)

    async def add(self, file: RequestedFile, job_repr=None):
        pass

    async def remove(self, file: StoredFile, job_repr=None):
        pass


class FileBasedHitrateStorage(StorageElement):
    def __init__(
        self,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1000 * 1000 * 1000,
        throughput_limit: int = 10 * 1000 * 1000 * 1000,
        files: Optional[dict] = None,
    ):
        super(FileBasedHitrateStorage, self).__init__(
            name=name,
            sitename=sitename,
            size=size,
            throughput_limit=throughput_limit,
            files=files,
        )

    @property
    def available(self):
        return self.size

    @property
    def used(self):
        return 0

    async def transfer(self, file: RequestedFile_HitrateBased, job_repr=None):
        # print(
        #     "TRANSFER: on {} with {}, filesize {}, remote: {}/{}, cache: {}/{}".format(
        #         self.name,
        #         file.cachehitrate,
        #         file.filesize,
        #         (1 - file.cachehitrate) * file.filesize,
        #         self.remote_storage.connection.throughput,
        #         file.cachehitrate * file.filesize,
        #         self.connection.throughput,
        #     )
        # )
        if file.cachehitrate:
            await self.connection.transfer(total=file.filesize)
        else:
            await self.remote_storage.connection.transfer(total=file.filesize)

    def find(self, requested_file: RequestedFile_HitrateBased, job_repr=None):
        # return LookUpInformation(requested_file.filesize, self)
        return LookUpInformation(
            requested_file.filesize * requested_file.cachehitrate, self
        )

    async def add(self, file: RequestedFile, job_repr=None):
        pass

    async def remove(self, file: StoredFile, job_repr=None):
        pass
