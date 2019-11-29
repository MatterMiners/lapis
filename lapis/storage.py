from typing import Optional, NamedTuple

from usim import time, Resources, Pipe, Scope

from lapis.files import StoredFile, RequestedFile


class LookUpInformation(NamedTuple):
    cached_filesize: int
    storage: "Storage"


class RemoteStorage(object):
    def __init__(self, pipe: Pipe):
        self._connection = pipe

    async def transfer(self, total, job_repr):
        await self._connection.transfer(total=total)


class Storage(object):

    __slots__ = (
        "name",
        "sitename",
        "size",
        "deletion_duration",
        "update_duration",
        "_usedstorage",
        "files",
        "filenames",
        "connection",
        "remote_connection",
    )

    def __init__(
        self,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1024 * 1024 * 1024,
        throughput_limit: int = 10 * 1024 * 1024 * 1024,
        files: Optional[dict] = None,
    ):
        self.name = name
        self.sitename = sitename
        self.deletion_duration = 5
        self.update_duration = 1
        self.size = size
        self.files = files
        self._usedstorage = Resources(
            size=sum(file.filesize for file in files.values())
        )
        self.connection = Pipe(throughput_limit)
        self.remote_connection = None

    @property
    def usedstorage(self):
        return self._usedstorage.levels.size

    @property
    def free_space(self):
        return self.size - self.usedstorage

    async def remove(self, file: StoredFile, job_repr):
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
        await self._usedstorage.decrease(usedsize=file.filesize)
        self.files.pop(file.filename)

    async def add(self, file: RequestedFile, job_repr):
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
        await self._usedstorage.increase(usedsize=file.filesize)
        self.files[file.filename] = file
        await self.connection.transfer(file.filesize)

    async def update_file(self, stored_file: StoredFile, job_repr):
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

    async def transfer(self, file, job_repr):
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
            await self.update_file(self.files[file.filename], job_repr)
        except AttributeError:
            pass

    def look_up_file(self, requested_file: RequestedFile, job_repr):
        """
        Searches storage object for the requested_file and sends result (amount of
        cached data, storage object) to the queue.
        :param requested_file:
        :param job_repr: Needed for debug output, will be replaced
        :return: (amount of cached data, storage object)
        """
        print(
            "LOOK UP FILE: Job {}, File {}, Storage {} @ {}".format(
                job_repr, requested_file.filename, repr(self), time.now
            )
        )
        try:
            result = LookUpInformation(
                self.files[requested_file.filename].filesize, self
            )
        except KeyError:
            print(
                "File {} not cached on any reachable storage".format(
                    requested_file.filename
                )
            )
            result = LookUpInformation(0, self)
        return result

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.name or id(self))


class HitrateStorage(Storage):
    def __init__(
        self,
        hitrate,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1024 * 1024 * 1024,
        throughput_limit: int = 10 * 1024 * 1024 * 1024,
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

    async def transfer(self, file, job_repr):
        async with Scope() as scope:
            scope.do(self.connection.transfer(total=self._hitrate * file.filesize))
            scope.do(
                self.remote_connection.transfer(
                    total=(1 - self._hitrate) * file.filesize, job_repr=job_repr
                )
            )

    def look_up_file(self, requested_file: RequestedFile, job_repr):
        return LookUpInformation(requested_file.filesize, self)

    async def add(self, file: RequestedFile, job_repr):
        pass

    async def remove(self, file: StoredFile, job_repr):
        pass
