from usim import time, Resources, Pipe

from typing import Optional, NamedTuple

from lapis.files import StoredFile, RequestedFile
from lapis.cachealgorithm import CacheAlgorithm


class LookUpInformation(NamedTuple):
    cached_filesize: int
    storage: "Storage"


class Storage(object):

    __slots__ = (
        "name",
        "sitename",
        "storagesize",
        "deletion_duration",
        "update_duration",
        "_usedstorage",
        "files",
        "filenames",
        "cachealgorithm",
        "connection",
    )

    def __init__(
        self,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        storagesize: int = 1000,
        throughput_limit: int = 10,
        files: Optional[dict] = None,
    ):
        self.name = name
        self.sitename = sitename
        self.deletion_duration = 5
        self.update_duration = 1
        self.storagesize = storagesize
        self.files = files
        self._usedstorage = Resources(
            size=sum(file.filesize for file in files.values())
        )
        self.cachealgorithm = CacheAlgorithm(self)
        self.connection = Pipe(throughput_limit)
        self.__repr__()

    @property
    def usedstorage(self):
        return self._usedstorage.levels.size

    @property
    def free_space(self):
        return self.storagesize - self.usedstorage

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

    async def apply_caching_decision(self, requested_file: RequestedFile, job_repr):
        """
        Applies the storage objects caching algorithm to the requested_file and
        initiates resulting changes like placement and deletion of files
        :param requested_file:
        :param job_repr: Needed for debug output, will be replaced
        :return:
        """

        print(
            "APPLY CACHING DECISION: Job {}, File {} @ {}".format(
                job_repr, requested_file.filename, time.now
            )
        )
        to_be_removed = self.cachealgorithm.consider(requested_file)
        if not to_be_removed:
            await self.add(requested_file, job_repr)
        elif to_be_removed == {requested_file}:
            # file will not be cached because it either does not match
            # conditions or because there is no space in the cache
            print(
                "APPLY CACHING DECISION: Job {}, File {}: File wasnt "
                "cached @ {}".format(job_repr, requested_file.filename, time.now)
            )
        else:
            for file in to_be_removed:
                await self.remove(file, job_repr)

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
