from usim import time, Resources, Pipe, Queue

from typing import Optional, NamedTuple

from lapis.files import StoredFile, RequestedFile
from lapis.cachealgorithm import CacheAlgorithm


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
        name: str = None,
        sitename: str = None,
        storagesize: int = 1000,
        throughput_limit: int = 10,
        files: Optional[dict] = None,
    ):
        self.name = name
        self.sitename = sitename
        self.deletion_duration = 5
        self.update_duration = 1
        self.storagesize = storagesize
        self.files = self._dict_to_file_object(files)
        self.filenames = set(file.filename for file in self.files)
        self._usedstorage = Resources(usedsize=sum(file.filesize for file in self.files))
        self.cachealgorithm = CacheAlgorithm(self)
        self.connection = Pipe(throughput_limit)
        self.__repr__()

    def _initial_used_storage(self):
        initial_value = sum(file.filesize for file in self.files)
        print("{} set initial value {}".format(self.name, initial_value))
        return initial_value

    def _dict_to_file_object(self, files):
        files_set = set()
        if files:
            for filename, filespecs in files.items():
                files_set.add(StoredFile(filename, filespecs))
        return files_set

    @property
    def usedstorage(self):
        return self._usedstorage.levels.usedsize

    def free_space(self):
        return self.storagesize - self.usedstorage

    def find_file(self, filename):
        """
        Searches storage object for file with passed filename
        :param filename:
        :return:
        """
        return filename in self.files
            if file.filename == filename:
                return file

    async def remove_from_storage(self, file: StoredFile, job_repr):
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
        await self._usedstorage.decrease(**{"usedsize": file.filesize})
        self.filenames.remove(file.filename)
        self.files.remove(file)

    async def add_to_storage(self, file: RequestedFile, job_repr):
        """
        Adds file to storage object transfering it through the storage objects
        connection. This should be sufficient for now because files are only added
        to the storage when they are also transfered through the FileProviders remote
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
        print(file.filesize)
        await self.connection.transfer(file.filesize)
        await self._usedstorage.increase(**{"usedsize": file.filesize})
        self.filenames.add(file.filename)
        self.files.add(file)

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
            await self.update_file(self.find_file(file.filename), job_repr)
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
            await self.add_to_storage(requested_file, job_repr)
        elif to_be_removed == {requested_file}:
            # file will not be cached because it either does not match
            # conditions or because there is no space in the cache
            print(
                "APPLY CACHING DECISION: Job {}, File {}: File wasnt "
                "cached @ {}".format(job_repr, requested_file.filename, time.now)
            )
        else:
            for file in to_be_removed:
                await self.remove_from_storage(file, job_repr)

    async def look_up_file(self, requested_file: RequestedFile, queue: Queue, job_repr):
        """
        Searches storage object for the requested_file and sends result (amount of
        cached data, storage object) to queue if queue was passed as parameter.
        If no queue was passed the result is returned normally. This might be needed
        when looking up files during coordination and is to be removed if it's not
        necessary.
        :param requested_file:
        :param queue:
        :param job_repr: Needed for debug output, will be replaced
        :return: (amount of cached data, storage object)
        """
        print(
            "LOOK UP FILE: Job {}, File {}, Storage {} @ {}".format(
                job_repr, requested_file.filename, repr(self), time.now
            )
        )

        class LookUpInformation(NamedTuple):
            cached_filesize: int
            storage: Storage

        if queue:
            try:
                print(self.find_file(requested_file.filename).filesize)
                await queue.put(
                    LookUpInformation(
                        self.find_file(requested_file.filename).filesize, self
                    )
                )
            except AttributeError:
                print(
                    "File {} not cached on any reachable storage".format(
                        requested_file.filename
                    )
                )
                await queue.put(LookUpInformation(0, self))
        else:
            return LookUpInformation(
                self.find_file(requested_file.filename).filesize, self
            )

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.name or id(self))

        # return "{name} on site {site}: {used}MB of {tot}MB used ({div} %)".format(
        #     name=self.name,
        #     site=self.sitename,
        #     used=self.usedstorage,
        #     tot=self.storagesize,
        #     div=100.0 * self.usedstorage / self.storagesize,
        # )
