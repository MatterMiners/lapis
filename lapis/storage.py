from usim import time, Resources

from typing import Optional

from lapis.files import StoredFile, RequestedFile
from lapis.cachealgorithm import CacheAlgorithm


class Storage(object):

    __slots__ = (
        "name",
        "sitename",
        "storagesize",
        "placement_duration",
        "deletion_duration",
        "_usedstorage",
        "files",
        "filenames",
        "cachealgorithm",
    )

    def __init__(
        self, name: str, sitename: str, storagesize: int, files: Optional[dict] = None
    ):
        self.name = name
        self.sitename = sitename
        self.placement_duration = 10
        self.deletion_duration = 5
        self.storagesize = storagesize
        self.files = self._dict_to_file_object(files)
        self.filenames = set(file.filename for file in self.files)
        self._usedstorage = Resources(**dict(usedsize=self._initial_used_storage()))
        self.cachealgorithm = CacheAlgorithm(self)
        self.__repr__()

    def _initial_used_storage(self):
        initial_value = sum(file.filesize for file in self.files)
        print("{} set initial value {}".format(self.name, initial_value))
        return initial_value

    def _dict_to_file_object(self, files):
        files_set = set()
        for filename, filespecs in files.items():
            files_set.add(StoredFile(filename, filespecs))
        return files_set

    @property
    def usedstorage(self):
        return dict(self._usedstorage.levels)["usedsize"]

    def free_space(self):
        return self.storagesize - self.usedstorage

    def find_file(self, filename):
        for file in self.files:
            if file.filename == filename:
                return file

    async def remove_from_storage(self, file: StoredFile, job_repr):
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
        print(
            "ADD TO STORAGE: Job {}, File {} @ {}".format(
                job_repr, file.filename, time.now
            )
        )
        file = file.convert_to_stored_file(time.now)
        await (time + self.placement_duration)
        await self._usedstorage.increase(**{"usedsize": file.filesize})
        self.filenames.add(file.filename)
        self.files.add(file)

    async def update_file(self, stored_file: StoredFile, job_repr):
        await (time + 1)
        stored_file.lastaccessed = time.now
        stored_file.increment_accesses()
        print(
            "UPDATE: Job {}, File {} @ {}".format(
                job_repr, stored_file.filename, time.now
            )
        )

    async def apply_caching_decision(self, requested_file: RequestedFile, job_repr):
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

    async def providing_file(self, requested_file: RequestedFile, job_repr):
        if requested_file.filename in self.filenames:
            await self.update_file(self.find_file(requested_file.filename), job_repr)
            return True
        else:
            await self.apply_caching_decision(requested_file, job_repr)
            return False

    def __repr__(self):
        return (
            "{name} on site {site}: {used}MB of {tot}MB used ({div} %), contains "
            "files {filelist}".format(
                name=self.name,
                site=self.sitename,
                used=self.usedstorage,
                tot=self.storagesize,
                div=100.0 * self.usedstorage / self.storagesize,
                filelist=", ".join(self.filenames),
            )
        )
