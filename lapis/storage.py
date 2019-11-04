from usim import time

from typing import Optional

from lapis.utilities.cache_algorithm_implementations import cache_algorithm
from lapis.utilities.cache_cleanup_implementations import cache_cleanup


class Storage(object):

    __slots__ = ("name", "sitename", "storagesize", "usedstorage", "content")

    def __init__(
        self, name: str, sitename: str, storagesize: int, content: Optional[dict] = None
    ):
        self.name = name
        self.sitename = sitename
        self.storagesize = storagesize
        self.content = content
        self.usedstorage = self._calculate_used_storage()
        self.describe_state()

    def _calculate_used_storage(self):
        return sum(subdict["usedsize"] for subdict in self.content.values())

    def free_space(self):
        return self.storagesize - self.usedstorage

    def place_new_file(self, filerequest: tuple):
        filename, filespecs = filerequest
        if self.free_space() - filespecs["usedsize"] < 0:
            self.make_room(self.free_space() - filespecs["usedsize"])
        self.content.update({filename: filespecs})
        self.content[filename].update(
            cachedsince=time.now, lastaccessed=time.now, numberofaccesses=0
        )
        self.usedstorage = self._calculate_used_storage()

    def update_file(self, filerequest: tuple):
        filename, filespecs = filerequest
        requested_file = filename
        filesize_difference = (
            filespecs["usedsize"] - self.content[requested_file]["usedsize"]
        )
        if filesize_difference > 0:
            self.make_room(filesize_difference)
            self.content[requested_file]["usedsize"] += filesize_difference
        self.content[requested_file]["lastaccessed"] = time.now
        self.content[requested_file]["numberofaccesses"] += 1
        self.usedstorage = self._calculate_used_storage()

    def make_room(self, filesize_difference: int):
        if self.free_space() - filesize_difference < 0:
            cache_cleanup["fifo"](filesize_difference, self)

    def provides_file(self, filerequest: dict):
        filename, filespecs = filerequest
        if filename in self.content.keys():
            self.update_file(filerequest)
            return True
        else:
            if self.cache_file():
                self.place_new_file(filerequest)
            return False

    def cache_file(self):
        # cache everything, test different implementations
        return cache_algorithm["standard"]()

    def describe_state(self):
        print(
            "{name} on site {site}: {used}MB of {tot}MB used ({div} %), contains "
            "files {filelist}".format(
                name=self.name,
                site=self.sitename,
                used=self.usedstorage,
                tot=self.storagesize,
                div=100.0 * self.usedstorage / self.storagesize,
                filelist=", ".join(self.content.keys()),
            )
        )
