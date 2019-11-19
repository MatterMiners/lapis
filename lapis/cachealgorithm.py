from typing import Optional, Set
from lapis.files import RequestedFile
from lapis.utilities.cache_algorithm_implementations import cache_algorithm
from lapis.utilities.cache_cleanup_implementations import sort_files_by_cachedsince


class CacheAlgorithm(object):
    def __init__(self, storage, additional_information: Optional[str] = None):
        self._storage = storage
        self._additional_information = additional_information

    def _file_based_consideration(self, candidate: RequestedFile) -> bool:
        """
        File based caching decision: Checks if candidate file should be cached based on
        conditions that apply to
        file itself without considering the caches overall state.
        :param candidate:
        :return:
        """
        if self._storage.storagesize > candidate.filesize:
            return cache_algorithm["standard"](candidate)
        else:
            return False

    def _context_based_consideration(self, candidate: RequestedFile):
        """
        Caching decision based on the the overall context
        :param candidate:
        :return:
        """
        to_be_removed = set()
        sorted_stored_files = sort_files_by_cachedsince(self._storage.files)
        current_free_storage = self._storage.free_space()
        for stored_file in sorted_stored_files:
            if stored_file.numberofaccesses < 3:
                to_be_removed.add(stored_file)
                current_free_storage += stored_file.filesize
                if current_free_storage >= candidate.filesize:
                    return to_be_removed
            else:
                continue
        if current_free_storage >= candidate.filesize:
            return {candidate}

    def consider(self, candidate: RequestedFile) -> Optional[Set[RequestedFile]]:
        if self._file_based_consideration(candidate):
            if self._storage.free_space() < candidate.filesize:
                return self._context_based_consideration(candidate)
            else:
                return set()
        else:
            return {candidate}
