from typing import Optional


class StoredFile(object):
    def __init__(self, filename, filespecs):
        self.filename = filename
        self._filesize: Optional[int] = filespecs.get("filesize", None)
        self._storedsize: Optional[int] = filespecs.get("storedsize", self._filesize)
        self._cachedsince: Optional[int] = filespecs.get("cachedsince", None)
        self._lastaccessed: Optional[int] = filespecs.get("lastaccessed", None)
        self._numberofaccesses: int = filespecs.get("numberofaccesses", 0)

    @property
    def filesize(self):
        return self._filesize

    @property
    def cachedsince(self):
        return self._cachedsince

    @property
    def lastaccessed(self):
        return self._lastaccessed

    @property
    def numberofaccesses(self):
        return self._numberofaccesses

    @cachedsince.setter
    def cachedsince(self, value: int):
        self._cachedsince = value

    @lastaccessed.setter
    def lastaccessed(self, value: int):
        self._lastaccessed = value

    def increment_accesses(self):
        self._numberofaccesses += 1


class RequestedFile(object):
    def __init__(self, filename: str, filespecs: dict):
        self.filename: str = filename
        self._filesize: Optional[int] = filespecs.get("filesize", None)

    @property
    def filesize(self):
        return self._filesize

    def convert_to_stored_file(self, currenttime):
        filespecs = dict(
            filesize=self._filesize,
            cachedsince=currenttime,
            lastaccessed=currenttime,
            numberofaccesses=1,
        )
        return StoredFile(self.filename, filespecs)
