from typing import Optional, NamedTuple


class StoredFile(object):
    def __init__(self, filename, filespecs):
        self.filename = filename
        self.filesize: Optional[int] = filespecs.get("filesize", None)
        self.storedsize: Optional[int] = filespecs.get("storedsize", self.filesize)
        self.cachedsince: Optional[int] = filespecs.get("cachedsince", None)
        self.lastaccessed: Optional[int] = filespecs.get("lastaccessed", None)
        self.numberofaccesses: int = filespecs.get("numberofaccesses", 0)

    def increment_accesses(self):
        self.numberofaccesses += 1


class RequestedFile(NamedTuple):
    filename: str
    filesize: Optional[int] = None

    def convert_to_stored_file_object(self, currenttime):
        print(self.filesize)
        filespecs = dict(
            filesize=self.filesize,
            cachedsince=currenttime,
            lastaccessed=currenttime,
            numberofaccesses=1,
        )
        return StoredFile(self.filename, filespecs)
