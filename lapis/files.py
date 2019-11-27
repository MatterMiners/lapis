from typing import Optional, NamedTuple


class StoredFile(object):

    __slots__ = (
        "filename",
        "filesize",
        "storedsize",
        "cachedsince",
        "lastaccessed",
        "numberofaccesses",
    )

    def __init__(
        self,
        filename: str,
        filesize: Optional[int] = None,
        storedsize: Optional[int] = None,
        cachedsince: Optional[int] = None,
        lastaccessed: Optional[int] = None,
        numberofaccesses: Optional[int] = None,
        **filespecs,
    ):
        self.filename = filename
        self.filesize = filesize
        self.storedsize = storedsize or self.filesize
        self.cachedsince = cachedsince
        self.lastaccessed = lastaccessed
        self.numberofaccesses = numberofaccesses

    def increment_accesses(self):
        self.numberofaccesses += 1


class RequestedFile(NamedTuple):
    filename: str
    filesize: Optional[int] = None

    def convert_to_stored_file_object(self, currenttime):
        print(self.filesize)
        return StoredFile(
            self.filename,
            filesize=self.filesize,
            cachedsince=currenttime,
            lastaccessed=currenttime,
            numberofaccesses=1,
        )
