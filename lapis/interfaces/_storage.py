import abc

from typing import NamedTuple

from lapis.files import RequestedFile, StoredFile


class LookUpInformation(NamedTuple):
    cached_filesize: int
    storage: "Storage"


class Storage(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def size(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def available(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def used(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def transfer(self, file: RequestedFile, job_repr):
        raise NotImplementedError

    @abc.abstractmethod
    async def add(self, file: RequestedFile, job_repr):
        raise NotImplementedError

    @abc.abstractmethod
    async def remove(self, file: StoredFile, job_repr):
        raise NotImplementedError

    @abc.abstractmethod
    async def update(self, file: StoredFile, job_repr):
        raise NotImplementedError

    @abc.abstractmethod
    def find(self, file: RequestedFile, job_repr) -> LookUpInformation:
        raise NotImplementedError
