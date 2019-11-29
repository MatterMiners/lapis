import abc

from typing import NamedTuple

from lapis.files import RequestedFile, StoredFile


class LookUpInformation(NamedTuple):
    cached_filesize: int
    storage: "Storage"


class Storage(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def size(self) -> int:
        """Total size of storage in Bytes"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def available(self) -> int:
        """Available storage in Bytes"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def used(self) -> int:
        """Used storage in Bytes"""
        raise NotImplementedError

    @abc.abstractmethod
    async def transfer(self, file: RequestedFile, job_repr):
        """
        Transfer size of given file via the storages' connection and update file
        information. If the file was deleted since it was originally looked up
        the resulting error is not raised.

        .. TODO:: What does this mean with the error?
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def add(self, file: RequestedFile, job_repr):
        """
        Add file information to storage and transfer the size of the file via
        the storages' connection.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def remove(self, file: StoredFile, job_repr):
        """
        Remove all file information and used filesize from the storage.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def update(self, file: StoredFile, job_repr):
        """
        Updates a stored files information upon access.

        .. TODO:: This should be included in an operation to access/transfer.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def find(self, file: RequestedFile, job_repr) -> LookUpInformation:
        """Information if a file is stored in Storage"""
        raise NotImplementedError
