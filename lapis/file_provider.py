from lapis.storage import Storage
from lapis.files import RequestedFile
from typing import Optional
from usim import Queue, Scope


class FileProvider(object):

    __slots__ = ("storages",)

    def __init__(self):
        self.storages = dict()

    def add_storage_element(self, storage_element: Storage):
        """
        Register storage element in FileProvider clustering storage elements by sitename
        :param storage_element:
        :return:
        """
        try:
            self.storages[storage_element.sitename].append(storage_element)
        except KeyError:
            self.storages[storage_element.sitename] = [storage_element]

    async def input_file_coverage(
        self, dronesite: str, requested_files: Optional[dict] = None, job_repr=None
    ) -> float:
        """
        Dummy implementation, to be replaced

        :param requested_files:
        :param dronesite:
        :return:
        """
        print("FILEPROVIDER hit input file coverage")

        provided_storages = self.storages.get(dronesite, None)
        if provided_storages:
            score_queue = Queue()
            async with Scope() as scope:
                for inputfilename, inputfilespecs in requested_files.items():
                    scope.do(
                        self.look_file_up_in_storage(
                            RequestedFile(inputfilename, inputfilespecs),
                            provided_storages,
                            job_repr,
                            score_queue,
                        )
                    )
            await score_queue.close()
            total_score = await self.calculate_score(score_queue)
            return total_score / len(provided_storages)
        else:
            return 0

    async def look_file_up_in_storage(
        self, requested_file: RequestedFile, available_storages: list, job_repr, q
    ):
        """
        Calculates how many storages provide the requested file, puts result in queue
        for readout.
        :param requested_file:
        :param available_storages:
        :param job_repr:
        :param q:
        :return:
        """
        file_score = sum(
            [
                await storage.providing_file(requested_file, job_repr)
                for storage in available_storages
            ]
        )
        await q.put({requested_file.filename: file_score})

    async def calculate_score(self, queue: Queue):
        """
        Reads each input files individual score from queue and returns number of input
        files that are provided by a storage element.
        :param queue:
        :return:
        """
        return sum([1 async for element in queue if list(element.values())[0] > 0])
