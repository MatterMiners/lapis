from lapis.storage import Storage
from typing import Optional


class FileProvider(object):

    __slots__ = ("storages",)

    def __init__(self):
        self.storages = dict()

    def add_storage_element(self, storage_element: Storage):
        try:
            self.storages[storage_element.sitename].append(storage_element)
        except KeyError:
            self.storages[storage_element.sitename] = [storage_element]

    def input_file_coverage(
        self, dronesite: str, requested_files: Optional[dict] = None
    ) -> float:
        """
        Dummy implementation, to be replaced

        :param requested_files:
        :param dronesite:
        :return:
        """
        provided_storages = self.storages.get(dronesite, None)
        if provided_storages:
            provides_inputfile = []
            for inputfilename, inputfilespecs in requested_files.items():
                provides_inputfile.append(0)
                for storage in provided_storages:
                    provides_inputfile[-1] += storage.provides_file(
                        (inputfilename, inputfilespecs)
                    )
            return 1 - provided_storages.count(0) / len(provided_storages)
        else:
            return 0
