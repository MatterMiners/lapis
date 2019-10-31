from lapis.storage import Storage


class FileProvider(object):

    __slots__ = ("storages",)

    def __init__(self):
        self.storages = dict()

    def add_storage_element(self, storage_element: Storage):
        try:
            self.storages[storage_element.sitename].append(storage_element)
        except KeyError:
            self.storages[storage_element.sitename] = [storage_element]

    def provides_all_files(self, job):
        """
        Dummy implementation, to be replaced: if a part of every inputfile of the job is
        provided by a storage element located on the same site as the drone the job
        is running on this function returns True
        :param job:
        :return:
        """
        provided_storages = self.storages.get(job.drone.sitename, None)
        if provided_storages:
            for inputfilename, inputfilespecs in job.inputfiles.items():
                provides_inputfile = 0
                for storage in provided_storages:
                    provides_inputfile += storage.provides_file(
                        (inputfilename, inputfilespecs)
                    )
                if not provides_inputfile:
                    return False
            return True
        else:
            return False
