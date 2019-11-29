from tempfile import NamedTemporaryFile

from lapis.storageelement import StorageElement
from lapis.storage_io.storage import storage_reader


class TestStorageReader(object):
    def _create_simple_config(self):
        storage_config = NamedTemporaryFile(suffix=".csv")
        with open(storage_config.name, "w") as write_stream:
            write_stream.write(
                "name sitename cachesizeGB throughput_limit\n" "name sitename 10.1 1"
            )
        return storage_config

    def _create_simple_files(self):
        file_config = NamedTemporaryFile(suffix=".csv")
        with open(file_config.name, "w") as write_stream:
            write_stream.write(
                "filename cachename filesize storedsize cachedsince lastaccessed numberofaccesses\n"
                "file name 10.1 5.0 0 0 1"
            )
        return file_config

    def test_empty_files(self):
        simple_config = self._create_simple_config()
        count = 0
        for storage in storage_reader(
            open(simple_config.name, "r"), None, StorageElement
        ):
            assert storage is not None
            count += 1
        assert count == 1

    def test_simple_read(self):
        simple_config = self._create_simple_config()
        simple_files = self._create_simple_files()
        count = 0
        for storage in storage_reader(
            open(simple_config.name, "r"), open(simple_files.name, "r"), StorageElement
        ):
            assert storage is not None
            assert type(storage.available) == int
            assert storage.available == int(5.1 * 1024 * 1024 * 1024)
            assert type(storage.size) == int
            assert storage.size == int(10.1 * 1024 * 1024 * 1024)
            count += 1
        assert count == 1
