from tempfile import NamedTemporaryFile

from lapis.storageelement import StorageElement
from lapis.storage_io.storage import storage_reader


class TestStorageReader(object):
    def _create_simple_config(self, to_string=False):
        storage_config = NamedTemporaryFile(suffix=".csv")
        with open(storage_config.name, "w") as write_stream:
            write_stream.write(
                f"name sitename cachesizeGB throughput_limit\n"
                f"name sitename {str(10) if to_string else 10} {str(10.1) if to_string else 10.1} {str(1) if to_string else 1}"
            )
        return storage_config

    def _create_simple_files(self, to_string=False):
        file_config = NamedTemporaryFile(suffix=".csv")
        with open(file_config.name, "w") as write_stream:
            write_stream.write(
                f"filename cachename filesize storedsize cachedsince lastaccessed numberofaccesses\n"
                f"file name {str(10.1) if to_string else 10.1} {str(5.0) if to_string else 5.0} {str(0) if to_string else 0} {str(0) if to_string else 0} {str(1) if to_string else 1}"
            )
        return file_config

    def test_empty_files(self):
        simple_config = self._create_simple_config()
        count = 0
        for storage in storage_reader(
            open(simple_config.name, "r+"), None, StorageElement
        ):
            assert storage is not None
            count += 1
        assert count == 1

    def test_simple_read(self):
        for variant in [False, True]:
            print(f"starting with {variant}")
            simple_config = self._create_simple_config(to_string=variant)
            simple_files = self._create_simple_files(to_string=variant)
            count = 0
            for storage in storage_reader(
                open(simple_config.name, "r"),
                open(simple_files.name, "r"),
                StorageElement,
            ):
                assert storage is not None
                assert type(storage.available) == int
                assert storage.available == int(5.0 * 1024 * 1024 * 1024)
                assert type(storage.size) == int
                assert storage.size == int(10.0 * 1024 * 1024 * 1024)
                count += 1
            assert count == 1
