from tempfile import NamedTemporaryFile

from lapis.storage import Storage
from lapis.storage_io.storage import storage_reader


class TestStorageReader(object):
    def _create_simple_config(self):
        storage_config = NamedTemporaryFile(suffix=".csv")
        with open(storage_config.name, "w") as write_stream:
            write_stream.write(
                "name sitename cachesizeGB throughput_limit\n" "name sitename 10 10"
            )
        return storage_config

    def test_empty_files(self):
        simple_config = self._create_simple_config()
        count = 0
        for storage in storage_reader(open(simple_config.name, "r"), None, Storage):
            assert storage is not None
            count += 1
        assert count == 1
