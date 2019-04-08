import os

import pytest

from lapis.pool_io.htcondor import htcondor_pool_reader


def data_path():
    return os.path.join(os.path.dirname(__file__), "..", "data", "htcondor_pools.csv")


class TestHtcondorPoolReader(object):
    def test_init(self):
        with open(data_path()) as input_file:
            with pytest.raises(AssertionError):
                next(htcondor_pool_reader(input_file))

    def test_simple(self):
        with open(data_path()) as input_file:
            pools = 0
            for pool in htcondor_pool_reader(input_file, make_drone=lambda: None):
                assert pool is not None
                pools += 1
            assert pools > 0
