import pytest

from lapis.job import Job


class TestJob(object):
    def test_init(self):
        with pytest.raises(AssertionError):
            Job({}, {})
        assert Job({}, {"walltime": 100})

    def test_name(self):
        name = "test"
        job = Job({}, {"walltime": 100}, name=name)
        assert job.name == name
        job = Job({}, {"walltime": 100})
        assert job.name == id(job)
