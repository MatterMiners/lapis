import os

from lapis.job_io.htcondor import htcondor_job_reader


class TestHtcondorJobReader(object):
    def test_simple_read(self):
        with open(
            os.path.join(os.path.dirname(__file__), "..", "data", "htcondor_jobs.csv")
        ) as input_file:
            jobs = 0
            for job in htcondor_job_reader(input_file):
                assert job is not None
                jobs += 1
            assert jobs > 0
        with open(
            os.path.join(os.path.dirname(__file__), "..", "data", "htcondor_jobs.csv")
        ) as input_file:
            # ensure that one job was removed by importer (wrong walltime given)
            lines = sum(1 for _ in input_file)
            assert jobs == (lines - 2)
