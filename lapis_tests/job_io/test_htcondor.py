from lapis.job_io.htcondor import htcondor_job_reader


class TestHtcondorJobReader(object):
    def test_simple_read(self):
        with open("../data/htcondor_jobs.csv") as input_file:
            jobs = 0
            for job in htcondor_job_reader(None, input_file):
                assert job is not None
                jobs += 1
            assert jobs > 0
