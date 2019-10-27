import os
from lapis.job_io.swf import swf_job_reader


class TestSwfJobReader(object):
    def test_simple_read(self):
        with open(
            os.path.join(os.path.dirname(__file__), "..", "data", "swf_jobs.swf")
        ) as input_file:
            job_count = 0
            for job in swf_job_reader(input_file):
                assert job is not None
                job_count += 1
            assert job_count > 0
