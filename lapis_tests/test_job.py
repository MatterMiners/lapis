import pytest
from usim import Scope, time

from lapis.drone import Drone
from lapis.job import Job
from lapis_tests import via_usim, DummyScheduler


class TestJob(object):
    def test_init(self):
        with pytest.raises(KeyError):
            Job(resources={}, used_resources={})
        with pytest.raises(KeyError):
            Job(resources={"walltime": 100}, used_resources={})
        assert Job(resources={}, used_resources={"walltime": 100})
        with pytest.raises(AssertionError):
            Job(resources={}, used_resources={"walltime": 100}, in_queue_since=-5)

    def test_name(self):
        name = "test"
        job = Job(resources={}, used_resources={"walltime": 100}, name=name)
        assert job.name == name
        assert repr(job) == "<Job: %s>" % name
        job = Job(resources={}, used_resources={"walltime": 100})
        assert job.name == id(job)
        assert repr(job) == "<Job: %s>" % id(job)

    @via_usim
    async def test_run_job(self):
        job = Job(resources={"walltime": 50}, used_resources={"walltime": 10})
        assert float("inf") == job.waiting_time
        async with Scope() as scope:
            scope.do(job.run())
        assert 10 == time
        assert 0 == job.waiting_time
        assert job.successful

    @via_usim
    async def test_job_in_drone(self):
        scheduler = DummyScheduler()
        job = Job(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 1, "memory": 1},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.start_job(job=job))
        assert 10 == time
        assert 0 == job.waiting_time
        assert job.successful

    @via_usim
    async def test_nonmatching_job_in_drone(self):
        scheduler = DummyScheduler()
        job = Job(
            resources={"walltime": 50, "cores": 2, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 1, "memory": 1},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.start_job(job=job))
        assert 0 == time
        assert not job.successful
        assert 0 == job.waiting_time

    @via_usim
    async def test_two_nonmatching_jobs(self):
        scheduler = DummyScheduler()
        job_one = Job(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        job_two = Job(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 1, "memory": 1},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.start_job(job=job_one))
            scope.do(drone.start_job(job=job_two))
        assert 10 == time
        assert job_one.successful
        assert not job_two.successful
        assert 0 == job_one.waiting_time
        assert 0 == job_two.waiting_time

    @via_usim
    async def test_two_matching_jobs(self):
        scheduler = DummyScheduler()
        job_one = Job(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        job_two = Job(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 2, "memory": 2},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.start_job(job=job_one))
            scope.do(drone.start_job(job=job_two))
        assert 10 == time
        assert job_one.successful
        assert job_two.successful
        assert 0 == job_one.waiting_time
        assert 0 == job_two.waiting_time
