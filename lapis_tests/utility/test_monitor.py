import ast
import pytest
from time import time as pytime

from cobald.monitor.format_line import LineProtocolFormatter
from usim import Scope, time

from lapis_tests import via_usim

from . import make_test_logger

from lapis.monitor.general import resource_statistics
from lapis.monitor import SimulationTimeFilter, Monitoring


def parse_line_protocol(literal: str):
    name_tags, _, fields_stamp = literal.strip().partition(" ")
    fields, _, stamp = fields_stamp.partition(" ")
    fields = fields.split(",") if fields else []
    name, *tags = name_tags.split(",")
    return (
        name,
        {key: value for key, value in (tag.split("=") for tag in tags)},
        {
            key: ast.literal_eval(value)
            for key, value in (field.split("=") for field in fields)
        },
        None if not stamp else int(stamp),
    )


class TestSimulationTimeFilter(object):
    @via_usim
    async def test_simple(self):
        payload = {"a": "a"}
        logger, handler = make_test_logger(__name__)
        handler.formatter = LineProtocolFormatter(resolution=1)
        logger.addFilter(SimulationTimeFilter())
        logger.critical("message", payload)
        _, _, _, timestamp = parse_line_protocol(handler.content)
        handler.clear()
        assert timestamp == 0
        await (time + 10)
        logger.critical("message", payload)
        _, _, _, timestamp = parse_line_protocol(handler.content)
        assert timestamp == 10000000000

    @via_usim
    async def test_explicit(self):
        def record():
            pass

        record.created = pytime()
        filter = SimulationTimeFilter()
        async with Scope() as _:
            filter.filter(record)
        assert record.created == 0


def dummy_statistics():
    return []


class TestMonitoring(object):
    def test_registration(self):
        monitoring = Monitoring()
        statistics = resource_statistics
        monitoring.register_statistic(statistics)
        for element in statistics.whitelist:
            assert statistics in monitoring._statistics.get(element)

    def test_registration_failure(self):
        monitoring = Monitoring()
        statistics = dummy_statistics
        with pytest.raises(AssertionError):
            monitoring.register_statistic(statistics)
        assert all(statistics not in stat for stat in monitoring._statistics.values())
        # define required attributes except whitelist
        statistics.name = "test"
        statistics.logging_formatter = {}
        monitoring.register_statistic(statistics)
        assert all(statistics not in stat for stat in monitoring._statistics.values())
        statistics.whitelist = (str,)
        monitoring.register_statistic(statistics)
        assert all(statistics in stat for stat in monitoring._statistics.values())
