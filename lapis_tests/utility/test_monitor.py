import ast

from cobald.monitor.format_line import LineProtocolFormatter
from usim import Scope, time, until, eternity

from lapis_tests import via_usim

from . import make_test_logger

from lapis.utility.monitor import TimeFilter


def parse_line_protocol(literal: str):
    name_tags, _, fields_stamp = literal.strip().partition(' ')
    fields, _, stamp = fields_stamp.partition(' ')
    fields = fields.split(',') if fields else []
    name, *tags = name_tags.split(',')
    return name, {
        key: value
        for key, value
        in (tag.split('=') for tag in tags)
    }, {
        key: ast.literal_eval(value)
        for key, value
        in (field.split('=') for field in fields)
    }, None if not stamp else int(stamp)


class TestTimeFilter(object):
    @via_usim
    async def test_simple(self):
        payload = {"a": "a"}
        logger, handler = make_test_logger(__name__)
        handler.formatter = LineProtocolFormatter(resolution=1)
        logger.addFilter(TimeFilter())
        async with Scope() as _:
            logger.critical("message", payload)
        _, _, _, timestamp = parse_line_protocol(handler.content)
        handler.clear()
        assert timestamp == 0
        async with until(time == 10):
            await eternity
        logger.critical("message", payload)
        _, _, _, timestamp = parse_line_protocol(handler.content)
        assert timestamp == 10000000000
