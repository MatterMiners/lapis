import copy
import logging
import logging.handlers
from typing import Callable, TYPE_CHECKING

from cobald.monitor.format_json import JsonFormatter
from usim import time, Flag, each

if TYPE_CHECKING:
    from lapis.simulator import Simulator


class LoggingSocketHandler(logging.handlers.SocketHandler):
    def makePickle(self, record):
        return self.format(record).encode()


class LoggingUDPSocketHandler(logging.handlers.DatagramHandler):
    def makePickle(self, record):
        return self.format(record).encode()


class SimulationTimeFilter(logging.Filter):
    """
    Dummy filter to replace log record timestamp with simulation time.
    """
    def filter(self, record) -> bool:
        record.created = time.now
        return True


sampling_required = Flag()


class Monitoring(object):
    def __init__(self, simulator: "Simulator"):
        self.simulator = simulator
        self._statistics = []

    async def run(self):
        async for _ in each(delay=1):
            await sampling_required
            await sampling_required.set(False)
            for statistic in self._statistics:
                # do the logging
                for record in statistic(self.simulator):
                    logging.getLogger(statistic.name).info(
                        statistic.name, record
                    )

    def register_statistic(self, statistic: Callable):
        assert hasattr(statistic, "name") and hasattr(statistic, "logging_formatter")
        self._statistics.append(statistic)

        # prepare the logger
        logger = logging.getLogger(statistic.name)
        if not logger.handlers:
            logger.addFilter(SimulationTimeFilter())
            logger.propagate = False
            # append handlers of default logger and add required formatters
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                new_handler = copy.copy(handler)
                new_handler.setFormatter(statistic.logging_formatter.get(
                    type(handler).__name__, JsonFormatter()))
                logger.addHandler(new_handler)
