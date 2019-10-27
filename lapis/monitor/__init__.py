import copy
import logging
import logging.handlers
from typing import Callable, TYPE_CHECKING

from cobald.monitor.format_json import JsonFormatter
from usim import time, Queue

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


sampling_required = Queue()


class Monitoring(object):
    def __init__(self):
        self._statistics = {}

    async def run(self):
        async for log_object in sampling_required:
            for statistic in self._statistics.get(type(log_object), set()):
                # do the logging
                for record in statistic(log_object):
                    logging.getLogger(statistic.name).info(
                        statistic.name, record
                    )

    def register_statistic(self, statistic: Callable):
        assert hasattr(statistic, "name") and hasattr(statistic, "logging_formatter")
        try:
            for element in statistic.whitelist:
                self._statistics.setdefault(element, set()).add(statistic)
        except AttributeError:
            logging.getLogger("implementation").warning(
                f"Removing statistic {statistic.name} as no whitelist has been defined."
            )
            return

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
