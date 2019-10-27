import copy
import logging
import logging.handlers
from typing import Callable

from cobald.monitor.format_json import JsonFormatter
from usim import time, Queue


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
    """
    Enable monitoring of a simulation. Objects that change during simulation are
    registered in a queue. Whenever objects in the queue become available, the
    monitoring object takes care to dispatch the object to registered statistic
    callables taking care to generate relevant monitoring output.
    """

    def __init__(self):
        self._statistics = {}

    async def run(self):
        async for log_object in sampling_required:
            for statistic in self._statistics.get(type(log_object), set()):
                # do the logging
                for record in statistic(log_object):
                    logging.getLogger(statistic.name).info(statistic.name, record)

    def register_statistic(self, statistic: Callable) -> None:
        """
        Register a callable that takes an object for logging and generates a list
        of records. The callable should have the following accessible attributes:

        name:
            The identifying name of the statistic for logging
        logging_formatter:
            Pre-defined formatters for the different supported logging formats
            including socket, stream, and telegraf logging.
        whitelist:
            A tuple of objects the statistic callable is interested in to create
            the required logging messages.

        :param statistic: Callable that returns a list of records for logging
        """
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
                new_handler.setFormatter(
                    statistic.logging_formatter.get(
                        type(handler).__name__, JsonFormatter()
                    )
                )
                logger.addHandler(new_handler)
