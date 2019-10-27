import io
import logging
import threading


class CapturingHandler(logging.StreamHandler):
    @property
    def content(self) -> str:
        return self.stream.getvalue()

    def __init__(self):
        super().__init__(stream=io.StringIO())

    def clear(self):
        self.stream.truncate(0)
        self.stream.seek(0)


_test_index = 0
_index_lock = threading.Lock()


def make_test_logger(base_name: str = "test_logger"):
    with _index_lock:
        global _test_index
        log_name = base_name + ".test%d" % _test_index
        _test_index += 1
    logger = logging.getLogger(log_name)
    logger.propagate = False
    handler = CapturingHandler()
    logger.handlers = [handler]
    return logger, handler
