from usim import Pipe, instant
from usim._primitives.notification import Notification
from typing import Optional


class MonitoredPipe(Pipe):
    def __init__(self, throughput: float):
        super().__init__(throughput)
        self._monitor = Notification()
        self.storage = None
        self.transferred_data = 0

    async def load(self):
        """
        Monitor any changes of the throughput load of the pipe
        .. code:: python3
            async def report_load(pipe: MonitoredPipe):
                async for throughput in pipe.load():
                    print(f'{time.now:6.0f}: {throughput} \t [{throughput / pipe.throughput * 100:03.0f}%]')
        .. note::
            Currently only works for loads exceeding 100%.
        """
        await instant
        yield sum(self._subscriptions.values())
        while True:
            await self._monitor
            yield sum(self._subscriptions.values())

    def _throttle_subscribers(self):
        self._monitor.__awake_all__()
        super()._throttle_subscribers()

    async def transfer(self, total: float, throughput: Optional[float] = None) -> None:
        await super().transfer(total, throughput)
        self.transferred_data += total

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.storage or id(self))


if __name__ == "__main__":
    from usim import time, run, Scope

    async def report_load(pipe: MonitoredPipe):
        async for throughput in pipe.load():
            print(
                f"{time.now:6.0f}: {throughput} \t [{throughput / pipe.throughput * 100:03.0f}%]"
            )

    async def perform_load(pipe: MonitoredPipe, delay, amount):
        await (time + delay)
        await pipe.transfer(amount, pipe.throughput / 2)

    async def main():
        pipe = MonitoredPipe(128)
        async with Scope() as scope:
            scope.do(report_load(pipe), volatile=True)
            scope.do(perform_load(pipe, 0, 512))
            scope.do(perform_load(pipe, 4, 1024))
            scope.do(perform_load(pipe, 6, 128))
            scope.do(perform_load(pipe, 12, 1024))

    run(main())
