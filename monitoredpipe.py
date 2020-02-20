from typing import NamedTuple, Optional, Deque, Any, Dict, AsyncIterable

from usim import Pipe, instant, time
from collections import deque
from usim._primitives.notification import Notification


class MonitoredPipeInfo(NamedTuple):
    requested_throughput: float
    available_throughput: float
    pipename: Optional[str]
    throughputscale: float
    no_subscriptions: int


class MonitoredPipe(Pipe):
    def __init__(self, throughput: float):
        super().__init__(throughput)
        self._monitor = Notification()
        self._monitor_buffers: Dict[Any, Deque[MonitoredPipeInfo]] = {}
        self.storage = None
        self.transferred_data = 0

    async def load(self) -> AsyncIterable[MonitoredPipeInfo]:
        """
        Monitor any changes of the throughput load of the pipe
        .. code:: python3
            async def report_load(pipe: MonitoredPipe):
                async for event in pipe.load():
                    print(
                        f'{time.now:6.0f}:'
                        f'{event.requested_throughput} \t'
                        f'[{event.requested_throughput / event.available_throughput * 100:03.0f}%]'
                    )
        """
        await instant
        yield self._sample_state()
        sentinel = object()
        self._monitor_buffers[
            sentinel
        ] = buffer = deque()  # type: Deque[MonitoredPipeInfo]
        try:
            while True:
                while buffer:
                    yield buffer.popleft()
                await self._monitor
        finally:
            del self._monitor_buffers[sentinel]

    def _throttle_subscribers(self):
        print(time.now, "awakening monitors, throttling subscribers")

        self._monitor.__awake_all__()
        super()._throttle_subscribers()
        data = self._sample_state()
        for buffer in self._monitor_buffers.values():
            print(buffer)
            buffer.append(data)

    def _sample_state(self):
        return MonitoredPipeInfo(
            sum(self._subscriptions.values()),
            self.throughput,
            repr(self),
            self._throughput_scale,
            len(self._subscriptions),
        )

    async def transfer(self, total: float, throughput: Optional[float] = None) -> None:
        await super().transfer(total, throughput)
        self.transferred_data += total

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.storage or id(self))


if __name__ == "__main__":
    from usim import run, Scope

    async def report_load(pipe: MonitoredPipe):
        async for event in pipe.load():
            print(
                f"{time.now:6.0f}:"
                f"{event.requested_throughput} \t"
                f"[{event.requested_throughput / event.available_throughput * 100:03.0f}%]"
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
