import asyncio
from random import randint


class Timer:
    def __init__(self):
        self.sleeper = None
        self.started = False

    def stop(self):
        self.started = False
        self.reset()

    def reset(self):
        if self.sleeper:
            self.sleeper.cancel()

    async def start(self):
        self.started = True
        while self.started:
            self.sleeper = asyncio.ensure_future(self.sleep())
            try:
                await self.sleeper
                await self.callback()
            except asyncio.CancelledError:
                pass

    async def sleep(self):
        pass

    async def callback(self):
        pass


class ElectionTimer(Timer):
    def __init__(self, timeout_min, timeout_max, callback):
        super().__init__()
        self.timeout_min = timeout_min
        self.timeout_max = timeout_max
        self.callback = callback

    async def sleep(self):
        seconds = randint(self.timeout_min, self.timeout_max) / 1000
        await asyncio.sleep(seconds)


class HeartbeatTimer(Timer):
    def __init__(self, timeout, callback):
        super().__init__()
        self.timeout = timeout
        self.callback = callback

    async def sleep(self):
        await asyncio.sleep(self.timeout / 1000)
