import asyncio
from asyncio import CancelledError

from raft import loggers

from .schema import AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse
from .transport import Transport

logs = loggers.get(__name__)


class Server:
    def __init__(self):
        self.runtime = None

    async def stop(self):
        await self.runtime.__aexit__()

    async def serve(self, host, port):
        try:
            self.runtime = await asyncio.start_server(self.handle, host, port)
            logs.debug("started rpc server @%s:%d", *self.runtime.sockets[0].getsockname())
            async with self.runtime:
                await self.runtime.serve_forever()
        except CancelledError as e:
            logs.info("stopped rpc server", exc_info=e)

    async def handle(self, reader, writer):
        with Transport(reader, writer) as transport:
            message = await transport.read()
            if isinstance(message, VoteRequest):
                term, vote_granted = await self.handle_vote_request(message)
                response = VoteResponse(term=term, vote_granted=vote_granted)
            elif isinstance(message, AppendEntriesRequest):
                term, success = await self.handle_append_entries_request(message)
                response = AppendEntriesResponse(term=term, success=success)
            else:
                raise Exception("can not handle this type", type)
            await transport.write(response)

    async def handle_vote_request(self, message):
        raise NotImplementedError()

    async def handle_append_entries_request(self, message):
        raise NotImplementedError()
