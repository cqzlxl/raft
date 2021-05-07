import asyncio
import contextlib

from raft import loggers

from .schema import AppendEntriesRequest, VoteRequest
from .transport import Transport

logs = loggers.get(__name__)


class Client:
    def __init__(self, server_id, server_host, server_port):
        self.server_host = server_host
        self.server_port = server_port
        self.server_id = server_id

    async def ask_peer_vote(self, term, candidate_id):
        async with self.connect() as transport:
            request = VoteRequest(term=term, candidate_id=candidate_id)
            await transport.write(request)
            response = await transport.read()
            return response

    async def ask_peer_heartbeat(self, term, leader_id):
        async with self.connect() as transport:
            request = AppendEntriesRequest(term=term, leader_id=leader_id)
            await transport.write(request)
            response = await transport.read()
            return response

    @contextlib.asynccontextmanager
    async def connect(self):
        host, port = self.server_host, self.server_port
        reader, writer = await asyncio.open_connection(host, port)
        with Transport(reader, writer) as transport:
            yield transport
