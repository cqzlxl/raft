import asyncio
import logging
from asyncio import CancelledError
from contextlib import asynccontextmanager

from .cm import CM
from .logger import LoggerAdapter
from .rpc import AppendEntriesRequest, MessageType, VoteRequest, recv_message, send_message
from .timer import ElectionTimer, HeartbeatTimer

logger = logging.getLogger(__name__)


class Server:
    def __init__(
        self,
        id,
        peers=None,
        ask_peer_timeout=500,
        heartbeat_interval=50,
        election_timeout_min=150,
        election_timeout_max=300,
        rpc_host="127.0.0.1",
        rpc_port=8080,
    ):
        self.id = id
        self.peers = peers or dict()
        self.ask_peer_timeout = ask_peer_timeout
        self.cm = CM(self)
        self.heartbeat_timer = HeartbeatTimer(heartbeat_interval, self.cm.input_heartbeat_timeout)
        self.election_timer = ElectionTimer(election_timeout_min, election_timeout_max, self.cm.input_election_timeout)
        self.rpc_host = rpc_host
        self.rpc_port = rpc_port
        self.rpc_server = None
        self.logs = LoggerAdapter(logger, self)

    def ask_peers_votes(self, term):
        for peer in self.peers:
            asyncio.create_task(self.ask_peer_with_timeout(self.ask_peer_vote(peer, term)))

    async def ask_peer_vote(self, peer_id, term):
        async with self.connect_peer(peer_id) as (reader, writer):
            request = VoteRequest(term=term, candidate_id=self.id)
            await send_message(writer, request)
            self.logs.debug("send vote request to %s", peer_id)
            response, _ = await recv_message(reader)
            self.logs.debug("recv vote response from %s", peer_id)
            await self.cm.input_vote_response(term, peer_id, response)

    def ask_peers_heartbeats(self, term):
        for peer in self.peers:
            asyncio.create_task(self.ask_peer_with_timeout(self.ask_peer_heartbeat(peer, term)))

    async def ask_peer_heartbeat(self, peer_id, term):
        async with self.connect_peer(peer_id) as (reader, writer):
            request = AppendEntriesRequest(term=term, leader_id=self.id)
            await send_message(writer, request)
            self.logs.debug("send heartbeat request to server %s", peer_id)
            response, _ = await recv_message(reader)
            self.logs.debug("recv heartbeat response from server %s", peer_id)
            await self.cm.input_heartbeat_response(term, peer_id, response)

    async def ask_peer_with_timeout(self, future):
        try:
            return await asyncio.wait_for(future, self.ask_peer_timeout)
        except Exception as e:
            self.logs.error("ask peer", exc_info=e)

    @asynccontextmanager
    async def connect_peer(self, peer_id):
        host, port = self.peers[peer_id]
        reader, writer = await asyncio.open_connection(host, port)
        self.logs.debug("connected to peer %s(%s:%d)", peer_id, host, port)
        try:
            yield reader, writer
        finally:
            writer.close()

    async def close(self):
        self.election_timer.stop()
        self.heartbeat_timer.stop()
        await self.rpc_server.__aexit__()

    async def serve(self):
        try:
            asyncio.create_task(self.election_timer.start())
            self.logs.debug("started election timer")
            asyncio.create_task(self.heartbeat_timer.start())
            self.logs.debug("started heartbeat timer")
            self.rpc_server = await asyncio.start_server(self.rpc_handler, self.rpc_host, self.rpc_port)
            self.logs.debug("started rpc server @%s:%d", *self.rpc_server.sockets[0].getsockname())
            async with self.rpc_server:
                await self.rpc_server.serve_forever()
        except CancelledError as e:
            self.logs.info("service closed", exc_info=e)

    async def rpc_handler(self, reader, writer):
        peer = writer.get_extra_info("peername")
        self.logs.debug("rpc request from %s:%d", *peer)
        message, type = await recv_message(reader)
        self.logs.debug("message type %d(%s)", type, message.__class__.__name__)
        try:
            if type == MessageType.vote_request:
                response = await self.cm.input_vote_request(message)
            elif type == MessageType.append_entries_request:
                response = await self.cm.input_heartbeat_request(message)
            else:
                raise Exception("can not handle this type", type)
            await send_message(writer, response)
        except Exception as e:
            self.logs.error("handle message %s", message, exc_info=e)
        finally:
            writer.close()
