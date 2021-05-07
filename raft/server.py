import asyncio
from asyncio import CancelledError

from . import loggers, rpc
from .cm import CM
from .loggers import Adapter
from .timer import ElectionTimer, HeartbeatTimer

logs = loggers.get(__name__)


class Server(rpc.Server):
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
        self.rpc_serve_host = rpc_host
        self.rpc_serve_port = rpc_port
        self.rpc_clients = {k: rpc.Client(k, *v) for k, v in self.peers.items()}
        self.ask_peer_timeout = ask_peer_timeout
        self.logger = Adapter(logs, self)
        self.cm = CM(self)
        self.heartbeat_timer = HeartbeatTimer(heartbeat_interval, self.cm.input_heartbeat_timeout)
        self.election_timer = ElectionTimer(election_timeout_min, election_timeout_max, self.cm.input_election_timeout)

    async def handle_vote_request(self, term, candidate_id):
        return await self.cm.input_vote_request(term, candidate_id)

    async def handle_append_entries_request(self, term, leader_id):
        return await self.cm.input_append_entries_request(term, leader_id)

    def ask_peers_votes(self, term):
        for peer in self.peers:
            asyncio.create_task(self.ask_peer_with_timeout(self.ask_peer_vote(peer, term)))

    async def ask_peer_vote(self, peer_id, term):
        client = self.rpc_clients[peer_id]
        response = await client.ask_peer_vote(term, self.id)
        await self.cm.input_vote_response(term, peer_id, response.term, response.vote_granted)

    def ask_peers_heartbeats(self, term):
        for peer in self.peers:
            asyncio.create_task(self.ask_peer_with_timeout(self.ask_peer_heartbeat(peer, term)))

    async def ask_peer_heartbeat(self, peer_id, term):
        client = self.rpc_clients[peer_id]
        response = await client.ask_peer_heartbeat(term, self.id)
        await self.cm.input_append_entries_response(term, peer_id, response.term, response.success)

    async def ask_peer_with_timeout(self, future):
        try:
            return await asyncio.wait_for(future, self.ask_peer_timeout)
        except Exception as e:
            self.logger.error("ask peer timed out", exc_info=e)

    async def shutdown(self):
        self.election_timer.stop()
        self.heartbeat_timer.stop()
        await super().stop()

    async def run(self):
        try:
            asyncio.create_task(self.election_timer.start())
            self.logger.debug("started election timer")
            asyncio.create_task(self.heartbeat_timer.start())
            self.logger.debug("started heartbeat timer")
            await super().serve(self.rpc_serve_host, self.rpc_serve_port)
        except CancelledError as e:
            self.logger.info("service closed", exc_info=e)
