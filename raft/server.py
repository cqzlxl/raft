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

    async def handle_vote_request(self, params):
        return await self.cm.input_vote_request(
            params.term,
            params.candidate_id,
            params.last_log_index,
            params.last_log_term,
        )

    async def handle_append_entries_request(self, params):
        return await self.cm.input_append_entries_request(
            params.term,
            params.leader_id,
            params.leader_commit,
            params.prev_log_index,
            params.prev_log_term,
            params.entries,
        )

    def ask_peers_votes(self, term, last_index, last_term):
        for peer in self.peers:
            self.ask_peer(self.ask_peer_vote(peer, term, last_index, last_term))

    async def ask_peer_vote(self, peer_id, term, last_index, last_term):
        client = self.rpc_clients[peer_id]
        response = await client.ask_peer_vote(term, self.id, last_index, last_term)
        await self.cm.input_vote_response(response.term, response.vote_granted, term, peer_id)

    def ask_peers_append_entries(self, term, commit_index, previous, entries):
        for peer in self.peers:
            self.ask_peer(self.ask_peer_append_entries(peer, term, commit_index, previous, entries))

    async def ask_peer_append_entries(self, peer_id, term, commit_index, previous, entries):
        prev_index, prev_term = previous[peer_id]
        client = self.rpc_clients[peer_id]
        response = await client.ask_peer_append_entries(term, self.id, commit_index, prev_index, prev_term, entries)
        await self.cm.input_append_entries_response(response.term, response.success, term, peer_id)

    def ask_peer(self, action):
        asyncio.create_task(self.ask_peer_with_timeout(action))

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
