import asyncio
from enum import Enum
from functools import wraps

from .rpc import AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse


class State(Enum):
    leader = "leader"
    follower = "follower"
    candidate = "candidate"


def synchronized(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        async with self.lock:
            return await func(self, *args, **kwargs)

    return wrapper


class CM:
    def __init__(self, server):
        self.state = State.follower
        self.current_term = 0
        self.voted_for = None
        self.votes_rcv = 0
        self.lock = asyncio.Lock()
        self.server = server

    @property
    def logs(self):
        return self.server.logs

    @property
    def majority(self):
        return (len(self.server.peers) + 1) // 2 + 1

    async def start_election(self):
        self.as_candidate()
        self.accumulate_votes()
        self.server.ask_peers_votes(self.current_term)

    async def heart_beat(self):
        self.server.ask_peers_heartbeats(self.current_term)

    @synchronized
    async def input_vote_request(self, message: VoteRequest):
        t, c = message.term, message.candidate_id
        self.logs.debug("peer %s request vote for term %d", c, t)
        if t > self.current_term:
            self.as_follower(t)
        if t == self.current_term and self.voted_for in (None, c):
            self.reset_election_timer()
            self.voted_for = c
            granted = True
        else:
            granted = False
        self.logs.debug("granted? %s", granted)
        return VoteResponse(term=self.current_term, granted=granted)

    @synchronized
    async def input_vote_response(self, term, peer, message: VoteResponse):
        t, g = message.term, message.granted
        self.logs.debug("peer %s granted term %d? %s", peer, t, g)
        if not self.is_candidate():
            self.logs.debug("not candidate now, ignore vote")
        elif t < term:
            self.logs.debug("out of date, ignore vote")
        elif t == term:
            if g:
                self.accumulate_votes()
            else:
                pass
        else:
            self.as_follower(t)

    @synchronized
    async def input_heartbeat_request(self, message: AppendEntriesRequest):
        t, s = message.term, message.leader_id
        self.logs.debug("peer %s term %d request heartbeat", s, t)
        if t < self.current_term:
            success = False
        elif t == self.current_term:
            success = True
            if not self.is_follower():
                self.as_follower(t)
            else:
                self.reset_election_timer()
        else:
            success = True
            self.as_follower(t)
        return AppendEntriesResponse(term=self.current_term, success=success)

    @synchronized
    async def input_heartbeat_response(self, term, peer, message: AppendEntriesResponse):
        t, s = message.term, message.success
        self.logs.debug("peer %s term %d heartbeat OK? %s", peer, t, s)
        if t > self.current_term:
            self.as_follower(t)

    @synchronized
    async def input_election_timeout(self):
        if not self.is_leader():
            await self.start_election()

    @synchronized
    async def input_heartbeat_timeout(self):
        if self.is_leader():
            await self.heart_beat()

    def accumulate_votes(self):
        self.votes_rcv += 1
        if self.votes_rcv >= self.majority:
            self.as_leader()

    def as_leader(self):
        # current_term no change
        self.voted_for = None
        self.transition(State.leader)

    def as_follower(self, term):
        self.current_term = term
        self.voted_for = None
        self.transition(State.follower)
        self.reset_election_timer()

    def as_candidate(self):
        self.current_term += 1
        self.voted_for = self.server.id
        self.votes_rcv = 0  # accumulate later
        self.transition(State.candidate)
        self.reset_election_timer()

    def transition(self, new):
        old = self.state
        self.state = new
        self.logs.info("state change from %s to %s", old, new)

    def is_leader(self):
        return self.state == State.leader

    def is_follower(self):
        return self.state == State.follower

    def is_candidate(self):
        return self.state == State.candidate

    def reset_election_timer(self):
        self.server.election_timer.reset()
