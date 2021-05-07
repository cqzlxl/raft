import asyncio
from enum import Enum
from functools import wraps


class Roles(Enum):
    leader = "leader"
    follower = "follower"
    candidate = "candidate"


class State:
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.log = list()

        self.commit_index = -1
        self.last_applied = -1

        self.next_index = dict()
        self.match_index = dict()

        self.votes_rcv = set()


def synchronized(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        async with self.state_lock:
            return await func(self, *args, **kwargs)

    return wrapper


class CM:
    def __init__(self, server):
        self.server = server
        self.server_id = server.id
        self.logger = server.logger
        self.role = Roles.follower
        self.state = State()
        self.state_lock = asyncio.Lock()

    @synchronized
    async def input_vote_request(self, candidate_term, candidate_id):
        self.logger.debug("peer %s term %d request vote", candidate_id, candidate_term)
        if candidate_term > self.state.current_term:
            self.as_follower(candidate_term)
        if candidate_term == self.state.current_term and self.state.voted_for in (None, candidate_id):
            self.state.voted_for = candidate_id
            self.reset_election_timer()
            granted = True
        else:
            granted = False
        self.logger.debug("granted? %s", granted)
        return self.state.current_term, granted

    @synchronized
    async def input_vote_response(self, term, peer, vote_term, vote_granted):
        self.logger.debug("peer %s term %d vote granted? %s", peer, vote_term, vote_granted)
        if not self.is_candidate():
            self.logger.debug("not candidate now, ignore vote")
        elif vote_term < term:
            self.logger.debug("out of date, ignore vote")
        elif vote_term == term:
            if vote_granted:
                self.accumulate_vote(peer)
            else:
                pass
        else:
            self.as_follower(vote_term)

    @synchronized
    async def input_append_entries_request(self, leader_term, leader_id):
        self.logger.debug("peer %s term %d request heartbeat", leader_id, leader_term)
        if leader_term < self.state.current_term:
            success = False
        elif leader_term == self.state.current_term:
            success = True
            if not self.is_follower():
                self.as_follower(leader_term)
            else:
                self.reset_election_timer()
        else:
            success = True
            self.as_follower(leader_term)
        return self.state.current_term, success

    @synchronized
    async def input_append_entries_response(self, term, peer, append_term, append_success):
        self.logger.debug("peer %s term %d heartbeat OK? %s", peer, append_term, append_success)
        if append_term > self.state.current_term:
            self.as_follower(append_term)

    @synchronized
    async def input_election_timeout(self):
        if not self.is_leader():
            self.start_election()

    @synchronized
    async def input_heartbeat_timeout(self):
        if self.is_leader():
            self.ask_peers_heartbeats()

    def start_election(self):
        self.as_candidate()
        self.accumulate_vote(self.server_id)
        self.ask_peers_votes()

    def accumulate_vote(self, server_id):
        self.state.votes_rcv.add(server_id)
        if len(self.state.votes_rcv) >= self.majority:
            self.as_leader()

    def as_leader(self):
        # current_term no change
        self.state.voted_for = None
        self.transition(Roles.leader)

    def as_follower(self, term):
        self.state.current_term = term
        self.state.voted_for = None
        self.transition(Roles.follower)
        self.reset_election_timer()

    def as_candidate(self):
        self.state.current_term += 1
        self.state.voted_for = self.server_id
        self.state.votes_rcv = set()  # accumulate later
        self.transition(Roles.candidate)
        self.reset_election_timer()

    def transition(self, new):
        old = self.role
        self.role = new
        self.logger.info("role changed from %s to %s", old.name, new.name)

    def is_leader(self):
        return self.role == Roles.leader

    def is_follower(self):
        return self.role == Roles.follower

    def is_candidate(self):
        return self.role == Roles.candidate

    def reset_election_timer(self):
        self.server.election_timer.reset()

    def reset_heartbeat_timer(self):
        self.server.heartbeat_timer.reset()

    def ask_peers_heartbeats(self):
        return self.server.ask_peers_heartbeats(self.state.current_term)

    def ask_peers_votes(self):
        return self.server.ask_peers_votes(self.state.current_term)

    @property
    def majority(self):
        return (len(self.server.peers) + 1) // 2 + 1
