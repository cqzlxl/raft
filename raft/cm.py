import asyncio
import functools
from enum import Enum


class Roles(Enum):
    leader = "leader"
    follower = "follower"
    candidate = "candidate"


class Log:
    def __init__(self, term, command):
        self.term = term
        self.command = command


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
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        async with self.state_lock:
            return await func(self, *args, **kwargs)

    return wrapper


def try_follow_higher_term(func):
    @functools.wraps(func)
    async def wrapper(self, peer_term, *args, **kwargs):
        if self.state.current_term < peer_term:
            self.as_follower(peer_term)
        return await func(self, peer_term, *args, **kwargs)

    return wrapper


class CM:
    def __init__(self, server):
        self.server = server
        self.server_id = server.id
        self.peers = server.peers.keys()
        self.logger = server.logger
        self.role = Roles.follower
        self.state = State()
        self.state_lock = asyncio.Lock()

    @synchronized
    @try_follow_higher_term
    async def input_vote_request(self, candidate_term, candidate_id, candidate_last_index, candidate_last_term):
        self.logger.debug("peer %s term %d request vote", candidate_id, candidate_term)
        if (
            candidate_term == self.state.current_term
            and self.state.voted_for in (None, candidate_id)
            and not self.log_newer_than(candidate_last_index, candidate_last_term)
        ):
            self.state.voted_for = candidate_id
            self.reset_election_timer()
            granted = True
        else:
            granted = False
        self.logger.debug("granted? %s", granted)
        return self.state.current_term, granted

    @synchronized
    @try_follow_higher_term
    async def input_vote_response(self, vote_term, vote_granted, term, peer):
        self.logger.debug("peer %s term %d vote granted? %s", peer, vote_term, vote_granted)
        if not self.is_candidate():
            self.logger.debug("not candidate now, ignore vote")
        elif vote_term < term:
            self.logger.debug("out of date, ignore vote")
        elif vote_term == term:
            if vote_granted:
                self.accumulate_vote(peer)

    @synchronized
    @try_follow_higher_term
    async def input_append_entries_request(self, leader_term, leader_id, commit_index, prev_index, prev_term, entries):
        self.logger.debug("peer %s term %d request append entries", leader_id, leader_term)
        if -1 < prev_index < len(self.state.log) and self.log_term(prev_index) == prev_term:
            if entries:
                raise NotImplementedError()
            else:
                self.logger.debug("heartbeat request in fact")
                if leader_term < self.state.current_term:
                    success = False
                else:
                    success = True
                    if not self.is_follower():
                        self.as_follower(leader_term)
        else:
            success = False

        return self.state.current_term, success

    @synchronized
    @try_follow_higher_term
    async def input_append_entries_response(self, append_term, append_success, term, peer):
        self.logger.debug("peer %s term %d append entries for term %d OK? %s", peer, append_term, term, append_success)

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
        self.init_peers_indexes()
        self.ask_peers_heartbeats()

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

    def log_newer_than(self, index, term):
        last_index, last_term = self.last_log()
        if last_term < term:
            return False
        elif last_term == term:
            return last_index > index
        else:
            return True

    def last_log(self):
        last_index = len(self.state.log) - 1
        last_term = self.log_term(last_index)
        return last_index, last_term

    def peers_previous_indexes(self):
        indexes = dict()
        for id_, index in self.state.next_index.items():
            prev_index = index - 1
            prev_term = self.log_term(prev_index)
            indexes[id_] = (prev_index, prev_term)
        return indexes

    def init_peers_indexes(self):
        next_index = len(self.state.log)
        for id_ in self.server.peers:
            self.state.match_index[id_] = -1
            self.state.next_index[id_] = next_index

    def log_term(self, index):
        return -1 if index < 0 else self.state.log[index].term

    def reset_election_timer(self):
        self.server.election_timer.reset()

    def reset_heartbeat_timer(self):
        self.server.heartbeat_timer.reset()

    def ask_peers_heartbeats(self):
        previous = self.peers_previous_indexes()
        return self.server.ask_peers_append_entries(self.state.current_term, self.state.commit_index, previous, list())

    def ask_peers_votes(self):
        last_index, last_term = self.last_log()
        return self.server.ask_peers_votes(self.state.current_term, last_index, last_term)

    @property
    def majority(self):
        return (len(self.server.peers) + 1) // 2 + 1
