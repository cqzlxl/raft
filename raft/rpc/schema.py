from enum import IntEnum

from pydantic import BaseModel, NonNegativeInt


class AppendEntriesRequest(BaseModel):
    term: NonNegativeInt
    leader_id: NonNegativeInt
    # leader_commit: NonNegativeInt
    # prev_log_index: conint(ge=-1)
    # prev_log_term: conint(ge=-1)
    # entries: List[Any] = list()


class AppendEntriesResponse(BaseModel):
    term: NonNegativeInt
    success: bool


class VoteRequest(BaseModel):
    term: NonNegativeInt
    candidate_id: NonNegativeInt
    # last_log_index: conint(ge=-1)
    # last_log_term: conint(ge=-1)


class VoteResponse(BaseModel):
    term: NonNegativeInt
    vote_granted: bool


class MessageTypes(IntEnum):
    vote_request = 10
    vote_response = 11
    append_entries_request = 20
    append_entries_response = 21


CODE_CLASS_MAPPING = (
    (MessageTypes.vote_request, VoteRequest),
    (MessageTypes.vote_response, VoteResponse),
    (MessageTypes.append_entries_request, AppendEntriesRequest),
    (MessageTypes.append_entries_response, AppendEntriesResponse),
)
