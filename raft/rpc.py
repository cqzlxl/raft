from struct import pack, unpack

from pydantic import BaseModel


class MessageType:
    vote_request = 20
    vote_response = 21
    append_entries_request = 10
    append_entries_response = 11


class Message(BaseModel):
    term: int

    @staticmethod
    def loads(type: MessageType, data: bytes):
        if type == MessageType.vote_request:
            builder = VoteRequest
        elif type == MessageType.vote_response:
            builder = VoteResponse
        elif type == MessageType.append_entries_request:
            builder = AppendEntriesRequest
        elif type == MessageType.append_entries_response:
            builder = AppendEntriesResponse
        else:
            raise Exception(f"unknown message type: {type}")
        message = builder.parse_raw(data, encoding="utf8")
        return message

    @staticmethod
    def dumps(message):
        if isinstance(message, VoteRequest):
            type = MessageType.vote_request
        elif isinstance(message, VoteResponse):
            type = MessageType.vote_response
        elif isinstance(message, AppendEntriesRequest):
            type = MessageType.append_entries_request
        elif isinstance(message, AppendEntriesResponse):
            type = MessageType.append_entries_response
        else:
            raise Exception(f"unknown message type: {message.__class__.__qualname__}")
        data = message.json().encode("utf8")
        return type, data


class AppendEntriesRequest(Message):
    leader_id: str


class AppendEntriesResponse(Message):
    success: bool


class VoteRequest(Message):
    candidate_id: str


class VoteResponse(Message):
    granted: bool


async def recv_message(stream):
    head = await stream.read(4 + 8)
    type, size = unpack("!IQ", head)
    body = await stream.read(size)
    message = Message.loads(type, body)
    return message, type


async def send_message(stream, message: Message):
    type, body = Message.dumps(message)
    size = len(body)
    head = pack("!IQ", type, size)
    stream.write(head)
    await stream.drain()
    stream.write(body)
    await stream.drain()
    return size
