import contextlib
import json
import struct

from raft import loggers

from .schema import CODE_CLASS_MAPPING

logs = loggers.get(__name__)


class Transport(contextlib.ContextDecorator):
    code2class = dict((t.value, c) for t, c in CODE_CLASS_MAPPING)
    class2code = dict((c, t.value) for t, c in CODE_CLASS_MAPPING)

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def __enter__(self):
        return self

    def __exit__(self, _, exc_value, __):
        if exc_value is not None:
            logs.error("transfer message", exc_info=exc_value)
        self.writer.close()

    async def read(self):
        code, args = await recv_message(self.reader)
        builder = self.code2class.get(code)
        if builder is None:
            raise Exception("wrong message", code, args)
        else:
            return builder.parse_obj(args)

    async def write(self, message):
        code = self.class2code.get(message.__class__)
        if code is None:
            raise Exception("wrong message", None, message)
        else:
            args = message.dict()
            return await send_message(self.writer, code, args)

    def peer(self):
        return self.writer.get_extra_info("peername")


async def recv_message(stream):
    head = await stream.read(4 + 8)
    type_, size = struct.unpack("!IQ", head)
    body = await stream.read(size)
    args = json.loads(body, encoding="utf8")
    return type_, args


async def send_message(stream, type_, args):
    body = json.dumps(args).encode("utf8")
    size = len(body)
    head = struct.pack("!IQ", type_, size)
    stream.write(head)
    await stream.drain()
    stream.write(body)
    await stream.drain()
    return size
