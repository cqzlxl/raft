import logging


class Adapter(logging.LoggerAdapter):
    def __init__(self, logger, server):
        super().__init__(logger, dict())
        self.server = server

    def process(self, msg, kwargs):
        id = self.server.id
        cm = self.server.cm
        extra = dict(server_id=id, role=cm.role.name, current_term=cm.state.current_term, voted_for=cm.state.voted_for)
        msg_with_extra = f"State{extra} {msg}"
        return msg_with_extra, kwargs


def get(name=None):
    return logging.getLogger(name)


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(processName)s.%(threadName)s[%(process)d.%(thread)d]: %(message)s",
    level=logging.DEBUG,
)
logging.getLogger("asyncio").setLevel(logging.DEBUG)
