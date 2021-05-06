import logging


class Adapter(logging.LoggerAdapter):
    def __init__(self, logger, server):
        super().__init__(logger, dict())
        self.server = server

    def process(self, msg, kwargs):
        id = self.server.id
        cm = self.server.cm
        extra = dict(server_id=id, current_term=cm.current_term, state=cm.state.name, voted_for=cm.voted_for)
        msg_with_extra = f"ST{extra} {msg}"
        return msg_with_extra, kwargs
