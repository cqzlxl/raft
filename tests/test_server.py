import asyncio
import logging
import threading
import time

from raft.server import Server

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(processName)s.%(threadName)s[%(process)d.%(thread)d]: %(message)s",
        level=logging.DEBUG,
    )
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    class MyServer(threading.Thread):
        def __init__(self, raft):
            super().__init__(daemon=True)
            self.raft = raft

        def run(self):
            asyncio.run(self.raft.serve())

        def stop(self):
            asyncio.run(self.raft.close())

    cluster = {i: ("127.0.0.1", 8080 + i) for i in range(10)}
    servers = [
        MyServer(Server(id, {k: v for k, v in cluster.items() if k != id}, rpc_host=host, rpc_port=port))
        for id, (host, port) in cluster.items()
    ]
    for s in servers:
        s.start()
    time.sleep(5)
    for s in servers:
        s.stop()
        time.sleep(1)
