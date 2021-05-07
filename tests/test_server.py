import asyncio
import threading
import time

from raft.server import Server


class MyServer(threading.Thread):
    def __init__(self, raft):
        super().__init__(daemon=True)
        self.raft = raft

    def run(self):
        asyncio.run(self.raft.run())

    def stop(self):
        asyncio.run(self.raft.shutdown())


if __name__ == "__main__":
    cluster = {i: ("127.0.0.1", 8080 + i) for i in range(10)}
    servers = [
        MyServer(Server(i, {k: v for k, v in cluster.items() if k != i}, rpc_host=host, rpc_port=port))
        for i, (host, port) in cluster.items()
    ]
    for s in servers:
        s.start()
    time.sleep(30)
    for s in servers:
        s.stop()
        time.sleep(1)
