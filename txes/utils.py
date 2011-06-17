import time
import random

from txes import exceptions


class ServerList(list):
    def __init__(self, servers, retryTime=10):
        list.__init__(self, servers)
        self.dead = []
        self.retryTime = retryTime

    def get(self):
        if self.dead:
            retryTime, server = self.dead.pop()
            if retryTime > time.time():
                self.dead.append((retryTime, server))
            else:
                self.append(server)
        if not self:
            raise exceptions.NoServerAvailable()

        return random.choice(self)

    def markDead(self, server):
        self.remove(server)
        self.dead.insert(0, (time.time() + self.retryTime, server))
