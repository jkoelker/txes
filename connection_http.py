import anyjson

from twisted.internet import defer
from twisted.web import client
from twisted.web import iweb
from zope import interface

from elasticmail.txes import connection


DEFAULT_SERVER = "127.0.0.1:9200"


class StringProducer(object):
    interface.implements(iweb.IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class JSONProducer(StringProducer):
    def __init__(self, data):
        StringProducer.__init__(self, anyjson.serialize(data))


class HTTPConnection(object):
    interface.implements(connection.IConnection)

    def getAgent(self):
        server = self.server.get()


    def connect(self, servers=None, timeout=None, discover=True,
                retryTime=10, *args, **kwargs):
        if not servers:
            servers = [DEFAULT_SERVER]
        elif isinstance(server, (str, unicode)):
            servers = [servers]
        self.servers = connection.ServerList(servers, retryTime=retryTime)
        self.agents = {}

    def close(self):
        pass

    def execute(self, method, path, body):
        pass

