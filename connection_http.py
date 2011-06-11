import codecs
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO
import urllib

import anyjson

from twisted.internet import defer, reactor, protocol
from twisted.web import client
from twisted.web import iweb
from twisted.web import http
from zope import interface

from elasticmail.txes import connection


DEFAULT_SERVER = "127.0.0.1:9200"


class JSONProducer(object):
    interface.implements(iweb.IBodyProducer)

    def __init__(self, body):
        self.body = anyjson.serialize(body)
        self.length = len(self.body)

    def startProducing(self, consumer):
        return defer.maybeDeferred(consumer.write, self.body)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class JSONReceiver(protocol.Protocol):
    def __init__(self, deferred):
        self.deferred = deferred
        self.writter = codecs.getwritter("utf_8")(StringIO.StringIO())

    def dataReceived(self, bytes):
        self.writter.write(bytes)

    def connectionLost(self, reason):
        if reason.check(client.ResponseDone, http.PotentialDataloss):
            data = anyjson.deserialize(self.writter.getvalue())
            self.deferred.callback(data)
        else:
            self.deffered.errback(reason)


class HTTPConnection(object):
    interface.implements(connection.IConnection)

    def addServer(self, server):
        if server not in self.servers:
            self.servers.append(server)

    def getAgent(self):
        try:
            return self.client
        except AttributeError:
            self.client = client.Agent(reactor)
            return self.client

    def connect(self, servers=None, timeout=None, retryTime=10,
                *args, **kwargs):
        if not servers:
            servers = [DEFAULT_SERVER]
        elif isinstance(server, (str, unicode)):
            servers = [servers]
        self.servers = connection.ServerList(servers, retryTime=retryTime)
        self.agents = {}

    def close(self):
        pass

    def execute(self, method, path, body=None, params=None):
        def parse_response(response):
            d = defer.Deferred()
            reponse.deliverBody(JSONReceiver(d))
            return d

        agent = self.getAgent()
        server = self.servers.get()
        url = server + path

        if params:
            url = url + '?' + urllib.urlencode(params)

        if body:
            body = JsonProducer(body)

        if not url.startswith("http://"):
            url = "http://" + url

        d = agent.request(method, url, bodyProducer=body)
        d.addCallback(parse_response)
        return d
