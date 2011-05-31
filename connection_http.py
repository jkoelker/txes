import anyjson

from twisted.internet import defer
from twisted.web import client
from twisted.web import iweb
from zope import interface


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



def connect(server
