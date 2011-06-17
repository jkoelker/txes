from zope import interface


class IConnection(interface.Interface):
    def addServer(server):
        """
        Add a single server to the server pool
        """

    def connect(servers=None, timeout=None, retryTime=10,
                *args, **kwargs):
        """
        connect to elasticsearch
        """

    def close():
        """
        close all connections to elasticsearch
        """

    def execute(method, path, body=None, params=None):
        """
        Perform method on path with optional body
        """
