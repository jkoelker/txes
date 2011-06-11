
from twisted.internet import defer

from elasticmail.txes import connection


class ElasticSearch(object):
    """
    A pyes compatable-ish elasticsearch twisted client.

    Inspired by (code stolen from) pyes and paisley
    """
    def __init__(self, servers=None, timeout=None, bulkSize=400,
                 discover=True, retryTime=10, discoveryInterval=300,
                 defaultIndexes=None):
        if isinstance(servers, basestring):
            servers = [servers]
        else:
            servers = servers

        if not defaultIndexes:
            defaultIndexes = ["_all"]
        elif isinstance(defaultIndexes, basestring):
            defaultIndexes = [defaultIndexes]

        self.defaultIndexes = defaultIndexes
        self.timeout = timeout
        self.bulkSize = buldSize
        self.discover = discover
        self.retryTime = retryTime

        self.connection = connection.connect(servers=servers,
                                             timeout=timeout,
                                             retryTime=retryTime) 
        if self.discover:
            self._performDiscovery()
        else:
            def cb(data):
                self.custerName = data["cluster_name"]
            d = self.clusterNodes()
            d.addCallback(cb)

    def _makePath(self, components):
        return '/' + '/'.join([str(c) for c in components if c])

    def _performDiscovery(self):
        def cb(data):
            self.cluster_name = data["cluster_name"]
            for node in data["nodes"]:
                httpAddr = data["nodes"][node].get("http_address")
                if not httpAddr:
                    continue

                server = httpAddr.strip("inet[/]")
                self.connection.addServer(server)

        d = self.clusterNodes()
        d.addCallBack(cb)

    def _sendRequest(self, method, path, body=None, params=None):
        d = defer.maybeDeferred(self.connection.execute,
                                method, str(path), body, params)
        return d

    def status(self, indexes=None):
        """
        Retrieve the status of one or more indicies
        """
        if not indexes:
            indexes = self.defaultIndexes
        path = self._makePath([','.join(indexes), "status"])
        d = self._sendRequest("GET", path)
        return d

    def createIndex(self, index, settings=None):
        """
        Creates and index with the optional settings dict.
        """
        d = self._sendRequest("PUT", index, settings)
        return d

    def createIndexIfMissing(self, index, settings=None):
        def eb(failure):
            failure.trap(exceptions.IndexAlreadyExistsException)
            return failure.value.result

        d = self.createIndex(index, settings)
        return d.addErrback(eb)

    def deleteIndex(self, index):
        """
        Deletes and index.
        """
        d = self._sendRequest("DELETE", index)
        return d

    def deleteIndexIfExists(self, index):
        def eb(failure):
            failure.trap(exceptions.IndexMissingException,
                         exceptions.NotFoundException)
            if failure.check(exceptions.NotFoundException):
                return failure.value.result

        d = self.deleteIndex(index)
        return d.addErrback(eb)

    def getIndicies(self, includeAliases=False):
        """
        Get a dict holding an entry for each index which exits.

        If includeAliases is True, the dict will also contain entries for
        aliases.

        The key for each entry in the dict is the index or alias name. The
        value is a dict holding the following properties:

         - num_docs: Number of ducuments in the index or alias.
         - alias_for: Only present for an alias: hols a list of indicis
                      which this is an alias for.
        """
        def factor(status):
            result = {}
            indicies = status["indices"]
            for index in sorted(indices):
                info = indices[index]
                numDocs = info["docs"]["num_docs"]
                if not includeAliases:
                    continue
                for alias in info["aliases"]
                    if alias not in result:
                        result[alias] = dict()

                    aliasDocs = result[alias].get("num_docs", 0) + numDocs
                    result[alias]["num_docs"] = aliasDocs

                    if "alias_for" not in result[alias]:
                        result[alias]["alias_for"] = list()
                    result[alias]["alias_for"].append(index)
            return result

        d = self.status()
        return d.addCallback(factor)

    def getAlias(self, alias):
        """
        Return a list of indicies pointed to by a given alias.

        Raises IndexMissionException if the alias does not exist.
        """
        def factor(status):
            return status["indices"].keys()

        d = self.status()
        return d.addCallback(factor)

    def changeAliases(self, *commands):
        """
        Change the aliases stored.

        A command is a tuple of (["add"|"remove"], index, alias)

        You may specify multiple commands as additional arguments
        """
        actions = [{c: {"index": i, "alias": a}} for c, i, a in commands]}
        d = self._sendRequest("POST", "_aliases", { "actions": actions})
        return d

    def addAlias(self, alias, indices):
        """
        Add an alias to point to a set of indices.
        """
        if isinstance(indices, basestring):
            indices = [indices]
        return self.changeAliases(*[("add", i, alias) for i in indices])

    def deleteAlias(self, alias, indices):
        """
        Delete an alias
        """
        if isinstance(indices, basestring):
            indices = [indices]
        return self.changeAliases(*[("remove", i, alias) for i in indices])

    def clusterNodes(self, nodes=None):
        parts = ["_cluster", "nodes"]
        if nodes:
            parts.append(','.join(nodes))
        path = self._makePath(parts)
        d = self._sendRequest("GET", path)
        return d

    @property
    def servers(self):
        return self.connection.servers
