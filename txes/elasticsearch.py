
from twisted.internet import defer, reactor

from txes import connection


class ElasticSearch(object):
    """
    A pyes compatable-ish elasticsearch twisted client.

    Inspired by (code stolen from) pyes and paisley
    """
    def __init__(self, servers=None, timeout=None, bulkSize=400,
                 discover=True, retryTime=10, discoveryInterval=300,
                 defaultIndexes=None, autorefresh=False):
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

        self.autorefresh = autorefresh
        self.refeshed = True

        self.info = {}

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

    def _validateIndexes(self, indexes=None):
        indexes = indexes or self.defaultIndexes
        if isinstance(indexes, basestring):
            return [indexes]
        return indexes

    def status(self, indexes=None):
        """
        Retrieve the status of one or more indicies
        """
        indexes = self._validateIndexes(indexes)
        path = self._makePath([','.join(indexes), "_status"])
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

        d = self.status(alias)
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

    def setAlias(self, alias, indices):
        """
        Set and alias (possibly removing what it already points to)
        """
        def eb(failure):
            failure.trap(exceptions.IndexMissingException)
            return self.addAlias(alias, indices)

        def factor(old_indices):
            commands = [["remove", i, alias] for i in old_indices]
            commands.extend([["add", i, alias] for i in indices])
            if len(commands):
                return self.changeAliases(*commands)

        if isinstance(indices, basestring):
            indices = [indices]

        d = self.getAlias(alias)
        d.addCallbacks(factor, eb)
        return d

    def closeIndex(self, index):
        """
        Close an index.
        """
        d = self._sendRequest("POST", "/%s/_close" % index)
        return d

    def openIndex(self, index):
        """
        Open an index.
        """
        d = self._sendRequest("POST", "/%s/_open" % index)
        return d

    def flush(self, indexes=None, refresh=None):
        self.forceBulk()
        indexes = self._validateIndexes(indexes)
        path = self._makePath([','.join(indexes), "_flush"])
        params = None
        if refresh:
            params["refresh"] = True
        d = self._sendRequest("POST", path, params=params)
        return d

    def refresh(self, indexes=None, timesleep=1):
        def wait(results):
            d = self.cluster_health(wait_for_status="green")
            d.addCallback(lambda _: results)
            self.refreshed = True
            return d

        def delay(results):
            d = defer.Deferred()
            reactor.callLater(timesleep, d.callback, results)
            d.addCallback(wait)
            return d

        self.forceBulk()
        indexes = self._validateIndexes(indexes)
        path = self._makePath([','.join(indexes), "_refresh"])
        d = self._sendRequest("POST", path)
        d.addCallback(delay)
        return d

    def optimize(self, indexes=None, waitForMerge=False,
                 maxNumSegement=None, onlyExpungeDeletes=False,
                 refresh=True, flush=True):
        """
        Optimize one or more indices.
        """
        def done(results):
            self.refreshed = True
            return results

        indexes = self._validateIndexes(indexes)
        path = self._make_path([','.join(indexes), "_optimize"])
        params = {"wait_for_merge": waitForMerge,
                  "only_expunge_deletes": onlyExpungeDeletes,
                  "refesh": refresh,
                  "flush": flush}
        if maxNumSegments:
            params["max_num_segments"] = maxNumSegement
        d = self._sendRequest("POST", path, params=params)
        d.addCallback(done)
        return d

    def analyze(self, text, index=None, analyzer=None):
        """
        Perfoms the analysis process on a text and returns the tokens
        breakdown of the text
        """
        if analyzer:
            analyzer = {"analyzer": analyzer}

        body = {"text": text}
        path = self._makePaht([index, "_analyze"])
        d = self._sendRequest("POST", path, body=body, params=analyzer)
        return d

    def gatewaySnapshot(self, indexes=None):
        """
        Gateway shapshot one or more indices
        """
        indexes = self._validateIndexes(indexes)
        path = self.makePath([','.join(indexes), "_gateway", "snapshot"])
        d = self.sendRequest("POST", path)
        return d

    def putMapping(self, docType, mapping, indexes=None):
        """
        Register specific mapping definition for a specific type against
        one or more indices.
        """
        indexes = self._validateIndexes(indexes)
        path = self._makePath([','.join(indexes), docType, "_mapping"])
        if docType not in mapping:
            mapping = {docType: mapping}
        self.refreshed = False
        d = self._sendRequest("PUT", path, body=mapping)
        return d

    def getMapping(self, docType=None, indexes=None):
        """
        Get the mapping definition
        """
        indexes = self._validateIndexes(indexes)
        path = [','.join(indexes)]

        if docType:
            path.append(docType)

        path.append("_mapping")
        d = self._sendRequest("GET", path)
        return d

    def collectInfo(self):
        """
        Collect info about the connection and fill the info dicionary
        """
        def factor(result):
            self.info = {}
            self.info['server'] = {}
            self.info['server']['name'] = res['name']
            self.info['server']['version'] = res['version']
            self.info['allinfo'] = res
            self.info['status'] = self.status(["_all"])
            return self.info

        d = self._sendRequest("GET", '/')
        d.addCallback(factor)
        return d

    def clusterHealth(self, indexes=None, level="cluster",
                      waitForStatus=None, waitForRelocatingShards=None,
                      waitForNodes=None, timeout=30):
        """
        Check the current cluster health
        """

        path = self._mapPath(["_cluster", "health"])
        if level not in ("cluster", "indices", "shards"):
            raise ValueError("Invalid level: %s" % level)

        mapping = {"level": level}

        if waitForStatus:
            if waitForStatus not in ("green", "yellow", "red"):
                raise ValueError("Invalid waitForStatus: %s" % waitForStatus)
            mapping["wait_for_status"] = waitForStatus
        
        if waitForRelocatingShard:
            mapping["wait_for_relocating_shards"] = waitForRelocatingShards

        if waitForNodes:
            mapping["wait_for_nodes"] = waitForNodes

        if waitForStatus or waitForRelocatingShard or waitForNode:
            mapping["timeout"] = timeout

        d = self._sendRequest("GET", path, mapping)
        return d

    def clusterState(self, filterNodes=None, filterRoutingTable=None,
                     filterMetadata=None, filterBlocks=None,
                     filterIndices=None):
        """
        Retrieve the cluster state
        """
        path = self._makePath(["_cluster", "state"])
        params = {}

        if filterNodes:
            params['filter_nodes'] = filterNodes

        if filterRoutingTable:
            params['filter_routing_table'] = filterRoutingTable

        if filterMetadata:
            params['filter_metadata'] = filterMetadata

        if filterBlocks:
            params['filter_blocks'] = filterBlocks

        if filterIndicies:
            if isinstance(filterIndices, basestring):
                params['filter_indices'] = filterIndices
            else:
                params['filter_indices'] = ','.join(filterIndices)

        d = self._sendRequest("GET", path, params=params)
        return d

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
