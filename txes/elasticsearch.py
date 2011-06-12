import anyjson

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
        self.retryTime = retryTime
        self.discoveryInterval = discoveryInterval
        self.autorefresh = autorefresh
        self.refeshed = True

        self.info = {}
        self.bulkData = []

        self.connection = connection.connect(servers=servers,
                                             timeout=timeout,
                                             retryTime=retryTime)
        if discover:
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
            reactor.callLater(self.discoveryInterval,
                              self._performDiscovery)

        d = self.clusterNodes()
        d.addCallBack(cb)

    def _sendQuery(self, queryType, query, indexes=None, docTypes=None,
                   **params):
        def sendIt(_=None):
            indexes = self._validateIndexes(indexes)
            if docTypes is None:
                docTypes = []
            elif isinstance(docTypes, basestring):
                docTypes = [docTypes]
            path = self._makePath([','.join(indexes), ','.join(docTypes),
                                   queryType])
            d = self._sendRequest("GET", path, body=query, params=params)
            return d

        if self.autorefresh and not self.refreshed:
            d = self.refresh(indexes)
            d.addCallback(sendIt)
            return d
        else:
            return sendIt()

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
                for alias in info["aliases"]:
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
        actions = [{c: {"index": i, "alias": a}} for c, i, a in commands]
        d = self._sendRequest("POST", "_aliases", {"actions": actions})
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
        def flushIt(_=None):
            indexes = self._validateIndexes(indexes)
            path = self._makePath([','.join(indexes), "_flush"])
            params = None
            if refresh:
                params["refresh"] = True
            d = self._sendRequest("POST", path, params=params)
            return d

        if self.bulkData:
            d = self.forceBulk()
            d.addCallback(flushIt)
            return d
        else:
            return flushIt()

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

        def refreshIt(_=None):
            indexes = self._validateIndexes(indexes)
            path = self._makePath([','.join(indexes), "_refresh"])
            d = self._sendRequest("POST", path)
            d.addCallback(delay)
            return d

        if self.bulkData:
            d = self.forceBulk()
            d.addCallback(refreshIt)
            return d
        else:
            return refreshIt()

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

    def clusterStats(self, nodes=None):
        """
        The cluster nodes info API
        """
        parts = ["_cluster", "nodes"]
        if nodes:
            parts.append(','.join(nodes))
        parts.append("stats")
        path = self._makePath(parts)
        d = self._sendRequest("GET", path)
        return d

    def index(self, doc, index, docType, id=None, parent=None,
              forceInsert=None, bulk=False, version=None,
              querystringArgs=None):
        """
        Index a dict into a specific index and make it searchable
        """
        self.refreshed = False

        if bulk:
            optype = "index"
            if forceInsert:
                optype = "create"
            cmd = {optype: {"_index": index, "_type": docType}}
            if parent:
                cmd[optype]["_parent"] = parent
            if version:
                cmd[optype]["_version"] = version
            if id:
                cmd[optype]["_id"] = id
            data = '\n'.join([anyjson.serialize(cmd),
                              anyjson.serialize(doc)])
            self.bulkData.append(data)
            return self.flushBulk()

        if not querystringArgs:
            querystringArgs = {}

        if forceInsert:
            querystringArgs["opType"] = "create"

        if parent:
            querystringArgs["parent"] = parent

        if version:
            querystringArgs["version"] = version

        if id:
            requestMethod = "PUT"
        else:
            requestMethod = "POST"

        path = self._makePath([index, docType, id])
        d = self._sendRequest(requestMethod, path, body=doc,
                              params=querystringArgs)
        return d

    def flushBulk(self, forces=False):
        """
        Wait to process all pending operations
        """
        if not forced and len(self.bulkData) < self.bulkSize:
            return defer.succeed(None)
        return self.forceBulk()

    def forceBulk(self):
        """
        Force executing of all bulk data
        """
        if not len(self.bulkData):
            return defer.succeed(None)

        data = '\n'.join(self.buldData)
        d = self._sendRequest("POST", "/_bulk", body=data)
        self.bulkData = []
        return d

    def delete(self, index, docType, id, bulk=False):
        """
        Delete a typed JSON document from a specific index based on its id.
        """
        if bulk:
            cmd = {"delete": {"_index": index,
                              "_type": docType,
                              "_id": id}}
            self.bulkData.append(anyjson.serialize(cmd))
            return self.flushBulk()

        path = self._makePath([index, docType, id])
        d = self._sendRequest("DELETE", path)
        return d

    def deleteByQuery(self, indexes, docTypes, query, **params):
        """
        Delete documents from one or more indexes and one or more types
        based on query.
        """
        indexes = self._validateIndexes(indexes)
        if not docTypes:
            docTypes = []
        elif isinstance(docTypes, basestring):
            docTypes = [docTypes]

        path = self._makePath([','.join(indexes), ','.join(docTypes),
                               "_query"])
        d = self._sendRequest("DELETE", path, body=body, params=params)
        return d

    def deleteMapping(self, index, docType):
        """
        Delete a document type from a specific index.
        """
        path = self._makePath([index, docType])
        d = self._sendRequest("DELETE", path)
        return d

    def get(self, index, docType, id, fields=None, routing=None, **params):
        """
        Get a typed document form an index based on its id.
        """
        path = self._makePath([index, docType, id])
        if fields:
            params["fields"] = ','.join(fields)
        if routing:
            params["routings"] = routing
        d = self._sendRequest("GET", path, params=params)
        return d

    def search(self, query, indexes=None, docType=None, **params):
        """
        Execute a search agains one or more indices
        """
        indexes = self._validateIndexes(indexes)
        d = self._sendQuery("_search", query, indexes, docTypes, **params)
        return d

    def scan(self, query, indexes=None, docTypes=None, scrollTimeout="10m",
             **params):
        """
        Return an iterator which will scan against one or more indices.
        Each call to next() will yeild a deferred that will contain the
        next dataset
        """

        class Scroller(object):
            def __init__(self, results):
                self.results = results

            def __iter__(self):
                return self

            def _setResults(self, results):
                if not len(results["hits"]["hits"]):
                    raise StopIteration
                self.results = results
                return results

            def next(self):
                scrollId = self.results["_scroll_id"]
                d = self._send_request("GET", "_search/scroll", scrollId,
                                       {"scroll": scrollTimeout})
                d.addCallback(self._setResults)
                return

        def scroll(results):
            return Scroller(results)

        d = self.search(query=query, indexes=indexes, docTypes=docTypes,
                        searchTypes="scan", scroll=scrollTimeout, **params)
        d.addCallback(scroll)
        return d

    def reindex(self, query, indexes=None, docTypes=None, **params):
        """
        Execute a search query against one or more indices and reindex the
        hits.
        """
        indexes = self._validateIndexes(indexes)
        if not docTypes:
            docTypes = []
        elif isinstance(docTypes, basestring):
            docTypes = [docTypes]
        path = self._makePath([','.join(indexes), ','.join(docTypes),
                               "_reindexbyquery"])
        d = self._sendRequest("POST", path, body=query, params=params)
        return d

    def count(self, query, indexes=None, docTypes=None, **params):
        """
        Execute a query against one or more indices and get the hit count
        """
        indexes = self._balidateIndexes(indexes)
        d = self._sendQuery("_count", query, indexes, docTypes, **params)

    def createRiver(self, river, riverName=None):
        """
        Create a river
        """
        if not riverName:
            riverName = river["index"]["index"]
        d = self._sendRequest("PUT", "/_river/%s/_meta" % riverName,
                              body=river)
        return d

    def deleteRiver(seld, river, riverName=None):
        """
        Delete a river
        """
        if not riverName:
            riverName = river["index"]["index"]
        d = self._sendRequest("DELETE", "/_river/%s/" % riverName)
        return d

    def moreLikeThis(seld, index, docType, id, fields, **params):
        """
        Execute a "more like this" search query against on eor more fields.
        """
        path = self._makePath([index, docType, id, "_mlt"])
        params["fields"] = ','.join(fields)
        d = self._sendRequest("GET", path, params=params)
        return d

    def updateSettings(self, index, settings):
        """
        Update settings of an index.
        """
        path = self._makePath([index, "_settings"])
        d = self._sendRequest("PUT", path, body=settings)
        return d

    @property
    def servers(self):
        return self.connection.servers
