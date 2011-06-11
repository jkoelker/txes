# Stolen from pyes convert_errors and exceptions


class NoServerAvailable(Exception):
    pass


class InvalidQuery(Exception):
    pass


class InvalidParameterQuery(InvalidQuery):
    pass


class QueryError(Exception):
    pass


class QueryParameterError(Exception):
    pass


class ScriptFieldsError(Exception):
    pass


class ElasticSearchException(Exception):
    """
    Base class of exceptions raised as a result of parsing an error return
    from ElasticSearch.

    An exception of this class will be raised if no more specific subclass
    is appropriate.
    """
    def __init__(self, error, status=None, result=None):
        super(ElasticSearchException, self).__init__(error)
        self.status = status
        self.result = result


class ElasticSearchIllegalArgumentException(ElasticSearchException):
    pass


class IndexMissingException(ElasticSearchException):
    pass


class NotFoundException(ElasticSearchException):
    pass


class AlreadyExistsException(ElasticSearchException):
    pass


class IndexAlreadyExistsException(AlreadyExistsException):
    pass


class SearchPhaseExecutionException(ElasticSearchException):
    pass


class ReplicationShardOperationFailedException(ElasticSearchException):
    pass


class ClusterBlockException(ElasticSearchException):
    pass


class MapperParsingException(ElasticSearchException):
    pass


exception_patterns_trailing = {
    '] missing': NotFoundException,
    '] Already exists': AlreadyExistsException,
}


def raiseExceptions(status, result):
    """
    Raise an exception if the result is an error ( status > 400 )
    """
    status = int(status)

    if status < 400:
        return

    if status == 404 and isinstance(result, dict) and result.get("ok"):
        raise NotFoundException("Item not found", status, result)

    if not isinstance(result, dict) or "error" not in result:
        raise ElasticSearchException("Unknown exception type",
                                     status, result)

    error = result["error"]
    bits = error.split('[', 1)
    if len(bits) == 2:
        excClass = globals().get(bits[0])
        if exeClass:
            msg = bits[1].rstrip(']')
            raise excClass(msg, status, result)

    for pattern, excClass in exception_patterns_trailing.iteritems():
        if not error.endswith(pattern):
            continue
        raise excClass(error, status, result)

    raise ElasticSearchException(error, status, result)
