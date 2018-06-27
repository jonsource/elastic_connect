from collections import UserList
from .namespace import _namespaces


es_conf = {'_default': {'es_conf': None}
          }


def get_es():

    return _namespaces['_default'].get_es()


class Result(UserList):
    """
    Handles the conversion of Elasticsearch query results to models.
    """

    def __init__(self, result, model, method, pass_args):
        self.meta = result
        self.method = method
        self.pass_args = pass_args
        self.model = model
        ret = []
        try:
            self.hits = result['hits']['hits']
        except KeyError:
            self.hits = [result]

        for hit in self.hits:
            ret.append(model.from_es(hit))
        self.results = ret
        if len(self.hits):
            self.search_after_values = self.hits[-1]['sort']
        else:
            self.search_after_values = None
        super(Result, self).__init__(self.results)

    def search_after(self):
        """
        Utilizes the Elasticsearch search_after capability to perform some real-time scrolling through the results.
        Uses the parameters of the search that generated this result to perform another search, with the last models
        order values as search_after values.

        :example:

        .. code-block:: python

            found = models.find_by(website='www.zive.cz', size=10)
            # first 10 results (sorted by _uid in ascending order - the default) are returned

            found.search_after()
            # further 10 results (sorted by _uid) are returned

        :return: further results
        """
        self.pass_args['body']['search_after'] = self.search_after_values
        return getattr(self.model.get_es_connection(), self.method)(**self.pass_args)



class DocTypeConnection(object):
    """
    Connection for a specific model to Elasticsearch.

    In ES >= 6 each model type needs it's own index.

    deprecated - ES < 6 supports multiple doc_types in a single index.
    """

    # TODO: sanitize input by https://stackoverflow.com/questions/16205341/symbols-in-query-string-for-elasticsearch

    def __init__(self, model, es_namespace, index, doc_type, default_args={}):
        """
        :param model: class of the model for which the connection is created
        :param es_namespace: es_namespace in which the connection is created
        :param index: name of the index
        :param doc_type: name of the doc type - for future compliance should match the index name
        :param default_args: a dict of default args to pass to the underlying elasticsearch connections
        """
        self.es_namespace = es_namespace
        self.es = es_namespace.get_es()
        self.index_name = es_namespace.index_prefix + doc_type
        self.doc_type = doc_type
        self.default_args = default_args
        self.model = model

    def get_default_args(self):
        default = {"index": self.index_name, "doc_type": self.doc_type}
        default.update(self.default_args)
        return default

    def __getattr__(self, name):
        """
        All methods are redirected to the underlying elasticsearch connection.
        Search and get methods return Result on success, otherwise the JSON from elasticseach is returned.
        """

        def helper(**kwargs):
            es_func = getattr(self.es, name)
            pass_args = self.get_default_args().copy()
            pass_args.update(kwargs)
            data = es_func(**pass_args)
            if 'hits' in data or name == "get":
                result = Result(data, self.model, method=name, pass_args=pass_args)
                if name == "get" and len(result) == 1:
                    return result[0]
                return result
            return data

        return helper


def create_mappings(model_classes):
    """
    Shortcut for _namespaces['_default'].create_mappings

    Creates index mapping in Elasticsearch for each model passed in.
    Doesn't update existing mappings.

    :param model_classes: a list of classes for which indices are created
    :return: returns the names of indices which were actually created
    """

    return _namespaces['_default'].create_mappings(model_classes)


def delete_index(index, timeout=2.0):
    return _namespaces['_default'].delete_index(index, timeout)


def delete_indices(indices):
    """
    Delete all of the provided indices. Blocks untill they are deleted.

    Unlike the create_mappings and other operations, index deletes in elastic_connect *don't* perform any index_name
    prefix magic. All index deletions in elastic_connect are attempted with the name provided 'as is'.

    :param indices: names of indices to be deleted
    :return: None
    """
    return _namespaces['_default'].delete_indices(indices)


def connect(conf, index_prefix=''):
    """
    Establish a connection to elasticsearch using the _default namespace.

    :param conf: The parameters of the _default namespace
    :param index_prefix: prefix to be used for all indices using this connection. Default = ''
    :return: instance of the _default Namespace
    """
    _namespaces['_default'].es = None
    _namespaces['_default'].es_conf = conf
    _namespaces['_default']._index_prefix = index_prefix
    return _namespaces['_default']
