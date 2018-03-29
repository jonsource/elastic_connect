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

    def __init__(self, result, model):
        self.meta = result
        ret = []
        try:
            self.hits = result['hits']['hits']
        except KeyError:
            self.hits = [result]

        for hit in self.hits:
            ret.append(model.from_es(hit))
        self.results = ret
        super(Result, self).__init__(self.results)


class DocTypeConnection(object):
    """
    Connection for a specific model to Elasticsearch.

    For ES < 6 supports multiple doc_types in a single index.
    For ES >= 6 each model type needs it's own index.
    """

    # TODO: sanitize input by https://stackoverflow.com/questions/16205341/symbols-in-query-string-for-elasticsearch

    def __init__(self, model, es_namespace, index, doc_type, default_args={}):
        """
        :param model:
        :param es_namespace:
        :param index:
        :param doc_type:
        :param default_args:
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
        Search and get methods return Result on success, otherwise the JSON from Elasticseach is returned.
        """

        def helper(**kwargs):
            es_func = getattr(self.es, name)
            pass_args = self.get_default_args().copy()
            pass_args.update(kwargs)
            data = es_func(**pass_args)
            if 'hits' in data or name == "get":
                result = Result(data, self.model)
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
    return _namespaces['_default'].delete_indices(indices)


def connect(conf, index_prefix=''):
    _namespaces['_default'].es = None
    _namespaces['_default'].es_conf = conf
    _namespaces['_default']._index_prefix = index_prefix
    return _namespaces['_default']
