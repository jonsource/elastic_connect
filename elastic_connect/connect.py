from elasticsearch import Elasticsearch
import elasticsearch
from collections import UserList
import time
from elastic_connect.data_types.join import MultiJoin, SingleJoin

es = None
es_conf = None

compatibility = 6
index = 'api'


def connect(conf):
    global es_conf
    if not es_conf:
        # TODO handle reconnects
        es_conf = conf


def get_es():
    global es
    if not es:
        es = Elasticsearch(es_conf)
    return es


class Result(UserList):
    """Handles the conversion of Elasticsearch query results to models."""

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

    def to_dict(self):
        return [result.to_dict() for result in self.results]


class DocTypeConnection(object):
    """Connection for a specific model to Elasticsearch.

    For ES < 6 supports multiple doc_types in a single index.
    For ES >= 6 each model type needs it's own index.
    """

    # TODO: sanitize input by https://stackoverflow.com/questions/16205341/symbols-in-query-string-for-elasticsearch

    def __init__(self, model, es, index, doc_type, default_args={}):
        self.es = es
        if compatibility >= 6:
            self.index_name = doc_type
        else:
            self.index_name = index
        self.doc_type = doc_type
        self.default_args = default_args
        self.model = model

    def get_default_args(self):
        default = {"index": self.index_name, "doc_type": self.doc_type}
        default.update(self.default_args)
        return default

    def __getattr__(self, name):
        """All methods are redirected to the underlying elasticsearch connection.

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
    """Creates index mapping in Elasticsearch for each model passed in.

    Doesn't update existing mappings.
    """

    def safe_create(index, body):
        es = get_es()
        try:
            es.indices.create(index=index, body=body)
            print("** Index %s created" % index)
        except elasticsearch.exceptions.RequestError as e:
            print("** Index %s already exists!!" % index)
            if e.error != 'index_already_exists_exception':
                raise e

    mapping = {}
    for model_class in model_classes:
        properties = {}
        for name, type in model_class._mapping.items():
            if name.startswith('_'):
                continue
            es_type = type.get_es_type()
            if es_type:
                properties.update({name: {"type": es_type}})
        mapping[model_class._mapping['_doc_type']] = {"properties": properties}

    created = []
    if compatibility >= 6:
        for name in mapping.keys():
            safe_create(index=name, body={"mappings": {name: mapping[name]}})
            created.append(name)
    else:
        safe_create(index=index, body={"mappings": mapping})
        created.append(index)
    return created


def delete_index(index, timeout=2.0):
    """Deletes an index from Elasticsearch and blocks until it is deleted.

    Timeouts after timeout seconds.
    """

    result = es.indices.delete(index=index)
    rep = int(10 * timeout)
    while rep and es.indices.exists(index=index):
        rep -= 1
        time.sleep(0.1)

    if not rep and es.indices.exists(index=index):
        raise Exception("Timeout. Index %s still exists after %s seconds." % (index, timeout))

    print("** Index %s deleted" % index)


def delete_indices(indices):
    # TODO: delete the indices in parallel
    for index in indices:
        delete_index(index)
