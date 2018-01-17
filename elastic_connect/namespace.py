from elasticsearch import Elasticsearch

_global_prefix = ''

class Namespace(object):
    def __init__(self, name, es_conf, index_prefix=None):
        self.name = name
        self.es_conf = es_conf
        if index_prefix is None:
            index_prefix = name + '_'
        self._index_prefix = index_prefix
        self.es = None

    def register_model_class(self, model_class):
        if self.name == '_default':
            model_class._es_namespace = self
            return model_class

        class NewModelClass(model_class):
            #TODO: how to deal with __slots__
            _es_namespace = None
            _es_connection = None

        NewModelClass.__name__ = self.name + '_' + model_class.__name__
        NewModelClass._es_namespace = self
        return NewModelClass

    def get_es(self):
        if not self.es:
            self.es = Elasticsearch(self.es_conf)
        return self.es

    @property
    def index_prefix(self):
        return _global_prefix + self._index_prefix

    # TODO write wait_until_es_ready method


_namespaces = {'_default': Namespace(name='_default', es_conf=None, index_prefix='')}

def register_namespace(namespace: Namespace):
    if namespace.name in _namespaces:
        raise Exception("Namescpace " + namespace.name + " already exists!")

    _namespaces[namespace.name] = namespace
