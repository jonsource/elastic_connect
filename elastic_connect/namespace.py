from elasticsearch import Elasticsearch
import elasticsearch.exceptions
import time
import requests
import logging

_global_prefix = ''

logger = logging.getLogger(__name__)

class NamespaceConnectionError(Exception):
    pass


class NamespaceAlreadyExistsError(Exception):
    pass


class Namespace(object):
    def __init__(self, name, es_conf, index_prefix=None):
        self.name = name
        self.es_conf = es_conf
        if index_prefix is None:
            index_prefix = name + '_'
        self._index_prefix = index_prefix
        self.es = None

    def register_model_class(self, model_class):
        """
        Registers a model class in this namespace. By default all model classes are registered in the _default
        namespace. By registering a model in a namespace it is possible to reuse it to connect to a different
        Elasticsearch instance.
        :param model_class: The model class to be registered
        :return: Returns a new model class with name prefixed with Namespace.name and properly set _es_namespace
        reference.
        """

        if self.name == '_default':
            model_class._es_namespace = self
            return model_class

        class NewModelClass(model_class):
            _es_namespace = None
            _es_connection = None
            __slots__ = model_class.__slots__

        NewModelClass.__name__ = self.name + '_' + model_class.__name__
        NewModelClass._es_namespace = self
        return NewModelClass

    def get_es(self):
        if not self.es:
            self.es = Elasticsearch(self.es_conf)
        return self.es

    def wait_for_yellow(self):
        return self.get_es().cluster.health(wait_for_status="yellow")

    def get_es_url(self, https=False):
        # TODO: handle possible multiple urls
        protocol = "http"
        if https:
            protocol += "s"
        host = self.es_conf[0].get('host', 'localhost')
        port = self.es_conf[0].get('port', 9200)
        return "%s://%s:%s" % (protocol, host, port)

    def wait_for_http_connection(self, initial_wait=10.0, step=0.1, timeout=30.0, https=False):
        """
        Waits for http(s) connection to Elasticsearch to be ready
        :param initial_wait: initially wait in seconds
        :param step: try each step seconds after initial wait
        :param timeout: raise NamespaceConnectionError after timeout seconds of trying. This includes the inital wait.
        :param https: whether to use http or https protocol
        :return:
        """
        time.sleep(initial_wait)
        t = initial_wait
        url = self.get_es_url(https)
        while t < timeout:
            t += step
            time.sleep(step)
            try:
                ret = requests.get(url)
            except Exception as e:
                continue
            if ret.status_code == 200:
                return True
        raise NamespaceConnectionError("Elasticsearch @ %s connection timeout" % url)

    def wait_for_ready(self, initial_attempt=True, initial_wait=2.0, step=0.1, timeout=30.0, https=False):
        """
        Waits for Elasticsearch to get ready. First waits for the node to responde over http, then waits for
        the cluster to turn at least yellow.

        :param initial_attempt: If True, attempts a http connection right away, even before starting the initial_wait
        :param initial_wait: initially wait in seconds
        :param step: try each step seconds after initial wait
        :param timeout: raise NamespaceConnectionError after timeout seconds of trying. This includes the inital wait.
        :param https: whether to use http or https protocol
        :return: returns cluster health info
        """
        if initial_attempt:
            try:
                self.wait_for_http_connection(initial_wait=0, step=0, timeout=0, https=https)
            except Exception as e:
                self.wait_for_http_connection(initial_wait, step, timeout, https)
        return self.wait_for_yellow()

    @property
    def index_prefix(self):
        return _global_prefix + self._index_prefix

    def create_mappings(self, model_classes):
        """
        Creates index mapping in Elasticsearch for each model passed in.
        Doesn't update existing mappings.

        :param model_classes: a list of classes for which indices are created
        :return: returns the names of indices which were actually created
        """

        def safe_create(index, body):
            try:
                self.get_es().indices.create(index=index, body=body)
                logger.info("Index %s created", (index,))
            except elasticsearch.exceptions.RequestError as e:
                logger.info("Index %s already exists!", (index,))
                if e.error != 'index_already_exists_exception':
                    raise e

        mappings = {}
        for model_class in model_classes:
            mappings[model_class.get_index()] = {"properties": model_class.get_es_mapping()}

        created = []

        for name in mappings.keys():
            safe_create(index=name, body={"mappings": {name: mappings[name]}})
            created.append(name)

        return created

    def delete_index(self, index, timeout=2.0):
        """
        Deletes an index from Elasticsearch and blocks until it is deleted.

        :param index: index to be deleted
        :param timeout: default 2, if the index is not deleted after the number of seconds, Exception is riased.
        If timeout = 0 doesn't block and returns immediately
        :return: none
        """

        es = self.get_es()
        result = es.indices.delete(index=index)
        rep = int(10 * timeout)

        if not timeout:
            return

        while rep and es.indices.exists(index=index):
            rep -= 1
            time.sleep(0.1)

        if not rep and es.indices.exists(index=index):
            raise Exception("Timeout. Index %s still exists after %s seconds." % (index, timeout))

        logger.info("Index %s deleted", (index))

    def delete_indices(self, indices):
        # TODO: delete the indices in parallel
        for index in indices:
            self.delete_index(index)


_namespaces = {'_default': Namespace(name='_default', es_conf=None, index_prefix='')}


def register_namespace(namespace: Namespace):
    if namespace.name in _namespaces:
        raise NamespaceAlreadyExistsError("Namespace " + namespace.name + " already exists!")

    _namespaces[namespace.name] = namespace
