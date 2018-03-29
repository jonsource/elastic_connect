from elasticsearch import Elasticsearch
import elasticsearch.exceptions
import time
import requests
import logging

_global_prefix = ''
"""
Global index prefix. Used for example to distinguish between index names used in production and in tests.
"""

logger = logging.getLogger(__name__)

class NamespaceConnectionError(Exception):
    pass


class NamespaceAlreadyExistsError(Exception):
    pass


class Namespace(object):
    """
    Object describing a namespace of an elasticsearch cluster or a connection to a different elasticsearch cluster.
    Each namespace may have a different es_conf, thus connecting to a different elasticsearch cluster and/or a different
    index_prefix thus using a different set of indices on the same cluster.

    For example you may use two different namespaces to run two instances of the same application against a single
    elasticsearch cluster. But due to using different index_prefixes on the ``_default`` namespace, each application
    will preserve it's own data, i.e. one, with ``index_prefix="our"`` using indices ``our_users`` and ``our_data``,
    the other with ``index_prefix="their"`` using indices ``their_users`` and ``their_data``.

    It is also possible to use multiple namespaces in a single application.
    """

    def __init__(self, name, es_conf, index_prefix=None):
        """
        :param name: name of the namespace, must be unique
        :param es_conf: the configuration of the namespace i.e. at least {'host':..., 'port':...}. It is internally
            passed to the underlaying elasticsearch.Elasticsearch class.
        :param index_prefix: prefix of the namespace, it should probably be unique on the same cluster for sanity
            reasons, but no check is enforced
        """

        self.name = name
        self.es_conf = es_conf
        if index_prefix is None:
            index_prefix = name + '_'
        self._index_prefix = index_prefix
        self.es = None

    def register_model_class(self, model_class):
        """
        Registers a model class in this namespace. By default all model classes are registered in the ``_default``
        namespace. By registering a model in a namespace it is possible to reuse it to connect to a different
        Elasticsearch cluster.

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
        :return: True
        :raises: NamespaceConnectionError on connection timeout
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
        """
        @property

        Returns the calculated index prefix, taking into account any global prefixes as well.
        """
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

        :param index: name of index to be deleted
        :param timeout: if the index is not deleted after the number of seconds, Exception is raised.
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
        """
        Deletes multiple indices, blocks until they are deleted.

        :param indices: names of indices to be deleted
        :return: None
        """

        # TODO: delete the indices in parallel
        for index in indices:
            self.delete_index(index)


_namespaces = {'_default': Namespace(name='_default', es_conf=None, index_prefix='')}
"""
A singleton dict containing all registered namespaces indexed by their names.
"""

def register_namespace(namespace: Namespace):
    """
    Register a new namespace. Changing a Namespace's parameters after it was registered may do crazy things,
    don't do it.

    :param namespace: Namespace instance to be registered
    :return: None
    :raises: NamespaceAlreadyExistsError if a Namespace with the same name already exists
    """
    if namespace.name in _namespaces:
        raise NamespaceAlreadyExistsError("Namespace " + namespace.name + " already exists!")

    _namespaces[namespace.name] = namespace
