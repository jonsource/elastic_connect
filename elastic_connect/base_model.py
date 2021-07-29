import elastic_connect
import elastic_connect.data_types as data_types
import elastic_connect.data_types.base as data_types_base
import logging

logger = logging.getLogger(__name__)


class IntegrityError(Exception):
    pass


class Model(object):
    """
    Base class for Models stored in elasticseach.

    Handles creation, loading and saving of the models.
    Also handles simple SQL-like joins, in fact lazy loading dependent
    child/parent models.
    """

    __slots__ = ('id', '_version')

    _mapping = {  # type: dict[str:data_types.base.BaseDataType]
        'id': data_types.Keyword(name='id'),
    }
    """
    Dictionary describing the model.
    property_name: elasticsearch data type or 'ref' for reference to
    other model, defined by a join

    keys starting with _ are not saved in elasticsearch
    """

    _meta = {
        '_doc_type': 'model',
        # not needed - if missing, treated as false
        # '_load_version': False,
        # '_check_version': False,
        # '_post_save_refresh': False
    }

    _es_namespace = elastic_connect._namespaces['_default']
    _es_connection = None

    def __init__(self, **kw):
        r"""
        Creates an instance of the model using \*\*kw parameters for
        setting values of attributes. The values get converted by their
        respective data_type.from_python method.
        """

        for property, type in self._mapping.items():
            self._update(property, type.get_default_value())
        for property, type in self._mapping.items():
            value = kw.get(property, type.get_default_value())
            self._update(property,
                          type.on_update(type.from_python(value), self))

    @classmethod
    def get_index(cls):
        """
        Returns the name of the index this model is stored
            in, including any prefixes defined globally or in namespace.

            In ES >= 6 each model type needs it's own index.

            ES < 6 supports multiple doc_types in a single index.
        """

        return cls._es_namespace.index_prefix + cls._meta['_doc_type']

    @classmethod
    def get_doctype(cls):
        """
        :deprecated: Returns the name of the index this model is stored
            in, including any prefixes defined globally or in namespace.

            In ES >= 6 each model type needs it's own index.

            ES < 6 supports multiple doc_types in a single index.
        """

        return cls._meta['_doc_type']

    def get_id(self):
        return self.id

    def _compute_id(self):
        """
        Count or return stored id for this model instance.

        :return: None for unsaved models which should receive a unique
            id generated by Elasticsearch. Should be overriden and
            return some computed value for models which handle their
            uniqe id by themselves - mainly to keep a model parameter
            unique
        """

        return self.id

    @classmethod
    def get_es_connection(cls):
        """
        Initializes or returns an existing DocTypeConnection to
        elasticsearch for this model.

        :return: DocTypeConnection
        """
        if not cls._es_connection:
            logger.debug("%s connecting to %s",
                         cls.__name__, str(cls._es_namespace.__dict__))
            cls._es_connection = elastic_connect.DocTypeConnection(
                model=cls, es_namespace=cls._es_namespace,
                index=cls.get_index(),
                doc_type=cls.get_doctype())
            logger.debug("connection index name %s",
                         cls._es_connection.index_name)
        else:
            logger.debug("%s connection already established %s",
                         cls.__name__,
                         str(cls._es_connection.__dict__))
        return cls._es_connection

    @classmethod
    def from_dict(cls, **kw):
        """
        Create and return an unsaved model instance based on dict.

        :param kw: keyword arguments describing the model's attributes
        :return: instance of the model
        """

        model = cls(**kw)
        return model

    @classmethod
    def from_es(cls, hit):
        """
        Create and return an unsaved model instance based on
        elasticsearch query result.

        :param hit: a ``hit`` from an elasticsearch query
        :return: instance of the model
        """

        kwargs = {}
        for property, type in cls._mapping.items():
            kwargs.update({property: type.from_es(hit['_source'])})
        kwargs['id'] = hit['_id']
        if cls.should_load_version():
            kwargs['_version'] = hit['_version']
        model = cls(**kwargs)
        return model

    @classmethod
    def create(cls, **kw) -> 'Model':
        """
        Create, save and return a model instance based on dictionary.
        Property id gets set by elasticsearch or computed depending on
        cls._compute_id()

        :param kw: keyword arguments describing the model's attributes
        :return: instance of the model with the ``id`` set
        """
        # logger.warn('bm create %s' % kw)
        model = cls.from_dict(**kw)
        model.id = model._compute_id()
        ret = cls._create(model)
        return ret

    @classmethod
    def _create(cls, model):
        """
        Handles the creation of the model in elasticsearch
        Models without an id are indexed, thus receiving id from
        elasticsearch.
        Models with id are created. This prevents the creation of
        duplicates.

        :param model: the model to be created
        :return: the model with the ``id`` set
        """

        serialized_flat = model.serialize(
            exclude=['id', '_version'], flat=True)
        if model.id:
            response = cls.get_es_connection().create(id=model.id,
                                                      body=serialized_flat)
        # TODO: probably needs to call cls.refresh() to properly prevent
        # creation of duplicates
        else:
            response = cls.get_es_connection().index(body=serialized_flat)
        model.id = response['_id']
        logger.debug("model.id from _create %s", model.id)
        model._version = response['_version']
        model.post_save()
        return model

    def save(self):
        """
        Save a model that has an id, index a model without an id into
        elasticsearch. Saves unsaved joins recursively. Joined models,
        which already have an id (and thus are already present in the
        database) are not re-saved automatically. You must save them
        yourself if they changed.

        :return: self with dependencies updated
        """
        es_connection = self.get_es_connection()

        if self.id:
            cmp = self._compute_id()
            if cmp and cmp != self.id:
                raise IntegrityError("Can't save model with a changed "
                                     "computed id, create a new model")
            serialized = self.serialize(exclude=['id', '_version'])
            if self.should_check_version():
                ver = self._version
            else:
                ver = None
            response = es_connection.update(id=self.id,
                                            body={'doc': serialized},
                                            version=ver)
            self._version = response['_version']
        else:
            self.id = self._compute_id()
            self._create(self)

        return self.post_save()

    def post_save(self):
        logger.debug("post_save %s %s", self.__class__.__name__, self.id)
        ret = []
        for property, type in self._mapping.items():
            ret.append(type.on_save(model=self))
        logger.debug("post_save ret %s %s", self.id, ret)
        ret = [r for r in ret if r is not None]
        if len(ret):
            # self.refresh()
            if self._meta.get('_post_save_refresh'):
                self.refresh()
            # resave, because some child models were updated
            self.save()
        return self

    def delete(self, force=None):
        """
        Delete a model from elasticsearch.

        :return: None
        """

        self.get_es_connection().delete(id=self.id)

    def _lazy_load(self):
        """
        Lazy loads model's joins - child / parent models.
        """

        for property, type in self._mapping.items():
            logger.info("pre _lazy_load %s %s (%s)",
                         property, self.__getattribute__(property), type)
            self._update(property, type.lazy_load(self))
        logger.info("_lazy_load %s", self)
        return self

    @classmethod
    def get(cls, id):
        """
        Get a model by id from elasticsearch.

        :param id: id of the model to get. Can be a list of ids.
        :return: returns an instance of elastic_connect.connect.Result
        """
        if isinstance(id, str):
            logger.debug("getting single document %s" % id)
            ret = cls.get_es_connection().get(id=id)
            return ret
        else:
            if not id:
                return []
            logger.debug("getting multiple documents %s" % id)
            ret = cls.get_es_connection().mget(body={'ids': id})
            return ret

    @classmethod
    def all(cls, size=100, sort=None, search_after=None):
        """
        Get all models from Elasticsearch.
        :param size: max number of hits to return. Default = 100.
        :param sort: sorting of the result as provided by
            prepare_sort(sort)
        :param search_after: searches for results 'after' the value(s)
            supplied, preferably used with
            elastic_connect.connect.Result.search_after_values
        :return: returns an instance of elastic_connect.connect.Result
        """
        return cls.find_by(size=size, sort=sort, search_after=search_after)

    @classmethod
    def get_default_sort(cls):
        """
        Returns the default sort order, which is used by find_by() and
        all() if no other sorting is explicitly provided in their call.
        """
        sort = []
        if hasattr(cls, 'order'):
            sort.append({'order': 'asc'})
        sort.append({'_uid': 'asc'})
        return sort

    @classmethod
    def prepare_sort(cls, sort=None, stringify=False):
        """
        Prepares sorting for model. Defaults to get_default_sort,
        {"_uid": "asc"} is also appended as last resort to all sorts
        that don't use _uid. Sorting by _id is not supported by
        elasticsearch, use _uid (_doc_type + '#' + _id) instead.
        Important: _uid is not incremental in elasticsearch, it's here
        just to get constistent results on the same dataset.
        :param sort: array of {property: "asc|desc"} values
        :param stringify: default False: if the result should be
            stringified for kw parameter, or left in the json format for
            body of Elasticsearch query.
        :return: returns the input sort with appended {"_uid": "asc"}
        """

        def prepare_sort_array(sort):
            if not sort:
                return cls.get_default_sort()

            for s in sort:
                if '_uid' in s:
                    return sort

            sort.append({'_uid': 'asc'})
            return sort

        sort = prepare_sort_array(sort)

        if not stringify:
            return sort
        ret = []
        for pair in sort:
            for key, value in pair.items():
                ret.append("%s:%s" % (key, value))
        return ret

    @classmethod
    def find_by(cls,
                size=100,
                sort=None,
                search_after=None,
                query=None,
                **kw):
        """
        Search for models in Elasticsearch by attribute values.

        :example:

        .. code-block:: python

            # return model with email="test@test.cz"
            model.find_by(email="test@test.cz")

            # return model with both email="test@test.cz" and parent=10
            model.find_by(email="test@test.cz", parent=10)

            # return models with parent 10 sorted by email ascending
            model.find_by(parent=10, sort=[{"email":"asc"}])

            # return models with email >= "foo@bar.cz" (and _uid > '' as
            # per default sort order, every _uid is greated than '')
            model.find_by(parent=10,
                          sort=[{"email":"asc"}],
                          search_after["foo@bar.cz", ''])

            # return models with parent 10 and email _anything_@bar.cz
            model.find_by(query="parent: 10 AND email: *@bar.cz")

        :param size: max number of hits to return. Default = 100.
        :param kw: attributes of the model by which to search
        :param sort: sorting of the result as provided by
            prepare_sort(sort)
        :param search_after: searches for results 'after' the value(s)
            supplied, preferably used with
            elastic_connect.connect.Result.search_after_values
        :param query: instead of specifying kw search arguments, you may
            enter here a wildcard query
        :return: returns an instance of elastic_connect.connect.Result
        """

        if not query:
            query = kw

        sort = cls.prepare_sort(sort)

        if isinstance(query, str):
            _query = {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": query,
                                "analyze_wildcard": True
                            }
                        }
                    ]
                }
            }
        else:
            if len(query.keys()) == 1:
                _query = {"term": query}
            else:
                _query = {
                    "bool": {
                        "must": [{"term": {k: kw[k]}} for k in query.keys()]
                    }
                }

        body = {
            "size": size,
            "query": _query,
            "sort": sort
        }
        if search_after:
            body['search_after'] = search_after

        logger.debug("find_by body %s", body)
        ret = cls.get_es_connection().search(
            body=body, version=cls.should_load_version())
        return ret

    def serialize(self,
                  exclude=["password"],
                  depth=0,
                  to_str=False,
                  flat=False):
        """
        Serilaizes the model for storing to elasticsearch.

        Joins are flattened from join: model format to join: model.id
        format. Other attributes are serialized by their respective
        type.serialize method

        :param exclude: default=["password"]
        :param depth: default=0, the depth up to which models are
            serialized as jsons, deeper than that models are reduced to
            their id
        :param to_str: default=False, pass True if the serialization is
            for console output purposes
        :param flat: default=False, unsaved joined models are returned
            as Models if False, as None if True
        :return: json representation of the model
        """

        ret = {}
        for property, type in self._mapping.items():
            if property not in exclude:
                serialized = type.serialize(self.__getattribute__(property),
                                            depth=depth,
                                            to_str=to_str,
                                            flat=flat)
                ret.update({property: serialized})
        return ret

    def __repr__(self):
        if self.id:
            return object.__repr__(self) + str(self)
        return object.__repr__(self)

    def __str__(self):
        return str(self.serialize(depth=0, to_str=True))

    @classmethod
    def refresh(cls):
        """
        Refresh the index where this model is stored to make all changes
        immediately visible to others.
        """

        cls._es_namespace.get_es().indices.refresh(index=cls.get_index())

    def __setattr__(self, name, value):
        if name in self._mapping:
            return self._update(name, value)
        return super().__setattr__(name, value)

    def _update(self, name, value):
        super().__setattr__(name, self._mapping[name].on_update(value, self))
        return self

    @classmethod
    def get_es_mapping(cls):
        """
        Returns a dict representing the elastic search mapping for this
        model

        :return: dict
        """

        mapping = {}
        for name, type in cls._mapping.items():
            es_type = type.get_es_type()
            if es_type and (name != 'id' and name != '_version'):
                mapping[name] = es_type

        logger.debug("mapping %s", mapping)
        return mapping

    @classmethod
    def model_mapping(cls, **args):
        """
        Creates a Mapping with given parameters.

        Automatically adds the id field as Keyword if not present

        :param **args: field_name=data_type pairs describing the fields
            of this model.

        :return: Mapping
        """

        mapping = Mapping(**args)
        mapping.add_field('id', data_types.Keyword())
        mapping.add_field('_version', data_types.Long())
        return mapping

    @classmethod
    def should_check_version(cls):
        return cls._meta.get('_check_version', False)

    @classmethod
    def should_load_version(cls):
        return cls._meta.get('_load_version', False) or cls._meta.get(
            '_check_version', False)

    @classmethod
    def version_checking(cls, check_version):
        """
        Sets the version checking behavior

        :param check_version: boolean, whether to check or not check the
            version of document on update. Updating an out of sync
            version raises elasticsearch.exceptions.ConflictError
        :return: CheckVersionManager - can be used in a with statement
            like this

        :example:

        Used as setter:

        .. code-block:: python

           model.version_checking(True)
           instance = model.get(id)

           # some time consuming operations

           instance.new_vale = computed

           # will raise elasticsearch.exceptions.ConflictError if
           # existing document changed since we got our instance

           instance.save()


        Used as a context manager:

        .. code-block:: python

            instance = model.get(id)

            # some time consuming operations

            instance.new_vale = computed

            # make sure the model didn't change underhand
            try:
                with model.version_checking(True):
                    instance.save()
            except elasticsearch.exceptions.ConflictError:
                # handle the differing underlying model here

        """
        manager = cls.CheckVersionManager(cls)
        cls._meta['_check_version'] = check_version
        return manager

    class CheckVersionManager:
        """
        ContextManager that resets _meta('_check_version') attribute
        back to it's old value upon exit from with statement.
        """

        def __init__(self, parent_class):
            self.parent_class = parent_class
            self.old_value = parent_class._meta.get('_check_version', False)

        def __enter__(self):
            pass

        def __exit__(self, type, value, traceback):
            self.parent_class._check_version = self.old_value


class Mapping(dict):
    """
    Class describing an Elasticsearch mapping.
    """

    def __init__(self, **kwargs):
        for n, v in kwargs.items():
            self.__setitem__(n, v)

    def __setitem__(self, name, value):
        if not isinstance(value, data_types_base.BaseDataType):
            raise Exception(
                "Only BaseDataType derived classes can be used in a Mapping")
        value.name = name
        return super().__setitem__(name, value)

    def add_field(self, name, value):
        if name not in self:
            self.__setitem__(name, value)
