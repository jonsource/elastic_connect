import elastic_connect
import elastic_connect.data_types as data_types
import elastic_connect.data_types.base
import logging

logger = logging.getLogger(__name__)


class IntegrityError(Exception):
    pass

class Model(object):
    """
    Base class for Models stored in elasticseach.

    Handles creation, loading and saving of the models.
    Also handles simple SQL-like joins, in fact lazy loading dependent child/parent models.
    """

    __slots__ = ('id', )

    _mapping = {  # type: dict[str:elastic_connect.data_types.base.BaseDataType]
        'id': data_types.Keyword(name='id'),
    }
    """
    Dictionary describing the model.
    property_name: elasticsearch data type or 'ref' for reference to other model, defined by a join
    keys starting with _ are not saved in elasticsearch
    """

    _meta = {
        '_doc_type': 'model',
    }

    _es_namespace = elastic_connect._namespaces['_default']
    _es_connection = None

    def __init__(self, **kw):
        """
        Creates an instance of the model using \*\*kw parameters for setting values of attributes.
        The values get converted by their respective data_type.from_python method.
        """

        for property, type in self._mapping.items():
            self.__update(property, type.get_default_value())
        for property, type in self._mapping.items():
            self.__update(property, type.on_update(type.from_python(kw.get(property, type.get_default_value())), self))

    @classmethod
    def get_index(cls):
        """
        :deprecated: Returns the name of the index this model is stored in, includin any prefixes defined globally or in
            namespace.

            In ES >= 6 each model type needs it's own index.

            ES < 6 supports multiple doc_types in a single index.
        """

        return cls._es_namespace.index_prefix + cls._meta['_doc_type']

    def _compute_id(self):
        """
        Count or return stored id for this model instance.

        :return: None for unsaved models which should receive a unique id generated by Elasticsearch.
            Should be overriden and return some computed value for models which handle their uniqe id by themselves - mainly
            to keep a model parameter unique
        """

        return self.id

    @classmethod
    def get_es_connection(cls):
        """
        Initializes or returns an existing DocTypeConnection to elasticsearch for this model.

        :return: DocTypeConnection
        """
        if not cls._es_connection:
            logger.debug(cls.__name__ + " connecting to " + str(cls._es_namespace.__dict__))
            cls._es_connection = elastic_connect.DocTypeConnection(model=cls, es_namespace=cls._es_namespace,
                                                        index=cls.get_index(),
                                                        doc_type=cls._meta['_doc_type'])
            logger.debug("connection index name " + cls._es_connection.index_name)
        else:
            logger.debug(cls.__name__ + " connection already established " + str(cls._es_connection.__dict__))
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
        Create and return an unsaved model instance based on elasticsearch query result.

        :param hit: a ``hit`` from an elasticsearch query
        :return: instance of the model
        """


        kwargs = {}
        for property, type in cls._mapping.items():
            kwargs.update({property: type.from_es(hit['_source'])})
            kwargs['id'] = hit['_id']
        model = cls(**kwargs)
        return model

    @classmethod
    def create(cls, **kw) -> 'Model':
        """
        Create, save and return a model instance based on dictionary.
        Property id gets set by elasticsearch or computed depending on cls._compute_id()

        :param kw: keyword arguments describing the model's attributes
        :return: instance of the model with the ``id`` set
        """


        model = cls.from_dict(**kw)
        model.id = model._compute_id()
        ret = cls._create(model)
        return ret

    @classmethod
    def _create(cls, model):
        """
        Handles the creation of the model in elasticsearch
        Models without an id are indexed, thus receiving id from elasticsearch.
        Models with id are created. This prevents the creation of duplicates.

        :param model: the model to be created
        :return: the model with the ``id`` set
        """

        if model.id:
            response = cls.get_es_connection().create(id=model.id, body=model.serialize(exclude=['id'], flat=True))
        # TODO: probably needs to call cls.refresh() to properly prevent creation of duplicates
        else:
            logger.debug("serialize in _create %s", model.serialize(exclude=['id']))
            response = cls.get_es_connection().index(body=model.serialize(exclude=['id'], flat=True))
        model.id = response['_id']
        logger.debug("model.id from _create %s", model.id)
        model.post_save()
        return model

    def save(self):
        """
        Save a model that has an id, index a model without an id into elasticsearch.
        Saves unsaved joins recursively. Joined models, which already have an id (and thus are already present in the
        database) are not re-saved automatically. You must save them yourself if they changed.

        :return: self with dependencies updated
        """

        if self.id:
            cmp = self._compute_id()
            if cmp and cmp != self.id:
                raise IntegrityError("Can't save model with a changed computed id, create a new model")
            self.get_es_connection().update(id=self.id, body={'doc': self.serialize(exclude=['id'])})
        else:
            self.id = self._compute_id()
            if self.id:
                response = self.get_es_connection().create(id=self.id, body=self.serialize(exclude=['id'], flat=True))
            else:
                response = self.get_es_connection().index(body=self.serialize(exclude=['id'], flat=True))
                self.id = response['_id']
            logger.debug("model.id from save %s", self.id)
        return self.post_save()

    def post_save(self):
        logger.debug("post_save %s %s", self.__class__.__name__, self.id)
        ret = []
        for property, type in self._mapping.items():
            ret.append(type.on_save(model=self))
        logger.debug("post_save ret %s %s", self.id, ret)
        ret = [r for r in ret if r is not None]
        if len(ret):
            # resave, because some child models were updated
            self.save()
        return self

    def delete(self):
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
            logger.debug("pre _lazy_load %s %s", property, self.__getattribute__(property))
            self.__update(property, type.lazy_load(self))
        logger.debug("_lazy_load %s", self)
        return self

    @classmethod
    def get(cls, id):
        """
        Get a model by id from elasticsearch.

        :param id: id of the model to get
        :return: returns an instance of elastic_connect.connect.Result
        """

        ret = cls.get_es_connection().get(id=id)
        return ret

    @classmethod
    def all(cls, size=100):
        """
        Get all models from Elasticsearch.
        :param size: max number of hits to return. Default = 100.
        :return: returns an instance of elastic_connect.connect.Result
        """

        return cls.get_es_connection().search(size=size)

    @classmethod
    def find_by(cls, size=100, sort=[], search_after=None, query=None, **kw):
        """
        Search for models in Elasticsearch by attribute values.

        :example:
            model.find_by(email="test@test.cz")

        :param size: max number of hits to return. Default = 100.
        :param kw: attributes of the model by which to search
        :return: returns an instance of elastic_connect.connect.Result
        """

        if not query:
            query = kw

        append_uid = True
        for s in sort:
            if '_uid' in s:
                append_uid = False
                break

        if append_uid:
            sort.append({"_uid": "asc"})

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
        ret = cls.get_es_connection().search(body=body)
        return ret

    def serialize(self, exclude=["password"], depth=0, to_str=False, flat=False):
        """
        Serilaizes the model for storing to elasticsearch.

        Joins are flattened from join: model format to join: model.id format.
        Other attributes are serialized by their respective type.serialize method

        :param exclude: default=["password"]
        :param depth: default=0, the depth up to which models are serialized as jsons, deeper than that models are
            reduced to their id
        :param to_str: default=False, pass True if the serialization is for console output purposes
        :param flat: default=False, unsaved joined models are returned as Models if False, as None if True
        :return: json representation of the model
        """


        ret = {}
        for property, type in self._mapping.items():
            if property not in exclude:
                ret.update({property: type.serialize(self.__getattribute__(property), depth=depth, to_str=to_str, flat=flat)})
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
        Refresh the index where this model is stored to make all changes immediately visible to others.
        """

        cls._es_namespace.get_es().indices.refresh(index=cls.get_index())

    def __setattr__(self, name, value):
        if name in self._mapping:
            return self.__update(name, value)
        return super().__setattr__(name, value)

    def __update(self, name, value):
        super().__setattr__(name, self._mapping[name].on_update(value, self))
        return self

    @classmethod
    def get_es_mapping(cls):
        """
        Returns a dict representing the elastic search mapping for this model

        :return: dict
        """

        mapping = {}
        for name, type in cls._mapping.items():
            es_type = type.get_es_type()
            if name != 'id' and es_type:
                mapping[name] = {"type": es_type}

        logger.debug("mapping %s", mapping)
        return mapping
