import elastic_connect
import elastic_connect.data_types as data_types


class Model(object):
    """Base class for Models stored in elasticseach.

    Handles creation, loading and saving of the models.
    Also handles simple SQL-like joins, in fact lazy loading dependent child/parent models.

    Attributes:
        _mapping (dict): dictionary describing the model.
                         property_name: elasticsearch data type or 'ref' for reference to other model, defined by a join
                         keys starting with _ are not saved in elasticsearch
        _es: connection to elasticsearch.
    """

    __slots__ = ('id', )

    _mapping = {
        'id': data_types.Keyword(name='id'),
    }

    _meta = {
        '_doc_type': 'model',
    }

    print(elastic_connect._namespaces)
    _es_namespace = elastic_connect._namespaces['_default']
    _es_connection = None

    def __init__(self, **kw):
        """Creates an instance of the model using **kw parameters for setting values.

        properties defined as 'date' in cls._mapping are converted to datetime
        """
        for property, type in self._mapping.items():
            self.__update(type.to_dict(type.from_python(kw.get(property, type.get_default_value()))))

    @classmethod
    def get_index(cls):
        """Returns the name of the index this model is stored in.

        For ES < 5 returns what is defined in the database settings.
        For ES >= 5 returns the '_doc_type' defined in cls._mapping
        """

        if elastic_connect.compatibility >= 5:
            return cls._es_namespace.index_prefix + cls._meta['_doc_type']
        else:
            return cls._es_namespace.index_prefix + elastic_connect.index

    def _compute_id(self):
        """Count or return stored id for this model instance.

        Returns None for unsaved models which should receive a unique id generated by Elasticsearch.
        Should be overriden and return some computed value for models which handle their uniqe id by themselves - mainly
        to keep a model parameter unique

        """

        return self.id

    @classmethod
    def get_es_connection(cls):
        if not cls._es_connection:
            print(cls.__name__ + " connecting to " + str(cls._es_namespace.__dict__))
            cls._es_connection = elastic_connect.DocTypeConnection(model=cls, es_namespace=cls._es_namespace,
                                                        index=cls.get_index(),
                                                        doc_type=cls._meta['_doc_type'])
            print("connection index name " + cls._es_connection.index_name)
        else:
            print(cls.__name__ + " connection already established:", cls._es_connection.__dict__)
        return cls._es_connection

    @classmethod
    def from_dict(cls, **kw):
        """Create and return an unsaved model instance based on dictionary."""

        model = cls(**kw)
        return model

    @classmethod
    def from_es(cls, hit):
        """Create and return an unsaved model instance based on Elasticsearch query result."""

        # model = cls(**hit['_source'])  # it;s better to create an empty model, but is it always possible ?
        model = cls()
        for property, type in cls._mapping.items():
            model.__update(type.to_dict(type.from_es(hit['_source'])))
        model.id = hit['_id']
        return model

    @classmethod
    def create(cls, **kw) -> 'cls':
        """Create, save and return a model instance based on dictionary.

        Property id gets set by Elasticsearch or computed depending on cls._compute_id()
        """

        model = cls.from_dict(**kw)
        model.id = model._compute_id()
        ret = cls._create(model)
        return ret

    @classmethod
    def _create(cls, model):
        """Handles the creation of the model in Elasticsearch

        Models without an id are indexed, thus receiving id from Elasticsearch.
        Models with id are created. This prevents the creation of duplicates.
        """

        if model.id:
            response = cls.get_es_connection().create(id=model.id, body=model.to_es(exclude=['id']))
        # TODO: probably needs to call cls.refresh() to properly prevent creation of duplicates
        else:
            response = cls.get_es_connection().index(body=model.to_es(exclude=['id']))
        model.id = response['_id']
        return model

    def save(self):
        """Save a model that has an id, index a model without an id into Elasticsearch"""

        if self.id:
            self.get_es_connection().update(id=self.id, body={'doc': self.to_es(exclude=['id'])})
        else:
            self.get_es_connection().index(body=self.to_es(exclude=['id']))

    def delete(self):
        """Delete a model from elasticsearch."""
        self.get_es_connection().delete(id=self.id)

    def _lazy_load(self):
        """Lazy loads model's joins - child / parent models."""
        for property, type in self._mapping.items():
            if property + '_id' not in self.__slots__:
                continue
            self.__update(type.lazy_load(self.__getattribute__(property + '_id')))
        print("_lazy_loaded:", self)
        return self

    @classmethod
    def get(cls, id):
        """Get a model by id from Elasticsearch."""
        ret = cls.get_es_connection().get(id=id)
        return ret

    @classmethod
    def all(cls):
        """Get all models from Elasticsearch."""

        return cls.get_es_connection().search()

    @classmethod
    def find_by(cls, **kw):
        """Search for models in Elasticsearch by property values.

        For example:
            model.find_by(email="test@test.cz")
        """

        ret = cls.get_es_connection().search(body={
            "query": {
                "term": kw
            }
        })
        return ret

    def to_es(self, exclude=["password"]):
        """Serilaizes the model for storing to Elasticsearch.

        Joins are transformed from join: model format to join_id: id format.
        Datetime attributes are converted to iso format.
        """

        ret = {}
        for property, type in self._mapping.items():
            if property not in exclude:
                ret.update(type.to_es(self.__getattribute__(property)))
        return ret

    def __repr__(self):
        return object.__repr__(self) + str(self.to_es())

    def __str__(self):
        return str(self.to_es())

    @classmethod
    def refresh(cls):
        """Refresh the index where this model is stored to make all changes immediately visible to others."""

        cls._es_namespace.get_es().indices.refresh(index=cls.get_index())

    def __setattr__(self, name, value):
        if name in self._mapping:
            self.__update(self._mapping[name].on_update(value, self))
            return
        return super().__setattr__(name, value)

    def __update(self, value):
        for key, val in value.items():
            super().__setattr__(key, val)

    def _set_reference(self, name, value):
        current = self.__getattribute__(name)
        if type(current) == list and value.id not in [o.id for o in current]:
            return current.append(value)
        return super().__setattr__(name, value)

    @classmethod
    def get_es_mapping(cls):
        """
        Returns a dict representing the elastic search mapping for this model
        :return: dict
        """

        mapping = {}
        for name, type in cls._mapping.items():
            if name != 'id':
                mapping[name] = {"type": type.get_es_type()}

        print("mapping", mapping)
        return mapping
