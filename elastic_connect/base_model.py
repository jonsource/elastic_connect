import elastic_connect
from .join import SingleJoin, MultiJoin
import datetime
import dateutil.parser


class Model(object):
	"""Base class for Models stored in elasticseach.

	Handles creation, loading and saving of the models.
	Also handles simple SQL-like joins, in fact lazy loading dependent child/parent models.

	Attributes:
        _mapping (dict): dictionary describing the model.
        				 property_name: elasticsearch data type or 'ref' for reference to other model, defined by a join
        				 keys starting with _ are not saved in elasticsearch
        _joins (dict): .
        es: connection to elasticsearch.
	"""

	_mapping = {
		'_doc_type': 'model',
		'id': '',
	}

	_joins = {}

	_es = None

	@classmethod
	def _get_index(cls):
		"""Returns the name of the index this model is stored in.

		For ES < 5 returns what is defined in the database settings.
		For ES >= 5 returns the '_doc_type' defined in cls._mapping
		"""
		if database.compatibility >= 5:
			return cls._mapping['_doc_type']
		else:
			return database.index

	def __init__(self, **kw):
		"""Creates an instance of the model using **kw parameters for setting values.

		properties defined as 'date' in cls._mapping are converted to datetime
		"""
		for property, type in self._mapping.items():
			if property.startswith('_'):
				continue

			self.__dict__[property] = kw.get(property, None)

			if type == 'date' and self.__dict__[property] and not isinstance(self.__dict__[property], datetime.datetime):
				self.__dict__[property] = dateutil.parser.parse(self.__dict__[property])

	def _count_id(self):
		"""Count or return stored id for this model instance.

		Returns None for unsaved models which should receive a unique id generated by Elasticsearch.
		Should be overriden and return some computed value for models which handle their uniqe id by themselves - mainly
		to keep a model parameter unique

		"""
		if self.id:
			return id
		return None

	@classmethod
	def get_es(cls):
		if not cls._es:
			cls._es = database.EsDocTypeConnection(model=cls, es=database.get_es(), index=cls._get_index(),
												   doc_type=cls._mapping['_doc_type'])
		return cls._es

	@classmethod
	def from_dict(cls, **kw):
		"""Create and return an unsaved model instance based on dictionary."""

		model = cls(**kw)
		return model

	@classmethod
	def from_es(cls, hit):
		"""Create and return an unsaved model instance based on Elasticsearch query result."""

		model = cls(**hit['_source'])
		for property, type in cls._joins.items():
			if isinstance(type, SingleJoin):
				model.__dict__[property + '_id'] = hit['_source'].get(property + '_id', None)
				continue
		model.id = hit['_id']
		return model

	@classmethod
	def create(cls, **kw):
		"""Create, save and return a model instance based on dictionary.

		Property id gets set by Elasticsearch od cumputed depending on cls._count_id()
		"""

		model = cls.from_dict(**kw)
		model.id = model._count_id()
		ret = cls._create(model)
		# print("created ", ret)
		return ret

	@classmethod
	def _create(cls, model):
		"""Handles the creation of the model in Elasticsearch

		Models without an id are indexed, thus receiving id from Elasticsearch.
		Models with id are created. This prevents the creation of duplicates.
		"""

		if model.id:
			response = cls.get_es().create(id=model.id, body=model.to_dict(exclude=['id']))
		# TODO: probably needs to call cls.refresh() to properly prevent creation of duplicates
		else:
			response = cls.get_es().index(body=model.to_dict(exclude=['id']))
		model.id = response['_id']
		return model

	def save(self):
		"""Save a model that has an id, index a model without an id into Elasticsearch"""

		if self.id:
			self.get_es().update(body=self.to_dict())
		else:
			self.get_es().index(body=self.to_dict())

	def delete(self):
		"""Delte a model from elasticsearch."""
		self.get_es().delete(id=self.id)

	def _lazy_load(self):
		"""Lazy loads model's joins - child / parent models."""

		for key in self._joins:
			self._lazy_load_join(key)
		print("lazy_loaded", self)

	def _lazy_load_join(self, join_key):
		"""Handles the loading of a single model join."""

		join = self._joins[join_key]
		if isinstance(join, MultiJoin):
			self.__dict__[join_key] = list(join.get_target().find_by(**{join.join_by: self.id}))
		else:
			self.__dict__[join_key] = join.get_target().get(self.__dict__[join_key + '_id'])

	@classmethod
	def get(cls, id):
		"""Get a model by id from Elasticsearch."""
		ret = cls.get_es().get(id=id)
		return ret

	@classmethod
	def all(cls):
		"""Get all models from Elasticsearch."""

		return cls.get_es().search()

	@classmethod
	def find_by(cls, **kw):
		"""Search for models in Elasticsearch by property values.

		For example:
			model.find_by(email="test@test.cz")
		"""

		ret = cls.get_es().search(body={
			"query": {
				"term": kw
			}
		})
		return ret

	def to_dict(self, exclude=["password"]):
		"""Serilaizes the model for storing to Elasticsearch.

		Joins are transformed from join: model format to join_id: id format.
		Datetime attributes are converted to iso format.
		"""

		ret = {}
		for property, type in self._mapping.items():
			if not property.startswith('_') and property not in exclude:
				if isinstance(type, SingleJoin):
					val = self.__dict__.get(property, None)
					if val:
						ret[property + '_id'] = val.id
					else:
						ret[property + '_id'] = self.__dict__.get(property + '_id', None)
					continue

				if type == 'datetime':
					ret[property] = self.__dict__.get(property).to_iso()

				ret[property] = self.__dict__.get(property, None)
				try:
					ret[property] = ret[property].to_dict()
				except AttributeError:
					pass
		return ret

	def __repr__(self):
		return object.__repr__(self) + str(self.to_dict())

	def __str__(self):
		return str(self.__dict__)

	@classmethod
	def refresh(cls):
		"""Refresh the index where this model is stored to make all changes immediately visible to others."""

		database.get_es().indices.refresh(index=cls._get_index())

	def __getattr__(self, name):
		return self.__dict__.get(name, None)

	@classmethod
	def _add_join(cls, name, join_class, **kw):
		"""Handles the creation of this model's joins."""

		cls._mapping[name] = join_class(source=cls, **kw)
		cls._joins[name] = cls._mapping[name]

	@classmethod
	def add_single_join(cls, name, **kw):
		cls._add_join(name, SingleJoin, **kw)

	@classmethod
	def add_multi_join(cls, name, **kw):
		cls._add_join(name, MultiJoin, **kw)
