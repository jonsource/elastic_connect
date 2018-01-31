from abc import ABC
import datetime
from dateutil import parser


class BaseDataType(ABC):

    def __init__(self, name):
        self.name = name

    def from_python(self, value):
        return value

    def serialize(self, value):
        return value

    def deserialize(self, value):
        return value

    def from_es(self, es_hit):
        return self.deserialize(es_hit.get(self.name, None))

    def to_dict(self, value):
        return {self.name: value}

    def to_es(self, value):
        return self.to_dict(self.serialize(value))

    def lazy_load(self, value):
        return {self.name: value}

    def _has_es_type(self):
        if self.name == 'id':
            return False
        return True

    def _get_es_type(self):
        return self.__class__.__name__.lower()

    def get_es_type(self):
        return self._has_es_type() and self._get_es_type()

    def on_update(self, value, model):
        return self.to_dict(value)

    def get_default_value(self):
        return None

    def on_save(self, model):
        return None

    def __repr__(self):
        return object.__repr__(self) + str(self.__dict__)


class Keyword(BaseDataType):
    pass


class Text(BaseDataType):
    pass


class Date(BaseDataType):

    def from_python(self, value):
        if not isinstance(value, datetime.datetime) and value is not None:
            value = self.deserialize(value)
        return super().from_python(value)

    def deserialize(self, value):
        return parser.parse(value)

    def serialize(self, value):
        return value.isoformat()
