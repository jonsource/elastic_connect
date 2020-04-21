from abc import ABC
import datetime
from dateutil import parser


class BaseDataType(ABC):

    def __init__(self, name=None):
        self.name = name

    def from_python(self, value):
        return value

    def serialize(self, value, depth, to_str, flat):
        return value

    def deserialize(self, value):
        return value

    def from_es(self, es_hit):
        return self.deserialize(es_hit.get(self.name, None))

    def to_es(self, value):
        return self.serialize(value)

    def lazy_load(self, model):
        return model.__getattribute__(self.name)

    def _has_es_type(self):
        if self.name == 'id':
            return False
        return True

    def _get_es_type(self):
        return {'type': self.__class__.__name__.lower()}

    def get_es_type(self):
        return self._has_es_type() and self._get_es_type()

    def get_default_value(self):
        return None

    def on_save(self, model):
        return None

    def on_update(self, value, model):
        return value

    def include_in_flat(self):
        return True

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

    def serialize(self, value, depth, to_str, flat):
        return value.isoformat()


class Boolean(BaseDataType):

    def from_python(self, value):
        return value == True  # noqa E712


class Integer(BaseDataType):
    pass


class Long(BaseDataType):
    pass


class ScaledFloat(BaseDataType):

    def __init__(self, scaling_factor, name=None):
        self.name = name
        self.scaling_factor = scaling_factor

    def _get_es_type(self):
        return {'type': 'scaled_float',
                'scaling_factor': self.scaling_factor
                }
