from abc import ABC
import datetime
import dateutil


class BaseDataType(ABC):

    def __init__(self, name):
        self.name = name

    def from_python(self, value):
        return value

    def from_es(self, es_hit):
        return self.from_python(es_hit.get(self.name, None))

    def to_dict(self, value):
        return {self.name: value}

    def to_es(self, value):
        return self.to_dict(value)

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

    def __repr__(self):
        return object.__repr__(self) + str(self.to_dict())


class Keyword(BaseDataType):
    pass


class Text(BaseDataType):
    pass


class Date(BaseDataType):

    def from_python(self, value):
        if not isinstance(value, datetime.datetime):
            value = dateutil.parser.parse(value)
        return super().from_python(value)

    def to_es(self, value):
        return super().to_es(value.to_iso())
