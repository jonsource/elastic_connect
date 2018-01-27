from .base import BaseDataType
import importlib


class Join(BaseDataType):
    """Abstract parent of model joins - dependent child / parent models."""

    def __init__(self, name, source, target):

        super().__init__(name=name)

        self.id_value = None

        self.source = None
        self.target = None
        self._source_module, self._source = self.split_class_def(source)
        self._target_module, self._target, self.target_property = self.parse_join_target(target)

    @staticmethod
    def split_class_def(class_def):
        module, class_name = class_def.rsplit('.', 1)
        return module, class_name

    @staticmethod
    def class_for_name(module_name, class_name):
        """Handles import of target model class. Needed to prevent circular dependency."""
        m = importlib.import_module(module_name)
        c = getattr(m, class_name)
        return c

    @staticmethod
    def parse_join_target(join_target):
        target_property = None
        parts = join_target.split(':')
        if len(parts) == 2:
            target_property = parts[1]
        module, class_name = Join.split_class_def(parts[0])
        return module, class_name, target_property

    def get_target(self):
        """Gets the target model of the join."""

        if not self.target:
            # get class from string - to avoid circular imports and class self reference
            self.target = self.class_for_name(self._target_module, self._target)
        return self.target

    def get_source(self):
        """Gets the source model of the join."""

        if not self.source:
            # get class from string - to avoid circular imports and class self reference
            self.source = self.class_for_name(self._source_module, self._source)
        return self.source

    def from_es(self, es_hit):
        return es_hit.get(self.name + '_id', None)

    def to_dict(self, value):
        to_es = self.to_es(value)
        to_es.update({self.name: value})
        return to_es

    def _get_es_type(self):
        return 'keyword'

class SingleJoin(Join):
    """1:1 model join."""
    def lazy_load(self, value):
        return {self.name: self.get_target().get(value)}

    def to_es(self, value):
        try:
            id = value.id
        except (AttributeError, TypeError):
            id = value

        return {self.name + '_id': id}

    def on_update(self, value, model):
        print("updating single model:%s:%s value:%s" % (model, self.target_property, value))
        if self.target_property:
            value._set_reference(self.target_property, model)
        return super().on_update(value, model)

class MultiJoin(Join):
    """1:N model join."""

    def __init__(self, name, source, target, join_by=None):
        super(MultiJoin, self).__init__(name=name, source=source, target=target)
        self.join_by = join_by

    def get_join_by(self):
        if not self.join_by:
            self.join_by = self.get_source()._mapping['_name'] + '_id'
        return self.join_by

    def lazy_load(self, value):
        return {self.name: [self.get_target().get(val) for val in value]}

    def to_es(self, value):
        try:
            ids = [model.id for model in value]
        except (AttributeError, TypeError):
            ids = value
        return {self.name + '_id': ids}

    def on_update(self, value, model):
        print("updating multi model:%s:%s value:%s" % (model, self.target_property, value))
        if self.target_property:
            for val in value:
                val._set_reference(self.target_property, model)
        return super().on_update(value, model)

    def get_default_value(self):
        return []
