from abc import ABC
import importlib


class Join(ABC):
    """Abstract parent of model joins - dependent child / parent models."""

    def __init__(self, source, target):

        def split_classdef(classdef):
            module, class_name = classdef.rsplit('.', 1)
            return None, module, class_name

        self.source, self._source_module, self._source = split_classdef(source)
        self.target, self._target_module, self._target = split_classdef(target)

    @staticmethod
    def class_for_name(module_name, class_name):
        """Handles import of target model class. Needed to prevent circular dependency."""
        m = importlib.import_module(module_name)
        c = getattr(m, class_name)
        return c

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


class SingleJoin(Join):
    """1:1 model join."""

    pass


class MultiJoin(Join):
    """1:N model join."""

    def __init__(self, source, target, join_by=None):
        super(MultiJoin, self).__init__(source=source, target=target)
        self.join_by = join_by

    def get_join_by(self):
        if not self.join_by:
            self.join_by = self.get_source()._mapping['_name'] + '_id'
        return self.join_by
