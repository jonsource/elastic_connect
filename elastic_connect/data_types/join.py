from .base import BaseDataType
import importlib
import logging

logger = logging.getLogger(__name__)


class BadJoinTargetError(Exception):
    pass


class Join(BaseDataType):
    """Abstract parent of model joins - dependent child / parent models."""

    def __init__(self, source: str, target: str, name: str = None):

        super().__init__(name=name)

        self.source_def = source
        self.target_def = target
        self.source = None
        self.target = None
        self._source_module, self._source = self.split_class_def(source)
        (self._target_module,
         self._target,
         self.target_property) = self.parse_join_target(target)

    @staticmethod
    def split_class_def(class_def: str):
        module, class_name = class_def.rsplit('.', 1)
        return module, class_name

    @staticmethod
    def class_for_name(module_name: str, class_name: str):
        """
        Handles import of target model class. Needed to prevent circular
        dependency.
        """
        m = importlib.import_module(module_name)
        c = getattr(m, class_name)
        return c

    @staticmethod
    def parse_join_target(join_target: str):
        target_property = None
        parts = join_target.split(':')
        if len(parts) == 2:
            target_property = parts[1]
        module, class_name = Join.split_class_def(parts[0])
        return module, class_name, target_property

    def get_target(self):
        """
        Gets the target model of the join.
        """

        if not self.target:
            # get class from string - to avoid circular imports and
            # class self reference
            try:
                self.target = self.class_for_name(self._target_module,
                                                  self._target)
            except ModuleNotFoundError:
                message = ("Cannot find module %s, to get %s. "
                           "Check the mapping of field '%s' "
                           "in model %s.")
                message = message % (self._target_module, self._target,
                                     self.name, self._source)
                raise BadJoinTargetError(message)
            except AttributeError:
                message = ("Cannot find class %s, in module %s. "
                           "Check the mapping of field '%s' "
                           "in model %s.")
                message = message % (self._target, self._target_module,
                                     self.name, self._source)
                raise BadJoinTargetError(message)
        return self.target

    def get_source(self):
        """Gets the source model of the join."""

        if not self.source:
            # get class from string - to avoid circular imports and
            # class self reference
            self.source = self.class_for_name(self._source_module,
                                              self._source)
        return self.source

    def _get_es_type(self):
        return {'type': 'keyword'}

    def insert_reference(self,
                         value: 'base_model.Model',   # noqa: F821
                         model: 'base_model.Model'):  # noqa: F821
        return None

    def include_in_flat(self):
        return False

    def _is_value_model(self, value):
        if value is None or isinstance(value, str):
            return False
        return True

    def class_params(self):
        params = super().class_params()
        params['source'] = self.source_def
        params['target'] = self.target_def
        return params


class SingleJoin(Join):
    """
    1:1 model join.
    """

    def lazy_load(self, model):
        try:
            value = model.__getattribute__(self.name).get_id()
        except AttributeError:
            value = model.__getattribute__(self.name)
        loaded = self.get_default_value()
        if value:
            loaded = self.get_target().get(value)
        return loaded

    def serialize(self,
                  value: (str, 'base_model.Model'),  # noqa: F821
                  depth: int,
                  to_str: bool = False,
                  flat: bool = True):
        if depth < 1:
            try:
                if value.get_id():
                    ret = value.get_id()
                else:
                    if to_str:
                        ret = object.__repr__(value)
                    else:
                        if flat:
                            return None
                        ret = value
            except (AttributeError, TypeError):
                ret = value
        else:
            ret = value.serialize(depth=depth - 1)
        logger.debug("serialize single %s", ret)
        return ret

    def on_update(self,
                  value: 'base_model.Model',   # noqa: F821
                  model: 'base_model.Model'):  # noqa: F821
        if self.target_property and self._is_value_model(value):
            logger.debug("SingleJoin::on_update %s.%s = %s -> %s",
                         model.__class__.__name__,
                         self.name, value,
                         self.name)
            target_type = value._mapping[self.target_property]
            target_type.insert_reference(model, value)
        return super().on_update(value, model)

    def insert_reference(self,
                         value: 'base_model.Model',   # noqa: F821
                         model: 'base_model.Model'):  # noqa: F821
        logger.debug("SingleJoin::insert_reference %s %s",
                     self.name,
                     value.get_id())
        model.__setattr__(self.name, value)

    def on_save(self, model):
        value = model.__getattribute__(self.name)
        logger.debug("SingleJoin::on_save %s %s %s",
                     self.name,
                     model.get_id(),
                     value and (hasattr(value, 'id') and value.get_id()))
        if value and hasattr(value, 'id') and value.get_id() is None:
            logger.debug("SingleJoin::on_save - saving")
            value.save()
            return value
        return None


class MultiJoin(Join):
    """1:N model join."""

    def __init__(self, source: str, target: str, join_by=None,
                 name: str = None):
        super(MultiJoin, self).__init__(name=name,
                                        source=source,
                                        target=target)
        self.join_by = join_by

    def get_join_by(self):
        if not self.join_by:
            self.join_by = self.get_source()._mapping['_name'] + '_id'
        return self.join_by

    def lazy_load(self, model):
        try:
            value = [v.get_id() for v in model.__getattribute__(self.name)]
        except AttributeError:
            value = model.__getattribute__(self.name)
        return self.get_target().get(value)

    def serialize(self,
                  value: (str, 'base_model.Model'),  # noqa: F821
                  depth: int,
                  to_str: bool = False,
                  flat: bool = True):
        ret = [SingleJoin.serialize(self, value=model, depth=depth,
                                    to_str=to_str, flat=flat)
               for model in value]
        logger.debug("MultiJoin::serialize %s", ret)
        return ret

    def get_default_value(self):
        return []

    def on_update(self,
                  value: 'list[base_model.Model]',  # noqa: F821
                  model: 'base_model.Model'):       # noqa: F821
        if self.target_property:
            logger.debug("MultiJoin::on_update %s.%s = %s -> %s",
                         model.__class__.__name__,
                         self.name,
                         value,
                         self.name)
            for val in value:
                if not self._is_value_model(val):
                    continue
                target_type = val._mapping[self.target_property]
                target_type.insert_reference(model, val)
        return super().on_update(value, model)

    def insert_reference(self,
                         value: 'base_model.Model',   # noqa: F821
                         model: 'base_model.Model'):  # noqa: F821
        logger.debug("MultiJoin::insert_reference %s.%s = %s",
                     model, self.name, value)
        referred_attribute = model.__getattribute__(self.name)
        referred_ids = [r.get_id() for r in referred_attribute
                        if self._is_value_model(r)]
        if value.get_id() not in referred_ids:
            referred_attribute.append(value)

    def on_save(self, model: 'base_model.Model'):  # noqa: F821
        logger.debug("MultiJoin::on_save %s %s", self.name, model.get_id())
        ret = []
        values = model.__getattribute__(self.name)
        initialized_values = [v for v in values
                              if v and hasattr(v, 'id') and v.get_id() is None]
        for value in initialized_values:
            ret.append(value.save())
        if len(ret):
            return ret
        return None

    def deserialize(self, value):
        if value is None:
            return []
        return value

    def class_params(self):
        params = super().class_params()
        params['join_by'] = self.join_by
        return params


class LooseJoin(Join):
    def to_es(self, value):
        return {}

    def lazy_load(self, value: str):
        return {self.name: self.get_default_value()}

    def _has_es_type(self):
        return False

    def from_es(self, es_hit):
        return self.get_default_value()


class SingleJoinLoose(SingleJoin, LooseJoin):

    def __init__(self, source: str, target: str, name: str = None,
                 do_lazy_load=False):
        super().__init__(name=name, source=source, target=target)
        self.do_lazy_load = do_lazy_load

    def lazy_load(self, value):
        if not self.do_lazy_load:
            logger.debug("SingleJoinLoose::lazy_load %s of %s skipped" %
                         (self.name, value))
            return None

        assert self.target_property
        target = self.get_target()
        find_by = {self.target_property: value.get_id()}
        try:
            ret = target.find_by(**find_by, size=1)[0]
        except IndexError:
            ret = None
        return ret

    def class_params(self):
        params = super().class_params()
        params['do_lazy_load'] = self.do_lazy_load
        return params


class MultiJoinLoose(MultiJoin, LooseJoin):
    """
    Important! Dosen't preserve order!
    """

    def __init__(self, source: str, target: str, name: str = None,
                 join_by=None, do_lazy_load=False):
        super().__init__(name=name, source=source, target=target)
        self.do_lazy_load = do_lazy_load

    def lazy_load(self, value):
        if not self.do_lazy_load:
            logger.debug("MultiJoinLoose::lazy_load %s of %s skipped" %
                         (self.name, value))
            return []

        assert self.target_property
        target = self.get_target()
        find_by = {self.target_property: value.get_id()}
        try:
            print("to_Version_entry", value.to_version_entry())
        except:
            print("no to_version_entry")
        print("lazyloading into: %r:%r target: %s .find_by: %s" % (self.get_source(), value, target, find_by))
        ret = target.find_by(**find_by)
        return ret

    def class_params(self):
        params = super().class_params()
        params['do_lazy_load'] = self.do_lazy_load
        return params
