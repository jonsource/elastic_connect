from .base import BaseDataType
import importlib


class Join(BaseDataType):
    """Abstract parent of model joins - dependent child / parent models."""

    def __init__(self, name: str, source: str, target: str):

        super().__init__(name=name)

        self.source = None
        self.target = None
        self._source_module, self._source = self.split_class_def(source)
        self._target_module, self._target, self.target_property = self.parse_join_target(target)

    @staticmethod
    def split_class_def(class_def: str):
        module, class_name = class_def.rsplit('.', 1)
        return module, class_name

    @staticmethod
    def class_for_name(module_name: str, class_name: str):
        """Handles import of target model class. Needed to prevent circular dependency."""
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

    def _get_es_type(self):
        return 'keyword'

    def insert_reference(self, value: 'base_model.Model', model: 'base_model.Model'):
        return None

    def include_in_flat(self):
        return False

    def _is_value_model(self, value):
        if value is None or isinstance(value, str):
            return False
        return True


class SingleJoin(Join):
    """1:1 model join."""

    def lazy_load(self, model):
        try:
            value = model.__getattribute__(self.name).id
        except AttributeError:
            value = model.__getattribute__(self.name)
        loaded = self.get_default_value()
        if value:
            loaded = self.get_target().get(value)
        return loaded

    def serialize(self, value: (str, 'base_model.Model'), depth: int, to_str: bool = False, flat: bool = True):
        #print("serialize single", self.name, depth)
        if depth < 1:
            try:
                if value.id:
                    #print("value.id")
                    ret = value.id
                else:
                    #print("value")
                    if to_str:
                        ret = object.__repr__(value)
                    else:
                        if flat:
                            return None
                        ret = value
            except (AttributeError, TypeError):
                ret = value
        else:
            #print("recourse")
            ret = value.serialize(depth=depth-1)
        print("serialize single", ret)
        return ret

    def on_update(self, value: 'base_model.Model', model: 'base_model.Model'):
        if self.target_property and self._is_value_model(value):
            print("single::on_update %s.%s = %s -> %s" % (model, self.name, value, self.name))
            target_type = value._mapping[self.target_property]
            target_type.insert_reference(model, value)
        return super().on_update(value, model)

    def insert_reference(self, value: 'base_model.Model', model: 'base_model.Model'):
        print("single::insert_reference", self.name, value.id)
        model.__setattr__(self.name, value)

    def on_save(self, model):
        value = model.__getattribute__(self.name)
        print("single::on_save", self.name, model.id, value and value.id)
        if value and value.id is None:
            print("single::on_save, saving")
            value.save()
            return value
        return None


class MultiJoin(Join):
    """1:N model join."""

    def __init__(self, name: str, source: str, target: str, join_by=None):
        super(MultiJoin, self).__init__(name=name, source=source, target=target)
        self.join_by = join_by

    def get_join_by(self):
        if not self.join_by:
            self.join_by = self.get_source()._mapping['_name'] + '_id'
        return self.join_by

    def lazy_load(self, model):
        try:
            value = [v.id for v in model.__getattribute__(self.name)]
        except AttributeError:
            value = model.__getattribute__(self.name)
        return [self.get_target().get(val) for val in value]

    def serialize(self, value: (str, 'base_model.Model'), depth: int, to_str: bool = False, flat: bool = True):
        ret = [SingleJoin.serialize(self, value=model, depth=depth, to_str=to_str, flat=flat) for model in value]
        print("serialize multi", ret)
        return ret

    def get_default_value(self):
        return []

    def on_update(self, value: 'list[base_model.Model]', model: 'base_model.Model'):
        if self.target_property:
            # print("multi::on_update %s.%s = %s -> %s" % (model, self.name, value, self.name))
            for val in value:
                if not self._is_value_model(val):
                    continue
                target_type = val._mapping[self.target_property]
                target_type.insert_reference(model, val)
        return super().on_update(value, model)

    def insert_reference(self, value: 'base_model.Model', model: 'base_model.Model'):
        print("multi::insert_reference %s.%s = %s" % (model, self.name, value))
        #print("multi::insert_reference", self.name, value.id, model)
        referred_attribute = model.__getattribute__(self.name)
        if value.id not in [r.id for r in referred_attribute if self._is_value_model(r)]:
            referred_attribute.append(value)

    def on_save(self, model: 'base_model.Model'):
        print("multi::on_save", self.name, model.id)
        ret = []
        values = model.__getattribute__(self.name)
        for value in [v for v in values if v and v.id is None]:
            ret.append(value.save())
        if len(ret):
            return ret
        return None

    def deserialize(self, value):
        if value is None:
            return []
        return value


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
    def to_dict(self, value: any):
        to_es = super().to_es(value)
        to_es.update({self.name: value})
        return to_es


class MultiJoinLoose(MultiJoin, LooseJoin):
    def to_dict(self, value: any):
        to_es = super().to_es(value)
        to_es.update({self.name: value})
        return to_es
