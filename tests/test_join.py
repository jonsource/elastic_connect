import pytest
from elastic_connect.base_model import Model
from elastic_connect.data_types import Keyword, SingleJoin, MultiJoin
import elastic_connect


class Parent(Model):
    __slots__ = ('id', 'value', 'dependant', 'dependant_id')

    _meta = {
        '_doc_type': 'model_parent'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'dependant': SingleJoin(name='dependant', source='test_join.Parent', target='test_join.Child')
    }


class Child(Model):
    __slots__ = ('id', 'value')

    _meta = {
        '_doc_type': 'model_child'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value')
    }


class One(Model):
    __slots__ = ('id', 'value', 'many', 'many_id')

    _meta = {
        '_doc_type': 'model_one'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'many': MultiJoin(name='many', source='test_join.One', target='test_join.Many'),
    }


class Many(Model):
    __slots__ = ('id', 'value', 'one', 'one_id')

    _meta = {
        '_doc_type': 'model_many'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'one': SingleJoin(name='one', source='test_join.Many', target='test_join.One'),
    }


@pytest.fixture(scope="module")
def fix_parent_child():
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[Parent, Child])
    assert es.indices.exists(index='model_parent')
    assert es.indices.exists(index='model_child')

    yield

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_parent')
    assert not es.indices.exists(index='model_child')


@pytest.fixture(scope="module")
def fix_one_many():
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One, Many])
    assert es.indices.exists(index='model_one')
    assert es.indices.exists(index='model_many')

    yield

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_one')
    assert not es.indices.exists(index='model_many')


def test_single_join(fix_parent_child):
    child = Child.create(value='two_val')  # type: Child
    parent = Parent.create(value='one_val', dependant=child)  # type: Parent
    print("parent", parent)

    Parent.refresh()
    Child.refresh()

    loaded = Parent.get(parent.id)
    print("loaded", loaded)
    loaded._lazy_load()
    print("loaded", loaded)

    assert loaded.dependant.id == child.id

    loaded.save()
    loaded = Parent.get(parent.id)
    print(loaded)
    loaded._lazy_load()
    print(loaded)
    assert loaded.dependant.id == child.id


def test_multi_join(fix_one_many):
    many1 = Many.create(value='one')  # type: Many
    many2 = Many.create(value='two')  # type: Many
    one = One.create(value='boss', many=[many1, many2])  # type: One
    print("one", one)
    One.refresh()
    Many.refresh()

    loaded = One.get(one.id)
    print("loaded", loaded)
    loaded._lazy_load()

    print("lazy_loaded", loaded)
    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id
    assert loaded.many[1].id == many2.id


def test_single_join_save(fix_parent_child):
    child = Child.create(value='two_val')  # type: Child
    parent = Parent.create(value='one_val')  # type: Parent

    parent.dependant = child
    parent.save()

    print("saved", parent.to_es())
    Child.refresh()
    Parent.refresh()

    loaded = Parent.get(parent.id)
    print("loaded", loaded.to_es())
    loaded._lazy_load()
    assert loaded.dependant.id == child.id


def test_multi_join_save(fix_one_many):
    many1 = Many.create(value='one')  # type: Many
    many2 = Many.create(value='two')  # type: Many

    one = One.create(value='boss')  # type: One
    one.many = [many1, many2]
    one.save()
    One.refresh()
    Many.refresh()

    loaded = One.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id
    assert loaded.many[1].id == many2.id
