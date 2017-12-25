import pytest
from elastic_connect.base_model import Model
from elastic_connect.join import SingleJoin, MultiJoin
import elastic_connect


class Parent(Model):
    _mapping = {
        '_doc_type': 'model_parent',
        'id': '',
        'value': 'keyword',
        'dependant': SingleJoin(source='test_join.Parent', target='test_join.Child')
    }


class Child(Model):
    _mapping = {
        '_doc_type': 'model_child',
        'id': '',
        'value': 'keyword'
    }


class One(Model):
    _mapping = {
        '_doc_type': 'model_one',
        'id': '',
        'value': 'keyword',
        'many': MultiJoin(source='test_join.One', target='test_join.Many'),
    }


class Many(Model):
    _mapping = {
        '_doc_type': 'model_many',
        'id': '',
        'value': 'keyword',
        'one': SingleJoin(source='test_join.Many', target='test_join.One'),
    }


@pytest.fixture(scope="module")
def fix_parent_child():
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[Parent, Child])
    assert es.indices.exists(index='model_parent')
    assert es.indices.exists(index='model_child')
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_parent')
    assert not es.indices.exists(index='model_child')


@pytest.fixture(scope="module")
def fix_one_many():
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One, Many])
    assert es.indices.exists(index='model_one')
    assert es.indices.exists(index='model_many')
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_one')
    assert not es.indices.exists(index='model_many')


def test_single_join(fix_parent_child):
    child = Child.create(value='two_val')
    parent = Parent.create(value='one_val', dependant=child)

    Parent.refresh()
    Child.refresh()

    loaded = Parent.get(parent.id)
    loaded._lazy_load()

    assert loaded.dependant.id == child.id

    loaded.save()
    loaded = Parent.get(parent.id)
    print(loaded)
    loaded._lazy_load()
    print(loaded)
    assert loaded.dependant.id == child.id


def test_multi_join(fix_one_many):
    many1 = Many.create(value='one')
    many2 = Many.create(value='two')
    one = One.create(value='boss', many=[many1, many2])
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
    child = Child.create(value='two_val')
    parent = Parent.create(value='one_val')

    parent.dependant = child
    parent.save()
    print("saved", parent.__dict__)
    Child.refresh()
    Parent.refresh()

    loaded = Parent.get(parent.id)
    print("loaded", loaded.__dict__)
    loaded._lazy_load()
    assert loaded.dependant.id == child.id


def test_multi_join_save(fix_one_many):
    many1 = Many.create(value='one')
    many2 = Many.create(value='two')

    one = One.create(value='boss')
    one.many = [many1, many2]
    one.save()
    One.refresh()
    Many.refresh()

    loaded = One.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id
    assert loaded.many[1].id == many2.id
