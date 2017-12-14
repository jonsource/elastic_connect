import pytest
from elastic_connect.base_model import Model
from elastic_connect.join import SingleJoin
import elastic_connect


class One(Model):
    _mapping = {
        '_doc_type': 'model_one',
        'id': '',
        'value': 'keyword',
        'dependant': SingleJoin('dependant', target='test_join.Two')
    }

class Two(Model):
    _mapping = {
        '_doc_type': 'model_two',
        'id': '',
        'value': 'keyword'
    }


@pytest.fixture(scope="module")
def fix_model_one(fix_model_two):

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One])
    assert es.indices.exists(index='model_one')

    yield One

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_one')


@pytest.fixture(scope="module")
def fix_model_two():

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[Two])
    assert es.indices.exists(index='model_two')

    yield Two

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_two')


def test_single_join(fix_model_one, fix_model_two):
    two = fix_model_two.create(value='two_val')
    one = fix_model_one.create(value='one_val', dependant=two)

    fix_model_one.refresh()
    fix_model_two.refresh()

    load = fix_model_one.get(one.id)
    load._lazy_load()

    assert load.dependant.id == two.id

    load.save()
    load = fix_model_one.get(one.id)
    print(load)
    load._lazy_load()
    print(load)
    assert load.dependant.id == two.id
