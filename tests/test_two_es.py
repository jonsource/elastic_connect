import pytest
from elastic_connect import Model
import elastic_connect
from elastic_connect.data_types import Keyword

@pytest.fixture(scope="module", autouse=True)
def fix_es_namespace():
    elastic_connect.es_conf['second'] = {'es_conf': 'localhost:18400'}
    elastic_connect.es_conf['second']['index_prefix'] = pytest.config.getoption("--es-prefix") + '_'

    yield

    elastic_connect.es_conf.pop('second', None)

@pytest.fixture(scope="module")
def fix_model_one_save():

    class OneSave(Model):
        _meta = {
            '_doc_type': 'model_save_one'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value')
        }

    es = elastic_connect.get_es(namespace=OneSave._es_namespace)
    indices = elastic_connect.create_mappings(model_classes=[OneSave], namespace=OneSave._es_namespace)
    assert es.indices.exists(index=OneSave.get_index())

    yield OneSave

    elastic_connect.delete_indices(indices=indices, namespace=OneSave._es_namespace)
    assert not es.indices.exists(index=OneSave.get_index())

@pytest.fixture(scope="module")
def fix_model_two_save():

    class TwoSave(Model):
        _es_namespace = 'second'
        _meta = {
            '_doc_type': 'model_save_one'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value')
        }

    es = elastic_connect.get_es(namespace=TwoSave._es_namespace)
    indices = elastic_connect.create_mappings(model_classes=[TwoSave], namespace=TwoSave._es_namespace)
    assert es.indices.exists(index=TwoSave.get_index())

    yield TwoSave

    elastic_connect.delete_indices(indices=indices, namespace=TwoSave._es_namespace)
    assert not es.indices.exists(index=TwoSave.get_index())


def test_save(fix_model_one_save, fix_model_two_save):
    cls1 = fix_model_one_save
    cls2 = fix_model_two_save

    print("conf", elastic_connect.es_conf)
    print(cls2.get_index())
    instance1 = cls1.create(id=1, value='value1')
    instance2 = cls2.create(id=1, value='value2')
    print("conf", elastic_connect.es_conf)
    cls1.refresh()
    cls2.refresh()

    loaded1 = cls1.get(instance1.id)
    loaded2 = cls2.get(instance2.id)
    assert loaded1.id == '1'
    assert loaded1.value == 'value1'
    assert loaded2.id == '1'
    assert loaded2.value == 'value2'
    print("instance1", instance1)
    print("instance2", instance2)
