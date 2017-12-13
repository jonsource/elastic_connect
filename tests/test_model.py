import pytest
from elastic_connect import Model
import elastic_connect

@pytest.fixture(scope="module")
def fix_model_one():

    class One(Model):
        _mapping = {
            '_doc_type': 'model_one',
            'id': '',
            'value': 'keyword'
        }

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One])
    assert es.indices.exists(index='model_one')

    yield One

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index='model_one')


def test_save(fix_model_one):
    cls = fix_model_one

    instance = cls.create(value='value1')
    cls.refresh()

    instance2 = cls.get(instance.id)
    assert instance2.id == instance.id
    assert instance2.value == 'value1'
    print("instance1", instance)
    print("instance2", instance2)

    instance.value = 'value2'
    instance.save()

    instance2 = cls.get(instance.id)
    assert instance2.id == instance.id
    assert instance2.value == 'value2'
