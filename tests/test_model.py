import pytest
from elastic_connect import Model
import elastic_connect
from elastic_connect.data_types import Keyword

@pytest.fixture(scope="module")
def fix_model_one_save():

    class OneSave(Model):
        __slots__ = ('value', )

        _meta = {
            '_doc_type': 'model_save_one'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value')
        }

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[OneSave])
    assert es.indices.exists(index=OneSave.get_index())

    yield OneSave

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=OneSave.get_index())


def test_save(fix_model_one_save):
    cls = fix_model_one_save

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
