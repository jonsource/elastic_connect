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

    if pytest.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=OneSave.get_index())


def test_create(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(value='value1')  # type: Model

    assert instance.id is not None

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), id=instance.id)

    assert es_result['found'] == True
    assert es_result['_source'] == {'value': 'value1'}


def test_get(fix_model_one_save):
    cls = fix_model_one_save

    es = elastic_connect.get_es()
    es_result = es.index(index=cls.get_index(), doc_type=cls._meta['_doc_type'], body={'value': 'pokus'}, refresh=True)

    assert es_result['created'] == True

    print(es_result)

    instance = cls.get(es_result['_id'])

    assert instance.id == es_result['_id']
    assert instance.value == 'pokus'


def test_save(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(value='value1')
    cls.refresh()

    instance2 = cls.get(instance.id)
    assert instance2.id == instance.id
    assert instance2.value == 'value1'

    instance.value = 'value2'
    instance.save()

    instance3 = cls.get(instance.id)
    assert instance3.id == instance.id
    assert instance3.value == 'value2'


def test_create_with_id(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(id='100', value='value1')  # type: Model

    assert instance.id == '100'

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), id=instance.id)

    assert es_result['found'] is True
    assert es_result['_source'] == {'value': 'value1'}
