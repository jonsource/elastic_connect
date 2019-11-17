import pytest
from elastic_connect import Model
import elastic_connect
from elastic_connect.data_types import Keyword, Date, Boolean, Long, ScaledFloat
import elasticsearch.exceptions
import datetime


class KeywordModel(Model):
    __slots__ = ('value', )

    _meta = {
        '_doc_type': 'type_keyword'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value')
    }

class DateModel(Model):
    __slots__ = ('value', )

    _meta = {
        '_doc_type': 'type_date'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Date(name='value')
    }    

class BooleanModel(Model):
    __slots__ = ('value', )

    _meta = {
        '_doc_type': 'type_boolean'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Boolean(name='value')
    } 

class LongModel(Model):
    __slots__ = ('value', )

    _meta = {
        '_doc_type': 'type_long'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Long(name='value')
    } 

class ScaledFloatModel(Model):
    __slots__ = ('value', )

    _meta = {
        '_doc_type': 'type_scaled_float'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': ScaledFloat(name='value', scaling_factor=10)
    } 

def prepare_es_for_model(cls):

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[cls])
    assert es.indices.exists(index=cls.get_index())
    print("before", cls.get_index())
    return indices


def clean_es_for_model(indices, cls, request):
    print("after")
    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    es = elastic_connect.get_es()
    assert not es.indices.exists(index=cls.get_index())


@pytest.mark.parametrize("cls, value, loaded_val, es_val", 
    [(KeywordModel, 'asdfasdf', None, None),
     (DateModel, '13.6.2019', datetime.datetime(2019, 6, 13, 0, 0), '2019-06-13T00:00:00'),
     (BooleanModel, False, None, None),
     (LongModel, 123, None, None),
     #(ScaledFloatModel, 12.325, 12.3, 123),
    ])
def test_type_on_model(request, cls, value, loaded_val, es_val):
    indices = prepare_es_for_model(cls)

    try:
        instance = cls.create(value=value)
        print("class", cls, cls.get_index())

        assert instance.id is not None

        # cls.refresh()

        es = elastic_connect.get_es()
        es_result = es.get(index=cls.get_index(), doc_type=cls.get_doctype(), id=instance.id)

        print("es: ", es_result)

        loaded = cls.get(instance.id)
        print("load: ",loaded.value)
        if loaded_val is None:
            assert value == loaded.value
        else:
            assert loaded_val == loaded.value
        if es_val is None:
            assert value == es_result['_source']['value']
        else:
            assert es_val == es_result['_source']['value']

    except Exception as e:
        raise e
    finally:
        clean_es_for_model(indices, cls, request)