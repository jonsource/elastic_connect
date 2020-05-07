import pytest
from elastic_connect.advanced_model import StampedModel
import elastic_connect
from elastic_connect.data_types import Keyword, Date
import elasticsearch.exceptions


@pytest.fixture(scope="module")
def fix_stamped_model(request):

    class StampedOne(StampedModel):
        __slots__ = ('value', )

        _meta = {
            '_doc_type': 'stamped_one'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value'),
            # move to a class method of StampedModel
            'created_at': Date(name='created_at'),
            'updated_at': Date(name='updated_at'),
        }

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[StampedOne])
    assert es.indices.exists(index=StampedOne.get_index())

    yield StampedOne

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=StampedOne.get_index())


def test_stamped_model_mapping(request, fix_stamped_model):
    StampedOne = fix_stamped_model()
    expected_mapping = {'created_at': {'type': 'date'},
                        'updated_at': {'type': 'date'},
                        'value': {'type': 'keyword'}}

    mapping = StampedOne.get_es_mapping()
    assert mapping == expected_mapping


def test_stamped_model_create(fix_stamped_model):
    cls = fix_stamped_model

    now = ''

    instance = cls.create(value='value1')  # type: StampedModel

    assert instance.id is not None
    assert instance.created_at == now
    assert instance.updated_at == None

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), doc_type=cls.get_doctype(), id=instance.id)

    assert es_result['found'] == True
    assert es_result['_source'] == {'value': 'value1'}


def test_get(fix_model_one_save):
    cls = fix_model_one_save

    es = elastic_connect.get_es()
    es_result = es.index(index=cls.get_index(), doc_type=cls.get_doctype(), body={'value': 'pokus'}, refresh=True)

    assert es_result['created'] == True

    instance = cls.get(es_result['_id'])

    assert instance.id == es_result['_id']
    assert instance.value == 'pokus'