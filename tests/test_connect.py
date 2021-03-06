import pytest
from elastic_connect import Model
import elastic_connect
from elastic_connect.data_types import Keyword, SingleJoinLoose, SingleJoin


class One(Model):
    _meta = {
        '_doc_type': 'model_one'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'single': SingleJoin(name='single', source='test_connect.One', target='test_connect.Two')
    }


class Two(Model):
    _meta = {
        '_doc_type': 'model_two'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'loose': SingleJoinLoose(name='loose', source='test_connect.Two', target='test_connect.One')
    }

@pytest.fixture
def mappings_one_two(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One, Two])
    assert len(indices) == 2

    yield

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
    else:
        elastic_connect.delete_indices(indices=indices)
        assert not es.indices.exists(index=One.get_index())
        assert not es.indices.exists(index=Two.get_index())


def test_create_indices(mappings_one_two):
    es = elastic_connect.get_es()

    # explicitly check for proper index and doc_type names
    expect_one = {'test_model_one': {
                    'mappings': {
                      'model_one': {
                        'properties': {
                          'single': {'type': 'keyword'}
                        }
                      }
                    }
                  }
                }
    assert es.indices.get_mapping(One.get_index()) == expect_one

    expect_two = {'test_model_two': {'mappings': {'model_two': {}}}}
    assert es.indices.get_mapping(Two.get_index()) == expect_two


@pytest.mark.skip_on_index_noclean
def test_delete_indices(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One, Two])
    assert es.indices.exists(index=request.config.getoption("--es-prefix") + '_' + 'model_one')
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=request.config.getoption("--es-prefix") + '_' + 'model_one')
