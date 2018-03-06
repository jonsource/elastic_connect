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

@pytest.mark.skipif(pytest.config.getoption("--index-noclean"), reason="not cleaning indices")
def test_delete_indices():
    # if pytest.config.getoption("--index-noclean"):
    #     print("Skipping test - not cleaning indices")
    #     return
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One])
    assert es.indices.exists(index=pytest.config.getoption("--es-prefix") + '_' + 'model_one')
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=pytest.config.getoption("--es-prefix") + '_' + 'model_one')


def test_create_indices():
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One, Two])
    assert len(indices) == 2
    assert es.indices.get_mapping(One.get_index()) == {'test_model_one': {'mappings': {'test_model_one': {'properties': {'single': {'type': 'keyword'}}}}}}
    assert es.indices.get_mapping(Two.get_index()) == {'test_model_two': {'mappings': {'test_model_two': {}}}}