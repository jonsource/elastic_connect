import pytest
from elastic_connect import Model
import elastic_connect
from elastic_connect.data_types import Keyword


def test_delete_indices():
    class One(Model):
        _meta = {
            '_doc_type': 'model_one'
        }
        _mapping = {
            'id': Keyword(name='id')
        }

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One])
    assert es.indices.exists(index=pytest.config.getoption("--es-prefix") + '_' + 'model_one')
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=pytest.config.getoption("--es-prefix") + '_' + 'model_one')
