from elastic_connect.base_model import Model
from elastic_connect.data_types import Keyword
import ujson as json

class Simple(Model):
    _mapping = {
        '_doc_type': 'model_child',
        'id': Keyword(name='id'),
        'value': Keyword(name='value')
    }


def test_simple_serialize():
    s = Simple(value="12")
    assert json.dumps(s) == '{"id":null,"value":"12"}'
