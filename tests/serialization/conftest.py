from elastic_connect.base_model import Model
from elastic_connect.data_types import Keyword, SingleJoin, MultiJoin


class Simple(Model):
    __slots__ = ('id', 'value')

    _meta = {
        '_doc_type': 'model_child',
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value')
    }


class WithJoin(Model):
    __slots__ = ('id', 'value', 'join')

    _meta = {
        '_doc_type': 'model_with_join',
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'join': SingleJoin(name='join', source='test_serialize.WithJoin', target='test.serialize.Simple')
    }


class SelfJoin(Model):
    __slots__ = ('id', 'value', 'join')

    _meta = {
        '_doc_type': 'model_self_join',
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'join': SingleJoin(name='join', source='test_serialize.SelfJoin', target='test.serialize.SelfJoin')
    }


class WithMultiJoin(Model):
    __slots__ = ('id', 'value', 'join')

    _meta = {
        '_doc_type': 'model_with_join',
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'join': MultiJoin(name='join', source='test_serialize.WithMultiJoin', target='test.serialize.Simple')
    }
