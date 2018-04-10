import pytest
from elastic_connect.base_model import Model
from elastic_connect.data_types import Keyword
import elastic_connect.namespace
import ujson as json


class Slots(Model):
    __slots__ = ('id', 'value')

    _meta = {
        '_doc_type': 'model_child',
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value')
    }


class NoSlots(Model):
    _meta = {
        '_doc_type': 'model_child',
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value')
    }


@pytest.fixture(scope="module")
def SimpleSlots():
    return Slots


@pytest.fixture(scope="module")
def SimpleNoSlots():
    return NoSlots


@pytest.fixture(scope="module")
def CreatedSlots(SimpleSlots, second_namespace):
    return second_namespace.register_model_class(SimpleSlots)


@pytest.fixture(scope="module")
def CreatedNoSlots(SimpleNoSlots, second_namespace):
    return second_namespace.register_model_class(SimpleNoSlots)


def test_simple_model(SimpleSlots, SimpleNoSlots):
    s = SimpleSlots(value="12")
    assert json.dumps(s) == '{"id":null,"value":"12"}'
    with pytest.raises(AttributeError):
        s.__dict__ == None

    s = SimpleNoSlots(value="12")
    assert json.dumps(s) == '{"id":null,"value":"12"}'
    assert s.__dict__ == {'value': '12'}


def test_created_model(CreatedSlots, CreatedNoSlots):
    s = CreatedSlots(value="12")
    assert json.dumps(s) == '{"id":null,"value":"12"}'
    with pytest.raises(AttributeError):
        s.__dict__ == None

    s = CreatedNoSlots(value="12")
    assert json.dumps(s) == '{"id":null,"value":"12"}'
    assert s.__dict__ == {'value': '12'}


def test_slots_raise_when_slot_is_missing(SimpleSlots):
    one = SimpleSlots(value="val")
    with pytest.raises(AttributeError):
        one.not_in_slots = 'fail'


def test_no_slots_dont_raise_when_missing(SimpleNoSlots):
    one = SimpleNoSlots(value="val")
    one.not_in_slots = 'ok'
    assert one.not_in_slots == 'ok'
