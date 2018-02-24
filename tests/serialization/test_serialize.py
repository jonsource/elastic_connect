from conftest import Simple, WithJoin, SelfJoin
import re


def test_simple_serialize():
    s = Simple(value="12")

    assert s.serialize() == {"id": None, "value": "12"}


def test_with_join_serialize():
    s = Simple(value="12")
    wj = WithJoin(value="13", join=s)
    wj_ser = wj.serialize()

    assert wj_ser["id"] is None
    assert wj_ser["value"] == "13"
    assert isinstance(wj_ser["join"], Simple)


def test_with_join_serialize_empty():
    wj = WithJoin(value="13")
    assert wj.serialize() == {"id": None, "value": "13", "join": None}


def test_with_join_serialize_id_simple_id():
    s = Simple(id="2", value="12")
    wj = WithJoin(id="1", value="13", join=s)

    assert wj.serialize() == {"id": "1", "join": "2", "value": "13"}
    assert wj.serialize(depth=1) == {"id": "1", "join": {"id": "2", "value": "12"}, "value": "13"}


def test_simple_id_serialize():
    s = Simple(id="1", value="12")

    assert s.serialize() == {'id': "1", 'value': '12'}


def test_self_join_id_serialize():
    a = SelfJoin(value="a")
    b = SelfJoin(id="1", value="b", join=a)
    a.join = b

    b_ser = b.serialize()
    assert b_ser['id'] == '1'
    assert b_ser['value'] == 'b'
    assert isinstance(b_ser['join'], SelfJoin)

    b_ser = b.serialize(depth=1)
    assert b_ser['id'] == '1'
    assert b_ser['value'] == 'b'
    assert not isinstance(b_ser['join'], SelfJoin)
    assert isinstance(b_ser['join'], dict)
    assert b_ser['join']['id'] == None
    assert b_ser['join']['value'] == 'a'
    assert b_ser['join']['join'] == '1'


def test_self_join_id_id_serialize():
    a = SelfJoin(id="2", value="a")
    b = SelfJoin(id="1", value="b", join=a)
    a.join = b

    b_ser = b.serialize()
    assert b_ser['id'] == '1'
    assert b_ser['value'] == 'b'
    assert b_ser['join'] == '2'

    b_ser = b.serialize(depth=1)
    assert b_ser['id'] == '1'
    assert b_ser['value'] == 'b'
    assert not isinstance(b_ser['join'], SelfJoin)
    assert isinstance(b_ser['join'], dict)
    assert b_ser['join']['id'] == '2'
    assert b_ser['join']['value'] == 'a'
    assert b_ser['join']['join'] == '1'
