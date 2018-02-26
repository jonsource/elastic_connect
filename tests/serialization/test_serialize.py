from conftest import Simple, WithJoin, SelfJoin, WithMultiJoin
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


def test_with_multi_join_serialize_empty():
    wj = WithMultiJoin(value="13")
    assert wj.serialize() == {"id": None, "value": "13", "join": []}


def test_with_multi_join_id_serialize_simple_id():
    s1 = Simple(id="2", value="12")
    s2 = Simple(id="3", value="22")
    wmj = WithMultiJoin(id="1", value="13", join=[s1, s2])

    assert wmj.serialize() == {"id": "1", "join": ['2', '3'], "value": "13"}
    assert wmj.serialize(depth=1) == {"id": "1", "join": [{'id': '2', 'value': '12'}, {'id': '3', 'value': '22'}], "value": "13"}


def test_with_multi_join_serialize():
    s1 = Simple(value="12")
    s2 = Simple(value="22")
    wmj = WithMultiJoin(value="13", join=[s1, s2])
    wmj_ser = wmj.serialize()

    assert wmj_ser["id"] is None
    assert wmj_ser["value"] == "13"
    assert len(wmj_ser["join"]) == 2
    assert isinstance(wmj_ser["join"][0], Simple)
    assert isinstance(wmj_ser["join"][1], Simple)
