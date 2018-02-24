from conftest import Simple, WithJoin, SelfJoin
import re


def test_simple_str():
    s = Simple(value="12")

    assert str(s) == "{'id': None, 'value': '12'}"


def test_with_join_str():
    s = Simple(value="12")
    wj = WithJoin(value="13", join=s)

    assert re.search(r"{'id': None, 'value': '13', 'join': '<conftest\.Simple object at 0x.*>'}",
                     str(wj))


def test_self_join_str():
    a = SelfJoin(value="a")
    b = SelfJoin(value="b", join=a)
    a.join = b

    assert re.search(r"{'id': None, 'value': 'a', 'join': '<conftest\.SelfJoin object at 0x.*>'}",
                     str(a))


def test_simple_id_str():
    s = Simple(id="1", value="12")

    assert str(s) == "{'id': '1', 'value': '12'}"


def test_with_join_id_str():
    s = Simple(value="12")
    wj = WithJoin(id="1", value="13", join=s)

    assert re.search(r"{'id': '1', 'value': '13', 'join': '<conftest\.Simple object at 0x.*>'}",
                     str(wj))


def test_with_join_id_simple_id_str():
    s = Simple(id="2", value="12")
    wj = WithJoin(id="1", value="13", join=s)

    assert str(wj) == "{'id': '1', 'value': '13', 'join': '2'}"


def test_self_join_id_str():
    a = SelfJoin(value="a")
    b = SelfJoin(id="1", value="b", join=a)
    a.join = b

    assert re.search(
        r"{'id': '1', 'value': 'b', 'join': '<conftest\.SelfJoin object at 0x.*>'}",
        str(b))


def test_self_join_id_id_str():
    a = SelfJoin(id="2", value="a")
    b = SelfJoin(id="1", value="b", join=a)
    a.join = b

    assert str(a) == "{'id': '2', 'value': 'a', 'join': '1'}"
