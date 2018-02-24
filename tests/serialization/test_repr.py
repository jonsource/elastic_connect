from conftest import Simple, WithJoin, SelfJoin
import re


def test_simple_repr():
    s = Simple(value="12")

    assert repr(s).startswith('<conftest.Simple')
    assert repr(s).endswith('>')


def test_with_join_repr():
    s = Simple(value="12")
    wj = WithJoin(value="13", join=s)

    assert repr(wj).startswith('<conftest.WithJoin')
    assert repr(wj).endswith('>')


def test_self_join_repr():
    a = SelfJoin(value="a")
    b = SelfJoin(value="b", join=a)
    a.join = b

    assert repr(a).startswith('<conftest.SelfJoin object at ')
    assert repr(a).endswith('>')


def test_simple_id_repr():
    s = Simple(id="1", value="12")

    assert repr(s).startswith('<conftest.Simple')
    assert repr(s).endswith(">{'id': '1', 'value': '12'}")


def test_with_join_id_repr():
    s = Simple(value="12")
    wj = WithJoin(id="1", value="13", join=s)

    assert re.search(r"<conftest\.WithJoin object at 0x.*>{'id': '1', 'value': '13', 'join': '<conftest\.Simple object at 0x.*>'}",
                     repr(wj))


def test_with_join_id_simple_id_repr():
    s = Simple(id="2", value="12")
    wj = WithJoin(id="1", value="13", join=s)

    assert re.search(
        r"<conftest\.WithJoin object at 0x.*>{'id': '1', 'value': '13', 'join': '2'",
        repr(wj))


def test_self_join_id_repr():
    a = SelfJoin(value="a")
    b = SelfJoin(id="1", value="b", join=a)
    a.join = b

    assert re.search(
        r"<conftest\.SelfJoin object at 0x.*>{'id': '1', 'value': 'b', 'join': '<conftest\.SelfJoin object at 0x.*>'}",
        repr(b))


def test_self_join_id_id_repr():
    a = SelfJoin(id="2", value="a")
    b = SelfJoin(id="1", value="b", join=a)
    a.join = b

    assert re.search(
        r"<conftest\.SelfJoin object at 0x.*>{'id': '2', 'value': 'a', 'join': '1'}",
        repr(a))
