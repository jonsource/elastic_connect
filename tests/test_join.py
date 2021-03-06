import pytest
from elastic_connect.base_model import Model
from elastic_connect.data_types import Keyword, SingleJoin, MultiJoin, SingleJoinLoose, MultiJoinLoose
import elastic_connect


class Parent(Model):
    __slots__ = ('id', 'value', 'child')

    _meta = {
        '_doc_type': 'model_parent'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'child': SingleJoin(name='child', source='test_join.Parent', target='test_join.Child')
    }


class Child(Model):
    __slots__ = ('id', 'value', 'parent')

    _meta = {
        '_doc_type': 'model_child'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'parent': SingleJoin(name='parent', source='test_join.Child', target='test_join.Parent')
    }


class One(Model):
    __slots__ = ('id', 'value', 'many', 'many_id')

    _meta = {
        '_doc_type': 'model_one'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'many': MultiJoin(name='many', source='test_join.One', target='test_join.Many'),
    }


class Many(Model):
    __slots__ = ('id', 'value', 'one', 'one_id')

    _meta = {
        '_doc_type': 'model_many'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'one': SingleJoin(name='one', source='test_join.Many', target='test_join.One'),
    }


class OneWithReference(Model):
    __slots__ = ('id', 'value', 'many')

    _meta = {
        '_doc_type': 'model_one_wr'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'many': MultiJoin(name='many', source='test_join.OneWithReference', target='test_join.ManyWithReference:one'),
    }


class ManyWithReference(Model):
    __slots__ = ('id', 'value', 'one')

    _meta = {
        '_doc_type': 'model_many_wrid'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'one': SingleJoin(name='one', source='test_join.ManyWithReference', target='test_join.OneWithReference:many'),
    }


class IdOneWithReference(Model):
    __slots__ = ('id', 'value', 'many')

    _meta = {
        '_doc_type': 'model_one_wrid'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'many': MultiJoin(name='many', source='test_join.IdOneWithReference', target='test_join.IdManyWithReference:one'),
    }

    def _compute_id(self):
        return self.value


class IdManyWithReference(Model):
    __slots__ = ('id', 'value', 'one')

    _meta = {
        '_doc_type': 'model_many_wr'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'one': SingleJoin(name='one', source='test_join.IdManyWithReference', target='test_join.IdOneWithReference:many'),
    }


class User(Model):
    __slots__ = ('value', 'key', 'key_id', 'keys', 'keys_id')

    _meta = {
        '_doc_type': 'model_user'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'key': SingleJoinLoose(name='key', source='test_join.User', target='test_join.Key:user', do_lazy_load=True),
        'keys': MultiJoinLoose(name='keys', source='test_join.User', target='test_join.Key:user', do_lazy_load=True),
    }


class UserNoLoad(Model):
    __slots__ = ('value', 'key', 'key_id', 'keys', 'keys_id')

    _meta = {
        '_doc_type': 'model_user_no_load'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'key': SingleJoinLoose(name='key', source='test_join.User', target='test_join.Key:user'),
        'keys': MultiJoinLoose(name='keys', source='test_join.User', target='test_join.Key:user'),
    }


class Key(Model):
    __slots__ = ('value', 'user', 'user_id')

    _meta = {
        '_doc_type': 'model_key'
    }
    _mapping = {
        'id': Keyword(name='id'),
        'value': Keyword(name='value'),
        'user': SingleJoin(name='user', source='test_join.Key', target='test_join.User'),
    }


@pytest.fixture(scope="module")
def fix_parent_child(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[Parent, Child])

    assert es.indices.exists(index=Parent.get_index())
    assert es.indices.exists(index=Child.get_index())

    yield

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=Parent.get_index())
    assert not es.indices.exists(index=Child.get_index())


@pytest.fixture(scope="module")
def fix_one_many(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[One, Many])
    assert es.indices.exists(index=One.get_index())
    assert es.indices.exists(index=Many.get_index())

    yield

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=One.get_index())
    assert not es.indices.exists(index=Many.get_index())

@pytest.fixture(scope="module")
def fix_one_many_with_reference(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[OneWithReference, ManyWithReference])
    assert es.indices.exists(index=OneWithReference.get_index())
    assert es.indices.exists(index=ManyWithReference.get_index())

    yield

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=OneWithReference.get_index())
    assert not es.indices.exists(index=ManyWithReference.get_index())


@pytest.fixture(scope="module")
def fix_id_one_many_with_reference(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[IdOneWithReference, IdManyWithReference])
    assert es.indices.exists(index=IdOneWithReference.get_index())
    assert es.indices.exists(index=IdManyWithReference.get_index())

    yield

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=IdOneWithReference.get_index())
    assert not es.indices.exists(index=IdManyWithReference.get_index())


@pytest.fixture(scope="module")
def fix_user_key(request):
    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[User, UserNoLoad, Key])

    assert es.indices.exists(index=User.get_index())
    assert es.indices.exists(index=UserNoLoad.get_index())
    assert es.indices.exists(index=Key.get_index())

    yield

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return
    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=User.get_index())
    assert not es.indices.exists(index=UserNoLoad.get_index())
    assert not es.indices.exists(index=Key.get_index())


def test_single_join(fix_parent_child):
    child = Child.create(value='child_val')  # type: Child
    parent = Parent.create(value='parent_val', child=child)  # type: Parent

    print("--------------")

    Parent.refresh()
    Child.refresh()

    # don't make two way references by default
    assert child.parent is None
    assert parent.child.id == child.id

    loaded = Parent.get(parent.id)
    loaded._lazy_load()

    # test loading of joined model
    assert loaded.child.id == child.id

    # resave and try loading again
    loaded.save()
    loaded = Parent.get(parent.id)
    loaded._lazy_load()
    assert loaded.child.id == child.id


def test_single_join_empty(fix_parent_child):
    parent = Parent.create(value='parent_val')  # type: Parent

    Parent.refresh()

    loaded = Parent.get(parent.id)
    loaded._lazy_load()

    # test loading of empty join
    assert loaded.child is None


def test_multi_join(fix_one_many):
    many1 = Many.create(value='one_slave')  # type: Many
    many2 = Many.create(value='two_sakve')  # type: Many
    one = One.create(value='boss', many=[many1, many2])  # type: One

    assert many1.one is None
    assert many2.one is None

    One.refresh()
    Many.refresh()

    loaded = One.get(one.id)
    loaded._lazy_load()

    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id
    assert loaded.many[1].id == many2.id
    assert loaded.many[0].one is None
    assert loaded.many[1].one is None


def test_multi_join_empty(fix_one_many):
    one = One.create(value='boss')  # type: One

    One.refresh()

    loaded = One.get(one.id)
    loaded._lazy_load()

    assert len(loaded.many) == 0


def test_single_join_explicit_save(fix_parent_child):
    child = Child.create(value='child_val')  # type: Child
    parent = Parent.create(value='parent_val')  # type: Parent

    parent.child = child
    parent.save()

    print("saved", parent.serialize())
    Child.refresh()
    Parent.refresh()

    loaded = Parent.get(parent.id)  # type: Parent
    print("loaded", loaded.serialize())
    loaded._lazy_load()
    assert loaded.child.id == child.id


def test_multi_join_explicit_save(fix_one_many):
    many1 = Many.create(value='child_val1')  # type: Many
    many2 = Many.create(value='child_val2')  # type: Many

    one = One.create(value='boss')  # type: One
    one.many = [many1, many2]
    one.save()
    One.refresh()
    Many.refresh()

    loaded = One.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id
    assert loaded.many[1].id == many2.id


def test_single_join_implicit_save(fix_parent_child):
    child = Child(value='child_val')  # type: Child
    parent = Parent.create(value='parent_val', child=child)  # type: Parent

    print("saved", parent)
    Child.refresh()
    Parent.refresh()

    loaded = Parent.get(parent.id)  # type: Parent
    print("loaded", loaded)
    loaded._lazy_load()
    assert loaded.child.id == child.id


def test_multi_join_implicit_save(fix_one_many):
    many1 = Many(value='one')  # type: Many
    many2 = Many(value='two')  # type: Many

    one = One.create(value='boss', many=[many1, many2])  # type: One

    print("one", one, one.many)

    m1 = one.many[0]
    m2 = one.many[1]

    assert many1.id == m1.id
    assert many2.id == m2.id

    One.refresh()
    Many.refresh()

    loaded = One.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    assert loaded.many[0].id == m1.id
    assert loaded.many[1].id == m2.id


def test_multi_join_reference(fix_one_many_with_reference):
    many1 = ManyWithReference.create(value='one')  # type: ManyWithReference
    many2 = ManyWithReference.create(value='two')  # type: ManyWithReference

    assert many1.one is None
    assert many2.one is None
    one = OneWithReference.create(value='boss')  # type: OneWithReference
    one.many = [many1, many2]

    assert many1.one.id == one.id
    assert many2.one.id == one.id

    one.save()
    many1.save()
    many2.save()
    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = OneWithReference.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    lm1 = loaded.many[0]
    lm2 = loaded.many[1]
    assert lm1.one.id == loaded.id
    assert lm2.one.id == loaded.id


def test_multi_join_reference_implicit_save(fix_one_many_with_reference):
    many1 = ManyWithReference(value='one')  # type: ManyWithReference
    many2 = ManyWithReference(value='two')  # type: ManyWithReference

    one = OneWithReference.create(value='boss', many=[many1, many2])  # type: OneWithReference

    assert many1.one.id == one.id
    assert many2.one.id == one.id

    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = OneWithReference.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    lm1 = loaded.many[0]
    lm2 = loaded.many[1]
    assert lm1.one.id == loaded.id
    assert lm2.one.id == loaded.id


def test_multi_join_reverse_reference_implicit_save(fix_one_many_with_reference):
    one = OneWithReference.create(value='boss')  # type: OneWithReference

    assert one.many == []

    many1 = ManyWithReference.create(one=one, value='one')  # type: ManyWithReference
    many2 = ManyWithReference.create(one=one, value='two')  # type: ManyWithReference

    assert len(one.many) == 2

    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = OneWithReference.get(one.id)
    loaded._lazy_load()
    assert loaded.many == []

    one.save()
    OneWithReference.refresh()

    loaded = OneWithReference.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2


def test_single_join_reference(fix_one_many_with_reference):
    one = OneWithReference.create(value='boss')  # type: OneWithReference
    many = ManyWithReference.create(value='slave')  # type: ManyWithReference

    assert one.many == []
    many.one = one
    assert len(one.many)
    assert one.many[0].id == many.id

    one.save()
    many.save()
    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = ManyWithReference.get(many.id)
    loaded._lazy_load()
    o = loaded.one
    assert o.id == one.id


def test_single_join_reference_implicit_save(fix_one_many_with_reference):
    one = OneWithReference(value='boss')  # type: OneWithReference
    many = ManyWithReference.create(value='slave', one=one)  # type: ManyWithReference

    assert one.many[0].id == many.id
    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = ManyWithReference.get(many.id)
    loaded._lazy_load()
    assert loaded.one.id == one.id


def test_loose_join_mapping():
    assert User.get_es_mapping() == {'value': {'type': 'keyword'}}


def test_single_join_loose(fix_user_key):
    u = User.create(value='pepa')  # type: User
    k1 = Key.create(value='111', user=u)  # type: Key

    u.key = k1
    u.save()
    assert u.key.id == k1.id

    User.refresh()
    Key.refresh()

    lu = User.get(u.id)  # type: User
    lu._lazy_load()
    assert lu.key.id == k1.id


def test_single_join_loose_empty(fix_user_key):
    u = User.create(value='pepa')  # type: User

    u.save()
    assert u.key is None

    User.refresh()

    lu = User.get(u.id)  # type: User
    lu._lazy_load()
    assert lu.key is None


def test_single_join_loose_no_load(fix_user_key):
    u = UserNoLoad.create(value='pepa')  # type: UserNoLoad
    k1 = Key.create(value='111', user=u)  # type: Key
    
    u.key = k1
    u.save()
    assert u.key.id == k1.id

    UserNoLoad.refresh()
    Key.refresh()

    lu = UserNoLoad.get(u.id)
    lu._lazy_load()
    assert lu.key is None

    lk = Key.find_by(user=lu.id)
    assert lk


def test_single_join_loose_implicit_reference(fix_user_key):
    u = User.create(value='pepa', key=Key(value='111'))  # type: User
        
    u.save()
    assert u.key.id

    User.refresh()
    Key.refresh()

    lu = User.get(u.id)  # type: User
    lu._lazy_load()
    assert lu.key.id == u.key.id


def test_multi_join_loose(fix_user_key):
    u = User.create(value='pepa')  # type: User
    k1 = Key.create(value='111', user=u)  # type: Key
    k2 = Key.create(value='222', user=u)  # type: Key

    u.keys = [k1, k2]
    u.save()
    assert u.keys[0].id == k1.id
    assert u.keys[1].id == k2.id

    User.refresh()
    Key.refresh()

    lu = User.get(u.id)  # type: User
    lu._lazy_load()
    assert len(lu.keys) == 2
    ids = [key.id for key in lu.keys]
    # doesn't keep order
    # assert lu.keys[0].id == u.keys[0].id
    # assert lu.keys[1].id == u.keys[1].id
    assert u.keys[0].id in ids
    assert u.keys[1].id in ids


def test_multi_join_loose_empty(fix_user_key):
    u = User.create(value='pepa')  # type: User

    u.save()
    assert len(u.keys) == 0

    User.refresh()
    Key.refresh()

    lu = User.get(u.id)  # type: User
    lu._lazy_load()
    assert len(lu.keys) == 0


def test_multi_join_loose_no_load(fix_user_key):
    u = UserNoLoad.create(value='pepa')  # type: User
    k1 = Key.create(value='111', user=u)  # type: Key
    k2 = Key.create(value='222', user=u)  # type: Key

    u.keys = [k1, k2]
    u.save()
    assert u.keys[0].id == k1.id
    assert u.keys[1].id == k2.id

    UserNoLoad.refresh()
    Key.refresh()

    lu = UserNoLoad.get(u.id)  # type: User
    lu._lazy_load()
    assert lu.keys == []

    lk = Key.find_by(user=lu.id)
    assert len(lk) == 2


def test_multi_join_loose_implicit_reference(fix_user_key):
    keys = [Key(value='111'), Key(value='222')]
    u = User.create(value='pepa', keys=keys)  # type: User

    u.save()
    assert u.keys[0].id
    assert u.keys[1].id

    User.refresh()
    Key.refresh()

    lu = User.get(u.id)  # type: User
    lu._lazy_load()
    assert len(lu.keys) == 2
    ids = [key.id for key in lu.keys]
    # doesn't keep order
    # assert lu.keys[0].id == u.keys[0].id
    # assert lu.keys[1].id == u.keys[1].id
    assert u.keys[0].id in ids
    assert u.keys[1].id in ids


def test_multi_join_reference_double_load(fix_one_many_with_reference):
    many1 = ManyWithReference(value='one')  # type: ManyWithReference
    many2 = ManyWithReference(value='two')  # type: ManyWithReference

    one = OneWithReference.create(value='boss', many=[many1, many2])  # type: OneWithReference

    assert many1.one.id == one.id
    assert many2.one.id == one.id

    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = OneWithReference.get(one.id)
    loaded._lazy_load()
    assert len(loaded.many) == 2
    assert loaded.many[0].one.id == one.id
    assert loaded.many[1].one.id == one.id

    loaded._lazy_load()
    assert len(loaded.many) == 2
    assert loaded.many[0].one.id == one.id
    assert loaded.many[1].one.id == one.id


def test_multi_join_reference_implicit_save_computed_id(fix_id_one_many_with_reference):
    many1 = IdManyWithReference(value='one')  # type: IdManyWithReference
    many2 = IdManyWithReference(value='two')  # type: IdManyWithReference

    assert many1.id is None

    one = IdOneWithReference.create(value='boss', many=[many1, many2])  # type: IdOneWithReference

    # check computed id
    assert one.id == one.value

    # check id of joined model gets updated
    assert many1.id is not None

    IdOneWithReference.refresh()
    IdManyWithReference.refresh()

    loaded = IdOneWithReference.get(one.id)
    loaded._lazy_load()

    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id


def test_single_join_unloaded_resave(fix_id_one_many_with_reference):
    one = OneWithReference(value='boss')  # type: OneWithReference
    many = ManyWithReference.create(value='slave', one=one)  # type: ManyWithReference

    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = ManyWithReference.get(many.id)
    assert loaded.value == 'slave'
    with pytest.raises(AttributeError):
        loaded.one[0].id == one.id
    assert loaded.one == one.id

    loaded.value = 'slave2'
    loaded.save()

    loaded = ManyWithReference.get(many.id)
    loaded._lazy_load()
    assert loaded.value == 'slave2'
    assert loaded.one.id == one.id


def test_multi_join_unloaded_resave(fix_id_one_many_with_reference):
    many1 = ManyWithReference(value='one')  # type: ManyWithReference
    many2 = ManyWithReference(value='two')  # type: ManyWithReference

    one = OneWithReference.create(value='boss', many=[many1, many2])  # type: OneWithReference

    OneWithReference.refresh()
    ManyWithReference.refresh()

    loaded = OneWithReference.get(one.id)
    assert loaded.value == 'boss'
    assert len(loaded.many) == 2
    with pytest.raises(AttributeError):
        loaded.many[0].id == many1.id
    assert loaded.many[0] == many1.id

    loaded.value = "boss2"
    loaded.save()

    loaded = OneWithReference.get(one.id)
    loaded._lazy_load()
    assert loaded.value == 'boss2'
    assert len(loaded.many) == 2
    assert loaded.many[0].id == many1.id
    assert loaded.many[1].id == many2.id
