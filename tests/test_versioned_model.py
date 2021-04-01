import pytest
from elastic_connect.base_model import Model
from elastic_connect.advanced_model import VersionedModel
from elastic_connect import advanced_model
import elastic_connect
from elastic_connect.data_types import Keyword, Date, MultiJoinLoose, SingleJoin
import elasticsearch.exceptions
from datetime import datetime, timedelta
from elasticsearch.exceptions import NotFoundError


@pytest.fixture(scope="module")
def fix_versioned_model(request):

    class VersionedOne(VersionedModel):
        __slots__ = ('value', )

        _meta = {
            '_doc_type': 'versioned_one',
            '_load_version': True,
        }
        _mapping = VersionedModel.model_mapping(value=Keyword())

    es = elastic_connect.get_es()
    VersionClass = VersionedOne.get_version_class()
    indices = elastic_connect.create_mappings(model_classes=[VersionedOne, VersionClass])
    assert es.indices.exists(index=VersionedOne.get_index())
    assert es.indices.exists(index=VersionClass.get_index())

    yield VersionedOne

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=VersionedOne.get_index())
    assert not es.indices.exists(index=VersionClass.get_index())

class VersionedParent(VersionedModel):
        __slots__ = ('value', 'child')

        _meta = {
            '_doc_type': 'versioned_parent',
            '_load_version': True,
        }
        _mapping = VersionedModel.model_mapping(
            value=Keyword(),
            child=MultiJoinLoose(
                source='test_versioned_model.VersionedParent',
                target='test_versioned_model.Child:parent',
                do_lazy_load=True
                )
        )

class Child(Model):
    __slots__ = ('value', 'parent')

    _meta = {
        '_doc_type': 'child',
        '_load_version': True,
    }
    _mapping = Model.model_mapping(
        value=Keyword(),
        parent=SingleJoin(
            source='test_versioned_model.Child:parent',
            target='test_versioned_model.VersionedParent'
        )
    )

@pytest.fixture(scope="module")
def fix_versioned_model_with_join(request):

    es = elastic_connect.get_es()
    VersionClass = VersionedParent.get_version_class()
    indices = elastic_connect.create_mappings(model_classes=[VersionedParent, VersionClass, Child])
    assert es.indices.exists(index=VersionedParent.get_index())
    assert es.indices.exists(index=VersionClass.get_index())
    assert es.indices.exists(index=VersionClass.get_index())

    yield VersionedParent, Child

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=VersionedParent.get_index())
    assert not es.indices.exists(index=Child.get_index())


@pytest.fixture(autouse=True)
def delete_all_instances(fix_versioned_model, fix_versioned_model_with_join):
    yield

    for model in [fix_versioned_model] + list(fix_versioned_model_with_join):
        instances = model.all()

        for instance in instances:
            print("deleting: %r" % instance)
            instance.delete(force=True)

        model.refresh()
        assert len(model.all()) == 0

def test_get_version_class(fix_versioned_model):
    cls = fix_versioned_model

    version_class = cls.get_version_class()
    assert version_class.__name__ == 'VersionedOne_version'

    assert cls.get_es_mapping() == {
            'value': {'type': 'keyword'},
            'created_at': {'type': 'date'},
            'deleted': {'type': 'boolean'},
            'updated_at': {'type': 'date'},
            }

    assert version_class.get_es_mapping() == {
            '_document_id': {'type': 'keyword'},
            '_status': {'type': 'keyword'},
            '_document_version': {'type': 'keyword'},
            'value': {'type': 'keyword'},
            'created_at': {'type': 'date'},
            'deleted': {'type': 'boolean'},
            'updated_at': {'type': 'date'},
            }

    assert version_class.get_index() == "test_versioned_one_version"
    assert cls.get_index() == "test_versioned_one"

def test_to_proposal(fix_versioned_model):
    # TODO: test also with a class that doesn't have slots
    cls = fix_versioned_model

    instance1 = cls.create(value='value1')
    cls.refresh()
    assert instance1._version == 1

    proposal = instance1.to_version_proposal()
    assert proposal.__class__.__name__ == 'VersionedOne_version'
    assert proposal.value == 'value1'
    assert proposal._document_version == 1
    assert proposal._status == 'proposal'
    assert proposal._document_id == instance1.id


def test_versioned_update(fix_versioned_model):
    cls = fix_versioned_model

    instance1 = cls.create(value='value1')
    cls.refresh()
    assert instance1._version == 1

    instance1.value = 'value2'
    instance1.save()
    cls.refresh()
    assert instance1._version == 2

    loaded1 = cls.get(instance1.id)
    assert loaded1.id == instance1.id
    assert loaded1.value == 'value2'
    assert loaded1._version == 2

    versions = cls.get_version_class().all()
    assert len(versions) == 1

    loaded2 = cls.get_document_version(instance1.id, 1)

    assert loaded2._document_id == instance1.id
    assert loaded2.value == 'value1'
    assert loaded2._document_version == 1


def test_versioned_timestamps(fix_versioned_model, freezer):
    cls = fix_versioned_model

    start = datetime.now()

    instance1 = cls.create(value='value1')
    assert instance1._version == 1
    assert instance1.created_at == start
    assert instance1.updated_at == start

    freezer.tick(delta=timedelta(seconds=10))
    now = datetime.now()

    instance1.value = 'value2'
    instance1.save()
    cls.refresh()

    assert instance1._version == 2
    assert instance1.created_at == start
    assert instance1.updated_at == now

    freezer.tick(delta=timedelta(seconds=10))
    now2 = datetime.now()

    instance1.value = 'value3'
    instance1.save()
    cls.refresh()

    assert instance1._version == 3
    assert instance1.created_at == start
    assert instance1.updated_at == now2

    version1 = cls.get_document_version(id=instance1.id, version=1)
    from pprint import pprint
    print("\n\nversion1")
    pprint(version1)
    assert version1.value == 'value1'
    assert version1.created_at == start
    assert version1.updated_at == now

def test_versioned_load_version(fix_versioned_model):
    cls = fix_versioned_model

    instance1 = cls.create(value='value1')
    assert instance1._version == 1

    instance1.value = 'value2'
    instance1.save()
    cls.refresh()

    assert instance1._version == 2

    version1 = cls.get_document_version(id=instance1.id, version=1)
    from pprint import pprint
    print("\n\nversion1")
    pprint(version1)
    print(version1.get_id())
    print(" --------- ")
    print(type(version1), type(instance1))
    assert version1.value == 'value1'
    assert version1.get_id() == instance1.get_id()


def test_versioned_with_join_update(fix_versioned_model_with_join):
    from pprint import pprint
    pa, ch = fix_versioned_model_with_join

    parent1 = pa.create(value='value1')
    child1 = ch.create(value='ch1', parent=parent1)
    parent1.child=[child1]
    ch.refresh()

    assert child1.parent.id == parent1.id
    assert parent1._version == 1

    parent1.value = 'value2'
    parent1.save()
    pa.refresh()
    assert parent1._version == 2
    assert parent1.child[0].id == child1.id

    loaded1 = pa.get(parent1.id)
    assert loaded1.id == parent1.id
    assert loaded1.value == 'value2'
    assert loaded1._version == 2
    assert len(loaded1.child) == 0
    loaded1._lazy_load()
    from pprint import pprint
    print("loaded1")
    pprint(loaded1)
    assert loaded1.child[0].id == child1.id

    versions = pa.get_version_class().all()
    assert len(versions) == 1

    loaded2 = pa.get_document_version(parent1.id, 1)


    assert loaded2._document_id == parent1.id
    assert loaded2.value == 'value1'
    assert loaded2._document_version == 1
    loaded2._lazy_load()
    print("loaded2")
    pprint(loaded2)
    assert loaded2.child[0].id == child1.id
