import pytest
from elastic_connect.base_model import Model
from elastic_connect.advanced_model import VersionedModel
from elastic_connect import advanced_model
import elastic_connect
from elastic_connect.data_types import Keyword, Date
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


@pytest.fixture(autouse=True)
def delete_all_instances(fix_versioned_model):
    yield

    instances = fix_versioned_model.all()

    for instance in instances:
        instance.delete()

    fix_versioned_model.refresh()

    assert len(fix_versioned_model.all()) == 0


def test_version_awareness(fix_versioned_model):
    cls = fix_versioned_model

    assert cls.get_es_mapping() == {'value': {'type': 'keyword'}}

    instance1 = cls.create(value='value1')
    cls.refresh()
    assert instance1._version == 1

    instance2 = cls.get(instance1.id)
    assert instance2._version == 1

    instance1.value='value2'
    instance1.save()
    assert instance1._version == 2

    instance2.value = 'value3'
    with pytest.raises(elasticsearch.exceptions.ConflictError):
        with cls.version_checking(True):
            instance2.save()

    assert instance2._version == 1

    with cls.version_checking(False):
        instance2.save()

    assert instance2._version == 3

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
            '_version_id': {'type': 'keyword'},
            'value': {'type': 'keyword'},
            'created_at': {'type': 'date'},
            'deleted': {'type': 'boolean'},
            'updated_at': {'type': 'date'},
            }

    assert version_class.get_index() == "test_versioned_one_version"
    assert cls.get_index() == "test_versioned_one"

def test_to_proposal(fix_versioned_model):
    cls = fix_versioned_model

    instance1 = cls.create(value='value1')
    cls.refresh()
    assert instance1._version == 1

    proposal = instance1.to_version_proposal()
    assert proposal.__class__.__name__ == 'VersionedOne_version'
    assert proposal._version_id == 1
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

    loaded2 = cls.get_version(instance.id, 1)
    assert loaded2.id == instance2.id
    assert loaded2.value == 'value1'
    assert loaded2._version == 1


