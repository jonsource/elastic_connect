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
            '_doc_type': 'versioned_one'
        }
        _mapping = VersionedModel.model_mapping(value=Keyword())

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[VersionedOne])
    assert es.indices.exists(index=VersionedOne.get_index())

    yield VersionedOne

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=VersionedOne.get_index())


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

    print("\n\npregetpreget\n\n")
    instance2 = cls.get(instance1.id)
    assert instance2._version == 1

    print("\n\npre save\n\n")

    instance1.value='value2'
    instance1.save()
    assert instance1._version == 2

    with pytest.raises(elasticsearch.exceptions.ConflictError):
        instance2.value='value3'
        instance2.save()
