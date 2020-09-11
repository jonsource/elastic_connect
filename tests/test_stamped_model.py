import pytest
from elastic_connect.base_model import Model
from elastic_connect.advanced_model import StampedModel
from elastic_connect import advanced_model
import elastic_connect
from elastic_connect.data_types import Keyword, Date
import elasticsearch.exceptions
from datetime import datetime, timedelta
from elasticsearch.exceptions import NotFoundError


@pytest.fixture(scope="module")
def fix_stamped_model(request):

    class StampedOne(StampedModel):
        __slots__ = ('value', )

        _meta = {
            '_doc_type': 'stamped_one'
        }
        _mapping = StampedModel.model_mapping(value=Keyword())

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[StampedOne])
    assert es.indices.exists(index=StampedOne.get_index())

    yield StampedOne

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=StampedOne.get_index())


@pytest.fixture(autouse=True)
def delete_all_instances(fix_stamped_model):
    yield

    with fix_stamped_model.thrashed():
        instances = fix_stamped_model.all()

    for instance in instances:
        instance.delete(force=True)

    fix_stamped_model.refresh()

    with fix_stamped_model.thrashed():
        assert len(fix_stamped_model.all()) == 0


def test_mapping(request, fix_stamped_model):
    StampedOne = fix_stamped_model()
    expected_mapping = {'created_at': {'type': 'date'},
                        'updated_at': {'type': 'date'},
                        'deleted': {'type': 'boolean'},
                        'value': {'type': 'keyword'}}

    mapping = StampedOne.get_es_mapping()
    assert mapping == expected_mapping


def test_create(freezer, fix_stamped_model):
    cls = fix_stamped_model

    now = datetime.now()

    instance = cls.create(value='value1')  # type: StampedModel

    assert instance.id is not None
    assert instance.created_at == now
    assert instance.updated_at == now

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), doc_type=cls.get_doctype(), id=instance.id)

    assert es_result['found'] == True
    assert es_result['_source'] == {
        'value': 'value1',
        'created_at': now.isoformat(),
        'updated_at': now.isoformat(),
        'deleted': False
    }

def test_create_by_save(freezer, fix_stamped_model):
    cls = fix_stamped_model

    now = datetime.now()

    in_memory = cls(value='value1')  # type: StampedModel
    instance = in_memory.save()

    assert instance.id is not None
    assert instance.created_at == now
    assert instance.updated_at == now

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), doc_type=cls.get_doctype(), id=instance.id)

    assert es_result['found'] == True
    assert es_result['_source'] == {
        'value': 'value1',
        'created_at': now.isoformat(),
        'updated_at': now.isoformat(),
        'deleted': False
    }

def test_update(freezer, fix_stamped_model):
    cls = fix_stamped_model

    created = datetime.now()

    instance = cls.create(value='value1')  # type: StampedModel
    cls.refresh()

    loaded = cls.get(instance.id)
    assert loaded.created_at == created
    assert loaded.updated_at == created

    freezer.tick(delta=timedelta(seconds=10))

    updated = created + timedelta(seconds=10)
    
    loaded.value = "dva"
    loaded.save()
    cls.refresh()

    loaded = cls.get(instance.id)
    assert loaded.created_at == created
    assert loaded.updated_at == updated


def test_get(freezer, fix_stamped_model):
    cls = fix_stamped_model

    now = datetime.now()

    instance = cls.create(value='value1')  # type: StampedModel
    cls.refresh()

    #loaded = cls.get(instance.id)
    loaded = cls.get(instance.id)
    
    assert loaded.created_at == now
    assert loaded.updated_at == now


def test_get_nonexistant(freezer, fix_stamped_model):
    cls = fix_stamped_model

    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        instance = cls.get("badbadId")


def test_all(freezer, fix_stamped_model):
    cls = fix_stamped_model

    instance1 = cls.create(value='value1')  # type: StampedModel
    instance2 = cls.create(value='value1')  # type: StampedModel
    instance3 = cls.create(value='value1')  # type: StampedModel
    cls.refresh()

    assert len(cls.all()) == 3


def test_delete_single(freezer, fix_stamped_model):
    cls = fix_stamped_model

    instance = cls.create(value='value1')  # type: StampedModel
    cls.refresh()

    assert cls.get(instance.id).id == instance.id

    instance.delete()
    cls.refresh()

    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        cls.get(instance.id)

    assert len(cls.all()) == 0


def test_delete(freezer, fix_stamped_model):
    cls = fix_stamped_model

    instance1 = cls.create(value='value1')  # type: StampedModel
    instance2 = cls.create(value='value1')  # type: StampedModel
    instance3 = cls.create(value='value1')  # type: StampedModel
    cls.refresh()

    assert len(cls.all()) == 3

    instance3.delete()
    cls.refresh()

    assert len(cls.all()) == 2
    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        cls.get(instance3.id)


def test_restore(freezer, fix_stamped_model):
    cls = fix_stamped_model

    instance = cls.create(value='value1')  # type: StampedModel
    instance.delete()
    cls.refresh()

    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        cls.get(instance.id)

    cls.restore(instance.id)
    cls.refresh()
    
    assert cls.get(instance.id).id == instance.id

    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        cls.restore('badbadId')


def test_contextmanager_thrashed(freezer, fix_stamped_model):
    cls = fix_stamped_model

    assert cls.thrash_handling == advanced_model.NONE

    inst = cls(value='value_some')
    assert inst.thrash_handling == advanced_model.NONE

    with cls.thrashed():
        assert cls.thrash_handling == advanced_model.WITH
        assert inst.thrash_handling == advanced_model.WITH

    assert cls.thrash_handling == advanced_model.NONE    
    assert inst.thrash_handling == advanced_model.NONE


def test_thrashed(freezer, fix_stamped_model):
    cls = fix_stamped_model

    instance1 = cls.create(value='value1')  # type: StampedModel
    instance2 = cls.create(value='value1')  # type: StampedModel
    instance3 = cls.create(value='value1')  # type: StampedModel
    instance3.delete()
    cls.refresh()

    assert len(cls.all()) == 2
    
    with cls.thrashed():
        assert len(cls.all()) == 3

    with cls.thrashed_only():
        assert len(cls.all()) == 1
