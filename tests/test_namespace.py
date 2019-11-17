import pytest
import elastic_connect
from elastic_connect import Model
from elastic_connect.data_types import Keyword


@pytest.fixture()
def override_default_namespace():
    default = elastic_connect._namespaces['_default']
    old_prefix = default._index_prefix
    default._index_prefix = 'namespace_test_'

    print("override default", default, default.__dict__)

    yield

    default._index_prefix = old_prefix


def test_default_namespace_prefix(request):

    class NamespacedModel(Model):
        __slots__ = ('id', 'value')
        
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value'),
        }
    
        _meta = {
            '_doc_type': 'namespaced_model',
        }
        pass

    assert NamespacedModel.get_index() == 'test_namespaced_model'

    namespace = NamespacedModel._es_namespace
    indices = namespace.create_mappings(model_classes=[NamespacedModel])
        
    instance = NamespacedModel.create(value='one')
    assert instance.id
    
    es = elastic_connect.get_es()
    es_result = es.indices.get(index=NamespacedModel.get_index())
    print(es_result)
    expected_mapping = {'test_namespaced_model': 
                            {'properties': 
                                {'value': 
                                    {'type': 'keyword'}
                                }
                            }
                        }
    assert 'test_namespaced_model' in es_result
    assert es_result['test_namespaced_model']['mappings'] == expected_mapping

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
    else:
        index_name = NamespacedModel.get_index()
        namespace.delete_indices(indices=indices)
        assert not namespace.get_es().indices.exists(index=index_name)



@pytest.mark.namespace
def test_default_namespace_prefix_override(override_default_namespace):

    class DnpModel(Model):
        pass

    assert DnpModel.get_index() == 'test_namespace_test_model'


@pytest.mark.namespace
def test_two_namespaces_prefix(second_namespace):

    class TnpModel(Model):
        pass

    second_TnpModel = second_namespace.register_model_class(TnpModel)

    assert second_TnpModel.get_index() == 'test_second_model'


@pytest.fixture(scope="module")
def fix_model_one_save(request):

    default_namespace = elastic_connect._namespaces['_default']

    class OneSave(Model):
        _meta = {
            '_doc_type': 'model_save'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value')
        }

    namespace = OneSave._es_namespace
    indices = namespace.create_mappings(model_classes=[OneSave])
    assert OneSave.get_index() == 'test_model_save'
    assert default_namespace.get_es().indices.exists(index=OneSave.get_index())

    yield OneSave

    if request.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    namespace.delete_indices(indices=indices)
    assert not default_namespace.get_es().indices.exists(index=OneSave.get_index())


@pytest.fixture(scope="module")
def fix_model_two_save(fix_model_one_save, second_namespace):

    OneSave = fix_model_one_save
    TwoSave = second_namespace.register_model_class(OneSave)

    namespace = TwoSave._es_namespace
    indices = namespace.create_mappings(model_classes=[TwoSave])
    assert TwoSave.get_index() == 'test_second_model_save'
    assert second_namespace.get_es().indices.exists(index=TwoSave.get_index())

    yield TwoSave

    namespace.delete_indices(indices=indices)
    assert not second_namespace.get_es().indices.exists(index=TwoSave.get_index())


@pytest.mark.namespace
def test_namespace_save(fix_model_one_save, fix_model_two_save):
    cls1 = fix_model_one_save
    cls2 = fix_model_two_save

    instance1 = cls1.create(id=1, value='value1')
    instance2 = cls2.create(id=1, value='value2')
    cls1.refresh()
    cls2.refresh()

    loaded1 = cls1.get(instance1.id)
    loaded2 = cls2.get(instance2.id)
    assert loaded1.id == '1'
    assert loaded1.value == 'value1'
    assert loaded2.id == '1'
    assert loaded2.value == 'value2'
    print("instance1", instance1)
    print("instance2", instance2)
