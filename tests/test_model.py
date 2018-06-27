import pytest
from elastic_connect import Model
import elastic_connect
from elastic_connect.data_types import Keyword
import elasticsearch.exceptions


@pytest.fixture(scope="module")
def fix_model_one_save():

    class OneSave(Model):
        __slots__ = ('value', )

        _meta = {
            '_doc_type': 'model_save_one'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value')
        }

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[OneSave])
    assert es.indices.exists(index=OneSave.get_index())

    yield OneSave

    if pytest.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=OneSave.get_index())


@pytest.fixture(scope="module")
def fix_model_one_id_save():

    class OneIdSave(Model):
        __slots__ = ('value', )

        _meta = {
            '_doc_type': 'model_save_one_id'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value')
        }

        def _compute_id(self):
            return self.value + '_id'

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[OneIdSave])
    assert es.indices.exists(index=OneIdSave.get_index())

    yield OneIdSave

    if pytest.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=OneIdSave.get_index())


@pytest.fixture(scope="module")
def fix_model_two_save():

    class TwoSave(Model):
        __slots__ = ('value', 'subvalue')

        _meta = {
            '_doc_type': 'model_save_two'
        }
        _mapping = {
            'id': Keyword(name='id'),
            'value': Keyword(name='value'),
            'subvalue': Keyword(name='subvalue')
        }

    es = elastic_connect.get_es()
    indices = elastic_connect.create_mappings(model_classes=[TwoSave])
    assert es.indices.exists(index=TwoSave.get_index())

    yield TwoSave

    if pytest.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    elastic_connect.delete_indices(indices=indices)
    assert not es.indices.exists(index=TwoSave.get_index())


def test_create(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(value='value1')  # type: Model

    assert instance.id is not None

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), doc_type=cls._meta['_doc_type'], id=instance.id)

    assert es_result['found'] == True
    assert es_result['_source'] == {'value': 'value1'}


def test_get(fix_model_one_save):
    cls = fix_model_one_save

    es = elastic_connect.get_es()
    es_result = es.index(index=cls.get_index(), doc_type=cls._meta['_doc_type'], body={'value': 'pokus'}, refresh=True)

    assert es_result['created'] == True

    print(es_result)

    instance = cls.get(es_result['_id'])

    assert instance.id == es_result['_id']
    assert instance.value == 'pokus'


def test_save(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(value='value1')
    cls.refresh()

    instance2 = cls.get(instance.id)
    assert instance2.id == instance.id
    assert instance2.value == 'value1'

    instance.value = 'value2'
    instance.save()

    instance3 = cls.get(instance.id)
    assert instance3.id == instance.id
    assert instance3.value == 'value2'


def test_create_with_id(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(id='100', value='value1')  # type: OneSave

    assert instance.id == '100'

    cls.refresh()

    es = elastic_connect.get_es()
    es_result = es.get(index=cls.get_index(), doc_type=cls._meta['_doc_type'], id=instance.id)

    assert es_result['found'] is True
    assert es_result['_source'] == {'value': 'value1'}


def test_lazy_save(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls(value='value15')  # type: OneSave
    instance.save()

    loaded = cls.get(instance.id)
    assert loaded.value == instance.value


def test_lazy_save_generated_id(fix_model_one_id_save):
    cls = fix_model_one_id_save

    instance = cls(value='value25')  # type: OneIdSave
    instance.save()

    loaded = cls.get(instance.id)
    assert loaded.value == instance.value
    assert loaded.id == 'value25_id'


def test_lazy_save_with_id(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls(value='value1')  # type: OneSave

    instance.id = '60'
    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        instance.save()


def test_change_id(fix_model_one_save):
    cls = fix_model_one_save

    instance = cls.create(id='52', value='value1')  # type: OneSave

    assert instance.id == '52'
    cls.refresh()

    instance.id = '50'
    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        instance.save()


def test_change_generated_id(fix_model_one_id_save):
    cls = fix_model_one_id_save

    instance = cls.create(value='value1')  # type: OneIdSave
    assert instance.id == 'value1_id'

    instance.id = '50'
    with pytest.raises(elastic_connect.base_model.IntegrityError):
        instance.save()

    instance = cls.get('value1_id')
    instance.value = 'value3'
    with pytest.raises(elastic_connect.base_model.IntegrityError):
        instance.save()


def test_find_by_simple(fix_model_one_save):
    cls = fix_model_one_save

    instance1 = cls.create(value='value1')  # type: OneSave
    instance2 = cls.create(value='value2')  # type: OneSave

    cls.refresh()

    found1 = cls.find_by(value='value1')
    assert len(found1) == 1
    assert found1[0].id == instance1.id

    found2 = cls.find_by(value='value2')
    assert len(found2) == 1
    assert found2[0].id == instance2.id


def test_find_by_simple_limit_size(fix_model_one_save):
    cls = fix_model_one_save

    instance1 = cls.create(value='value3')  # type: OneSave
    instance2 = cls.create(value='value3')  # type: OneSave
    instance3 = cls.create(value='value3')  # type: OneSave

    cls.refresh()

    found1 = cls.find_by(value='value3')
    assert len(found1) == 3

    found2 = cls.find_by(value='value3', size=1)
    assert len(found2) == 1
    assert found2[0].id in [instance1.id, instance2.id, instance3.id]


def test_find_by_simple_sort(fix_model_one_save):
    cls = fix_model_one_save

    instance1 = cls.create(value='value4')  # type: OneSave
    instance2 = cls.create(value='value4')  # type: OneSave
    instance3 = cls.create(value='value4')  # type: OneSave

    cls.refresh()

    found1 = cls.find_by(value='value4')
    assert len(found1) == 3

    found2 = cls.find_by(value='value4', sort=[{"_uid": "asc"}])
    assert len(found2) == 3
    assert found2[0].id < found2[1].id < found2[2].id

    found2 = cls.find_by(value='value4', sort=[{"_uid": "desc"}])
    assert len(found2) == 3
    assert found2[2].id < found2[1].id < found2[0].id


def test_find_by_multi(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value5', subvalue='val1')  # type: TwoSave
    instance2 = cls.create(value='value5', subvalue='val1')  # type: TwoSave
    instance3 = cls.create(value='value5', subvalue='val2')  # type: TwoSave
    instance4 = cls.create(value='value6', subvalue='val1')  # type: TwoSave
    instance5 = cls.create(value='value6', subvalue='val2')  # type: TwoSave

    cls.refresh()

    found = cls.find_by(value='value5')
    assert len(found) == 3

    found = cls.find_by(value='value6')
    assert len(found) == 2

    found = cls.find_by(subvalue='val1')
    assert len(found) == 3

    found = cls.find_by(subvalue='val2')
    assert len(found) == 2

    found = cls.find_by(value='value6', subvalue='val1')
    assert len(found) == 1

    found = cls.find_by(value='value6', subvalue='val3')
    assert len(found) == 0

    found = cls.find_by(value='value5', subvalue='val1')
    assert len(found) == 2


def test_find_by_multi_sort(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value7', subvalue='v1')  # type: TwoSave
    instance2 = cls.create(value='value7', subvalue='v2')  # type: TwoSave
    instance3 = cls.create(value='value7', subvalue='v3')  # type: TwoSave
    instance4 = cls.create(value='value8', subvalue='v1')  # type: TwoSave
    instance5 = cls.create(value='value8', subvalue='v2')  # type: TwoSave

    cls.refresh()

    found = cls.find_by(value='value7', sort=[{'subvalue': 'asc'}])
    assert found[0].value == 'value7'
    assert found[1].value == 'value7'
    assert found[2].value == 'value7'
    assert len(found) == 3
    assert found[0].subvalue < found[1].subvalue < found[2].subvalue


def test_find_by_search_after_default_sort(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value9', subvalue='va1')  # type: TwoSave
    instance2 = cls.create(value='value9', subvalue='va2')  # type: TwoSave
    instance3 = cls.create(value='value9', subvalue='va3')  # type: TwoSave

    cls.refresh()

    found1 = cls.find_by(value='value9', size=1)
    assert len(found1) == 1
    assert found1[0].subvalue == 'va1'
    assert found1[0].id == instance1.id
    assert ('model_save_two#'+found1[0].id) == found1.search_after_values[0]

    found2 = cls.find_by(value='value9', size=1, search_after=found1.search_after_values)
    assert len(found2) == 1
    assert found2[0].subvalue == 'va2'
    assert found2[0].id == instance2.id
    assert ('model_save_two#' + found2[0].id) == found2.search_after_values[0]

    found3 = cls.find_by(value='value9', size=1, search_after=found2.search_after_values)
    assert len(found3) == 1
    assert found3[0].subvalue == 'va3'
    assert found3[0].id == instance3.id
    assert ('model_save_two#' + found3[0].id) == found3.search_after_values[0]

    found4 = cls.find_by(value='value9', size=1, search_after=found3.search_after_values)
    assert len(found4) == 0


def test_find_by_search_after_custom_sort(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value10', subvalue='vl1')  # type: TwoSave
    cls.refresh()
    instance2 = cls.create(value='value10', subvalue='vl2')  # type: TwoSave
    cls.refresh()
    instance3 = cls.create(value='value10', subvalue='vl3')  # type: TwoSave
    cls.refresh()

    found1 = cls.find_by(value='value10', size=1, sort=[{'_uid': 'desc'}])
    assert len(found1) == 1
    assert found1[0].subvalue == 'vl3'
    assert found1[0].id == instance3.id
    assert 'model_save_two#'+found1[0].id == found1.search_after_values[0]

    found2 = cls.find_by(value='value10', size=1, sort=[{'_uid': 'desc'}], search_after=found1.search_after_values)
    assert len(found2) == 1
    assert found2[0].subvalue == 'vl2'
    assert found2[0].id == instance2.id
    assert 'model_save_two#' + found2[0].id == found2.search_after_values[0]

    found3 = cls.find_by(value='value10', size=1, sort=[{'_uid': 'desc'}], search_after=found2.search_after_values)
    assert len(found3) == 1
    assert found3[0].subvalue == 'vl1'
    assert found3[0].id == instance1.id
    assert 'model_save_two#' + found3[0].id == found3.search_after_values[0]

    found4 = cls.find_by(value='value10', size=1, sort=[{'_uid': 'desc'}], search_after=found3.search_after_values)
    assert len(found4) == 0


def test_find_by_search_after_default_sort_using_result(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value11', subvalue='sv1')  # type: TwoSave
    cls.refresh()
    instance2 = cls.create(value='value11', subvalue='sv2')  # type: TwoSave
    cls.refresh()
    instance3 = cls.create(value='value11', subvalue='sv3')  # type: TwoSave
    cls.refresh()

    found1 = cls.find_by(value='value11', size=1)
    assert len(found1) == 1
    assert found1[0].subvalue == 'sv1'
    assert found1[0].id == instance1.id
    assert ('model_save_two#'+found1[0].id) == found1.search_after_values[0]

    found2 = found1.search_after()
    assert len(found2) == 1
    assert found2[0].subvalue == 'sv2'
    assert found2[0].id == instance2.id
    assert ('model_save_two#' + found2[0].id) == found2.search_after_values[0]

    found3 = found2.search_after()
    assert len(found3) == 1
    assert found3[0].subvalue == 'sv3'
    assert found3[0].id == instance3.id
    assert ('model_save_two#' + found3[0].id) == found3.search_after_values[0]

    found4 = found3.search_after()
    assert len(found4) == 0


def test_find_by_search_after_custom_value(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value20', subvalue='1')  # type: TwoSave
    cls.refresh()
    instance2 = cls.create(value='value20', subvalue='2')  # type: TwoSave
    cls.refresh()
    instance3 = cls.create(value='value20', subvalue='3')  # type: TwoSave
    cls.refresh()
    instance4 = cls.create(value='value20', subvalue='3')  # type: TwoSave
    cls.refresh()

    found1 = cls.find_by(value='value20', sort=[{'subvalue': 'desc'}])
    assert len(found1) == 4
    assert found1[0].subvalue == '3'
    assert found1[0].id == instance3.id
    assert found1[1].subvalue == '3'
    assert found1[1].id == instance4.id
    assert found1[2].subvalue == '2'
    assert found1[3].subvalue == '1'

    found2 = cls.find_by(value='value20', sort=[{'subvalue': 'desc'}], search_after=['3', ''])
    assert len(found2) == 4

    found2 = cls.find_by(value='value20', sort=[{'subvalue': 'desc'}], search_after=['3', 'model_save_two#'+instance3.id])
    assert len(found2) == 3
    found2 = cls.find_by(value='value20', sort=[{'subvalue': 'desc'}], search_after=['3', 'model_save_two#' + instance4.id])
    assert len(found2) == 2

    found3 = cls.find_by(value='value20', sort=[{'subvalue': 'desc'}], search_after=['2', ''])
    assert len(found2) == 2

def test_find_by_query(fix_model_two_save):
    cls = fix_model_two_save

    instance1 = cls.create(value='value30', subvalue='www.zive.cz')  # type: TwoSave
    cls.refresh()
    instance2 = cls.create(value='value30', subvalue='zpravy.zive.cz')  # type: TwoSave
    cls.refresh()
    instance3 = cls.create(value='value40', subvalue='www.zive.cz')  # type: TwoSave
    cls.refresh()
    instance4 = cls.create(value='value50', subvalue='zpravy.aktualne.cz')  # type: TwoSave
    cls.refresh()

    found1 = cls.find_by(query="subvalue: *.zive.cz")
    assert len(found1) == 3

    found1 = cls.find_by(query="subvalue: *.zive.cz AND value: *40")
    assert len(found1) == 1

    found1 = cls.find_by(query="subvalue: *.zive.cz AND value: *50")
    assert len(found1) == 0
