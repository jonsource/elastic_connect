import os
import sys
import pytest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ROOT_TEST_DIR = os.path.abspath(os.path.join(ROOT_DIR, "tests"))

sys.path.insert(0, ROOT_DIR)
print(sys.path)

import elastic_connect

def pytest_addoption(parser):
    parser.addoption("--index-noclean", action="store_true",
                     help="Don't clear the index after each test. Debugging only, may break some tests.")
    parser.addoption("--es-host", action="store", default="localhost", help="Elasticsearch hostname")
    parser.addoption("--es-port", action="store", default="9200", help="Elasticsearch port")
    parser.addoption("--es-prefix", action="store", default="test", help="Elasticsearch indexes prefix")

@pytest.fixture(scope="session", autouse=True)
def prefix_indices(request):
    # set the prefix for all indices, just to be safe
    print("Prefixing all indices with: '" + pytest.config.getoption("--es-prefix") + '_' + "'")
    #elastic_connect._namespaces['_default'].index_prefix = pytest.config.getoption("--es-prefix") + '_'
    elastic_connect.namespace._global_prefix = pytest.config.getoption("--es-prefix") + '_'

@pytest.fixture(scope="module")
def fix_es():
    elastic_connect.connect(
        [{"host": pytest.config.getoption("--es-host"), "port": pytest.config.getoption("--es-port")}])
    yield


@pytest.fixture(scope="module")
def fix_index(model_classes):
    indices = elastic_connect.create_mappings(model_classes)

    print("** created indices:", indices)

    yield

    if pytest.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    for index in indices:
        elastic_connect.delete_index(index)

    print("\npost\n", elastic_connect.get_es().cat.indices() or "No indices")
