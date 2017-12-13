import os
import sys
import pytest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ROOT_TEST_DIR = os.path.abspath(os.path.join(ROOT_DIR, "tests"))

sys.path.insert(0, ROOT_DIR)
print(sys.path)


def pytest_addoption(parser):
    parser.addoption("--index-noclean", action="store_true",
                     help="Don't clear the index after each test. Debugging only, may break some tests.")
    parser.addoption("--es-host", action="store", default="localhost", help="Elasticsearch hostname")
    parser.addoption("--es-port", action="store", default="9200", help="Elasticsearch port")


@pytest.fixture(scope="module")
def fix_es():
    import elastic_connect

    elastic_connect.connect(
        [{"host": pytest.config.getoption("--es-host"), "port": pytest.config.getoption("--es-port")}])
    yield


@pytest.fixture(scope="module")
def fix_index(model_classes):
    import elastic_connect

    indices = elastic_connect.create_mappings(model_classes)

    print("** created indices:", indices)

    yield

    if pytest.config.getoption("--index-noclean"):
        print("** not cleaning")
        return

    for index in indices:
        elastic_connect.delete_index(index)

    print("\npost\n", elastic_connect.get_es().cat.indices() or "No indices")
