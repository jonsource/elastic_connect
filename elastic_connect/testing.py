import pytest
import elastic_connect.namespace
import logging

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption("--index-noclean", action="store_true",
                     help="Don't clear the index after each test. Debugging only, may break some tests.")
    parser.addoption("--es-host", action="store", default="localhost", help="Elasticsearch hostname")
    parser.addoption("--es-port", action="store", default="9200", help="Elasticsearch port")
    parser.addoption("--es-prefix", action="store", default="test", help="Elasticsearch indexes prefix")
    parser.addoption('--namespace', action='store_true', default=False, help='Also run tests for two namespaces')


def pytest_runtest_setup(item):
    """
    Skip tests if they are marked as namespace and --namespace is not given
    """
    if getattr(item.obj, 'namespace', None) and not pytest.config.getvalue('--namespace'):
        pytest.skip('Not running namespace tests')


@pytest.fixture(scope="session", autouse=True)
def prefix_indices(request):
    """
    @pytest.fixture(scope="session", autouse=True)

    Set the prefix for all indices according to ``--es-prefix`` option, just to be safe.
    So with the default option a model which uses index ``admin_users`` in production will use index
    ``testadmin_users`` in the tests.
    """
    logger.warning("Prefixing all indices with: '%s'", (pytest.config.getoption("--es-prefix") + '_'))
    elastic_connect.namespace._global_prefix = pytest.config.getoption("--es-prefix") + '_'

@pytest.fixture(scope="session", autouse=True)
def fix_es():
    """
    @pytest.fixture(scope="session", autouse=True)

    Fixes the default connection to elasticsearch according to ``--es-host`` and ``--es-port`` options.
    Waits for all namespaces to be ready.
    :yield: None
    """
    conf = {'host': pytest.config.getoption("--es-host"),
            'port': pytest.config.getoption("--es-port"),
            }
    elastic_connect._namespaces['_default'].es_conf = [conf]
    for namespace in elastic_connect._namespaces.values():
        namespace.wait_for_ready()
        logger.info(namespace.name + " ready!")

    yield

@pytest.fixture(scope="module")
def fix_index(model_classes):
    """
    @pytest.fixture(scope="module")

    Creates indices for supplied ``model_classes``.
    Deletes these indices after the tests according to the ``--index-noclean`` option.

    :param model_classes: list of model classes
    :yield: None
    """

    indices = elastic_connect.create_mappings(model_classes)

    logger.info("created indices: %s", indices)

    yield

    if pytest.config.getoption("--index-noclean"):
        logger.warning("not cleaning indices")
        return

    for index in indices:
        elastic_connect.delete_index(index)

    logger.info("teardown %s", (elastic_connect.get_es().cat.indices() or "No indices",))


@pytest.fixture(scope="module")
def second_namespace():
    """
    @pytest.fixture(scope="module")

    Returns the Namespace object of a second elasticsearch cluster. This cluster must be available on localhost:18400.
    You may use this fixture as a base for your own connections to multiple elasticsearch clusters.
    :return: Namespace
    """

    if 'second' in elastic_connect._namespaces:
        return elastic_connect._namespaces['second']

    second = elastic_connect.Namespace(name='second', es_conf=[{'host': 'localhost', 'port': 18400}])
    elastic_connect.namespace.register_namespace(second)

    return second
