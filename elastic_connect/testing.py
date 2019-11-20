import pytest
import elastic_connect.namespace
import logging
import elasticsearch.exceptions

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption("--index-noclean", action="store_true",
                     help=("Don't clear the index after each test.Debugging "
                           "only, may break some tests."))
    parser.addoption("--es-host", action="store", default="localhost",
                     help="Elasticsearch hostname")
    parser.addoption("--es-port", action="store", default="9200",
                     help="Elasticsearch port")
    parser.addoption("--es-prefix", action="store", default="test",
                     help="Elasticsearch indexes prefix")
    parser.addoption('--namespace', action='store_true', default=False,
                     help='Also run tests for two namespaces')


def pytest_runtest_setup(item):
    """
    Skip tests based on command line options
    """

    # Skip tests if they are marked as namespace and --namespace is not
    # given
    has_marker = [x for x in item.iter_markers(name='namespace')]
    if has_marker and not item.config.getvalue('--namespace'):
        pytest.skip('Not running namespace tests')

    # Skip tests if they are marked as skip_on_index_noclean and
    # --index-noclean is given
    has_marker = [x for x in item.iter_markers(name='skip_on_index_noclean')]
    if has_marker and item.config.getvalue('--index-noclean'):
        pytest.skip('Not cleaning indices')


def pytest_configure(config):
    """
    Register additional marker for namespaces testing
    """
    config.addinivalue_line("markers",
                            "namespace: Tests for two namespaces")
    config.addinivalue_line(
        "markers",
        "skip_on_index_noclean: Skip on not cleaning indices")


@pytest.fixture(scope="session", autouse=True)
def prefix_indices(request):
    """
    @pytest.fixture(scope="session", autouse=True)

    Set the prefix for all indices according to ``--es-prefix`` option,
    just to be safe. So with the default option a model which uses index
    ``admin_users`` in production will use index ``testadmin_users`` in
    the tests.
    """
    prefix = request.config.getoption("--es-prefix") + '_'
    logger.warning("Prefixing all indices with: '%s'", prefix)
    elastic_connect.namespace._global_prefix = prefix


@pytest.fixture(scope="session", autouse=True)
def fix_es(request):
    """
    @pytest.fixture(scope="session", autouse=True)

    Fixes the default connection to elasticsearch according to
    ``--es-host`` and ``--es-port`` options. Waits for all namespaces
    to be ready.
    :yield: None
    """
    conf = {'host': request.config.getoption("--es-host"),
            'port': request.config.getoption("--es-port"),
            }
    elastic_connect._namespaces['_default'].es_conf = [conf]
    for namespace in elastic_connect._namespaces.values():
        namespace.wait_for_ready()
        logger.info(namespace.name + " ready!")

    es = elastic_connect.get_es()
    template = {
                "template": "*",
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                }
    es.indices.put_template(name="test_all", body=template, order=1)
    logger.info("templates %s", es.indices.get_template(name='*'))

    yield

    es.indices.delete_template(name="test_all")
    with pytest.raises(elasticsearch.exceptions.NotFoundError):
        logger.info("templates %s", es.indices.get_template(name='test_all'))


@pytest.fixture(scope="module")
def fix_index(request, model_classes):
    """
    @pytest.fixture(scope="module")

    Creates indices for supplied ``model_classes``.
    Deletes these indices after the tests according to the
    ``--index-noclean`` option.

    :param model_classes: list of model classes
    :yield: None
    """

    indices = elastic_connect.create_mappings(model_classes)

    logger.info("created indices: %s", indices)

    yield

    if request.config.getoption("--index-noclean"):
        logger.warning("not cleaning indices")
        return

    for index in indices:
        elastic_connect.delete_index(index)

    logger.info("teardown %s",
                (elastic_connect.get_es().cat.indices() or "No indices",))


@pytest.fixture(scope="module")
def second_namespace():
    """
    @pytest.fixture(scope="module")

    Returns the Namespace object of a second elasticsearch cluster. This
    cluster must be available on localhost:18400. You may use this
    fixture as a base for your own connections to multiple elasticsearch
    clusters.
    :return: Namespace
    """

    if 'second' in elastic_connect._namespaces:
        return elastic_connect._namespaces['second']

    second = elastic_connect.Namespace(
        name='second',
        es_conf=[{'host': 'localhost', 'port': 18400}])
    elastic_connect.namespace.register_namespace(second)

    return second
