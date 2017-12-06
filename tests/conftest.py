###########
# Imports #
#######################################################################################################################

import os
import sys
import time
import pytest

###########
# Globals #
#######################################################################################################################

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ROOT_TEST_DIR = os.path.abspath(os.path.join(ROOT_DIR, "tests"))

sys.path.insert(0, ROOT_DIR)
print(sys.path)

import elastic_connect
import elasticsearch

def pytest_addoption(parser):
	parser.addoption("--index-noclean", action="store_true", help="Don't clear the index after each test. Debugging only, may break some tests.")
	parser.addoption("--es-host", action="store", default="localhost", help="Elasticsearch hostname")
	parser.addoption("--es-port", action="store", default="9200", help="Elasticsearch port")
	#parser.addoption("--sql-host", action="store", default="db", help="SQL hostname")

@pytest.fixture(scope="module")
def fixEs(request):
	elastic_connect.connect([{"host":pytest.config.getoption("--es-host"),"port":pytest.config.getoption("--es-port")}])
	yield

@pytest.fixture(scope="module")
def fixIndex(request, model_classes):

	indices = elastic_connect.create_mappings(model_classes)

	print("** created indices:", indices)

	yield

	if pytest.config.getoption("--index-noclean"):
		print("** not cleaning")
		return

	for index in indices:
		elastic_connect.delete_index(index)

	print("\npost\n", elastic_connect.get_es().cat.indices() or "No indices")
