import os
import sys
import pytest
import logging

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ROOT_TEST_DIR = os.path.abspath(os.path.join(ROOT_DIR, "tests"))

sys.path.insert(0, ROOT_DIR)
print(sys.path)

import elastic_connect
from elastic_connect.testing import *
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def fix_template(request):
    es = elastic_connect.get_es()
    template = {
                "template": "*",
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                }
    es.indices.put_template(name="all", body=template, order=1)
    logger.info("templates %s", es.indices.get_template(name='*'))
