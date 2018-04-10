#######
Testing
#######

.. toctree::
   :maxdepth: 2

Usage
=====

Elastic Connect supplies a set of convinience options and fixtures for testing with pytest.
To use these features just
::

   import elastic_connect.testing

in your tests or preferably ``conftest.py``. This automatically uses some fixtures, makes available others and adds
custom options for running the tests.

Options
=======

``--index-noclean``
   Don't clear the indices. Debugging only, may break some tests.

``--es-host, default="localhost"``
   Elasticsearch hostname

``--es-port, default="9200"``
   Elasticsearch port

``--es-prefix, default="test"``
   Elasticsearch indices prefix

``--namespace, default=False``
   Also run tests for two namespaces

Autouse Fixtures
================
   .. autofunction:: elastic_connect.testing.fix_es

   .. autofunction:: elastic_connect.testing.prefix_indices

Fixtures
========
   .. autofunction:: elastic_connect.testing.fix_index

   .. autofunction:: elastic_connect.testing.second_namespace
