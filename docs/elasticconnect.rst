#################
API documentation
#################

.. toctree::
   :maxdepth: 2

*******
Connect
*******

Convenience functions
=====================

   .. autofunction:: elastic_connect.connect.create_mappings

   .. autofunction:: elastic_connect.connect.connect

   .. autofunction:: elastic_connect.connect.delete_indices

Classes
=======

   .. autoclass:: elastic_connect.connect.DocTypeConnection
      :members:
      :special-members: __getattr__

   .. autoclass:: elastic_connect.connect.Result
      :members:

*********
BaseModel
*********

   .. automodule:: elastic_connect.base_model
      :members:
      :private-members:

*********
DataTypes
*********

Base
====

   .. automodule:: elastic_connect.data_types.base
      :members:

Join
====

   .. automodule:: elastic_connect.data_types.join
      :members:

**********
Namespaces
**********

   .. automodule:: elastic_connect.namespace
      :members: register_namespace, Namespace, NamespaceConnectionError, NamespaceAlreadyExistsError

      .. autodata:: _global_prefix
         :annotation:

      .. autodata:: _namespaces
         :annotation:




