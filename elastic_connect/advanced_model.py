import elastic_connect
import elastic_connect.data_types as data_types
import elastic_connect.data_types.base
import logging
from .base_model import Model

logger = logging.getLogger(__name__)


class StampedModel(Model):
    __slots__ = ('created_at', 'updated_at')

    @classmethod
    def create(cls, **kw) -> 'StampedModel':
        """
        Create, save and return a model instance based on dictionary.
        Property id gets set by elasticsearch or computed depending on
        cls._compute_id()

        :param kw: keyword arguments describing the model's attributes
        :return: instance of the model with the ``id`` set
        """


        instance = super().create(**kw)
        instance.created_at =
        instance.updated_at =
        return instance


class VersionedModel(Model):
    pass


class AuditedModel(Model):
    pass
