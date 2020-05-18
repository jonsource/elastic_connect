import elastic_connect.data_types as data_types
import logging
from datetime import datetime
from .base_model import Model, IntegrityError
from elasticsearch.exceptions import NotFoundError
from contextlib import contextmanager

logger = logging.getLogger(__name__)


NONE = 0
WITH = 1
ONLY = 2


class SoftDeleteInterface(Model):
    # must not define __slosts__ here to allow multiple inheritance
    # __slots__ = ('deleted',)

    thrash_handling = NONE

    @classmethod
    def model_mapping(cls, **args):
        mapping = super().model_mapping(**args)
        mapping.add_field('deleted', data_types.Boolean())
        return mapping

    @classmethod
    def create(cls, **kw) -> 'StampedModel':
        kw['deleted'] = False
        # logger.warn('sdm create %s' % kw)
        instance = super().create(**kw)
        return instance

    def delete(self, force=False):
        if force:
            return super().delete()

        self.deleted = True
        return self.save()

    @classmethod
    def restore(cls, id) -> 'StampedModel':
        with cls.thrashed():
            instance = cls.get(id)
        instance.deleted = False
        instance.save()
        return instance

    @classmethod
    def get(cls, id):
        result = cls.find_by(size=1, _id=id)
        if not len(result):
            raise NotFoundError()
        if len(result) > 1:
            raise IntegrityError(
                "Get returned multiple items, should return one!")
        return result[0]

    @classmethod
    def all(cls, size=100, sort=None):
        return cls.find_by(size=size, sort=sort)

    @classmethod
    def find_by(cls,
                size=100,
                sort=None,
                search_after=None,
                query=None,
                **kw):

        if query:
            # add deleted=false to query
            pass
        else:
            kw = {**kw, **cls._thrashed_kw()}
        return super().find_by(size=size, sort=sort,
                               search_after=search_after, query=query, **kw)

    @classmethod
    def _thrashed_kw(cls):
        if(cls.thrash_handling == WITH):
            return {}
        if(cls.thrash_handling == ONLY):
            return {'deleted': True}
        return {'deleted': False}

    @classmethod
    @contextmanager
    def thrashed(cls):
        return cls._thrashed(WITH)

    @classmethod
    @contextmanager
    def thrashed_only(cls):
        return cls._thrashed(ONLY)

    @classmethod
    def _thrashed(cls, handling):
        orig = cls.thrash_handling
        cls.thrash_handling = handling
        try:
            yield cls
        finally:
            cls.thrash_handling = orig


class TimeStampedInterface(Model):
    # must not define __slosts__ here to allow multiple inheritance
    # __slots__ = ('created_at', 'updated_at')

    @classmethod
    def model_mapping(cls, **args):
        """
        Creates a Mapping with given parameters.

        Automatically adds the id field as Keyword if not present
        Automatically adds created_at and updated_at as Date
        Automatically adds deleted as Boolean

        :param **args: field_name=data_type pairs describing the fields
            of this model.

        :return: Mapping
        """

        mapping = super().model_mapping(**args)
        mapping.add_field('created_at', data_types.Date())
        mapping.add_field('updated_at', data_types.Date())
        return mapping

    @classmethod
    def create(cls, **kw) -> 'StampedModel':
        now = datetime.now()
        kw['created_at'] = now
        kw['updated_at'] = now
        # logger.warn('tsm create %s' % kw)
        instance = super().create(**kw)
        return instance

    def save(self) -> 'StampedModel':
        now = datetime.now()
        self.updated_at = now
        return super().save()


class StampedModel(TimeStampedInterface, SoftDeleteInterface):
    __slots__ = ('created_at', 'updated_at', 'deleted')


class VersionedModel(Model):
    pass


class AuditedModel(Model):
    pass
