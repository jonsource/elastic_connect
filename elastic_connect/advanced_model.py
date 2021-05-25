import elastic_connect.data_types as data_types
import logging
from datetime import datetime
from .base_model import Mapping, Model, IntegrityError
from elasticsearch.exceptions import NotFoundError
from contextlib import contextmanager
import elasticsearch.exceptions

logger = logging.getLogger(__name__)

class MissingPreviousVersionError(Exception):
    pass

NONE = 0
WITH = 1
ONLY = 2

# TODO: sanitize inputs - cannot overwrite deleted manualy - but how?
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
        cls.refresh()
        return instance

    def delete(self, force=False):
        logger.debug("DELETING: %r force=%s" % (self, force))
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
            raise NotFoundError
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
            apends = {
                ONLY: " AND deleted: true",
                NONE: " AND deleted: false",
                WITH: "",
            }
            query = query + apends.get(cls.thrash_handling)
            logger.debug("query: %s" % query)

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
    def _create(cls, model) -> 'StampedModel':
        now = datetime.now()
        model.created_at = now
        model.updated_at = now
        # logger.warn('tsm create %s' % kw)
        model = super()._create(model)
        return model

    def save(self) -> 'StampedModel':
        now = datetime.now()
        self.updated_at = now
        return super().save()


class VersionedInterface(Model):
    _version_class = None

    @classmethod
    def get_version_class(cls):
        if(cls._version_class):
            return cls._version_class

        slots = set(list(cls.__slots__) + ['_document_id', '_document_version', '_type'])

        mapping = Mapping()
        for key, val in cls._mapping.items():
            mapping.add_field(key, val.__class__(**val.class_params()))
        mapping.add_field('_document_id', data_types.Keyword())
        mapping.add_field('_document_version', data_types.Keyword())
        mapping.add_field('_status', data_types.Keyword())

        meta = {'_doc_type': cls._meta['_doc_type'] + '_version'}

        # https://stackoverflow.com/questions/1401661/list-all-base-classes-in-a-hierarchy-of-given-class
        # https://www.python-course.eu/python3_classes_and_type.php

        # class VersionClass(Model):
        #     _es_namespace = cls._es_namespace
        #     _es_connection = None
        #     __slots__ = set(list(cls.__slots__) + ['_document_id', '_document_version', '_type'])

        #     _meta = meta

        #     _mapping = mapping

        #     def to_version_entry(self):
        #         self._type = "entry"

        VersionClass = type(cls.__name__ + '_version',
                        (VersionInterface, Model),
                        {
                            '__slots__': slots,
                            '_mapping': mapping,
                            '_meta': meta,
                            '_es_namespace': cls._es_namespace,
                            '_es_connection': None,
                        })

        cls._version_class = VersionClass
        # cls._es_namespace.create_mappings(model_classes=[VersionClass])
        return cls._version_class

    def save(self) -> 'VersionedModel':
        logger.debug('\nsaving' + self.__class__.__name__ + " " + self.id)

        try:
            previous = self.get(self.id)
        except NotFoundError:
            raise MissingPreviousVersionError()
        logger.debug('\ngot previous %r' % previous)
        if previous._version != self._version:
            raise elasticsearch.exceptions.ConflictError('Underlying document changed version.')
        proposal = previous.to_version_proposal()
        logger.debug('\nproposal %r' % proposal)
        proposal.save()
        try:
            main_entry = super().save()
            update_time = main_entry.updated_at
        except elasticsearch.exceptions.ConflictError as e:
            proposal.delete()
            raise e
        varsion = proposal.to_version_entry(updated_at=update_time).save()
        return main_entry

    def to_version_proposal(self):
        cls = self.get_version_class()
        proposal = cls()
        for attr, val in self.__dict__.items():
            proposal.__dict__[attr] = val
        for attr in self.__slots__:
            if attr in ['id', '_version', 'created_at']:
                continue
            setattr(proposal, attr, getattr(self, attr))
        proposal._status = 'proposal'
        proposal._version = 0
        proposal._document_version = self._version
        proposal._document_id = self.id
        proposal.created_at = self.created_at

        return proposal

    @classmethod
    def get_document_version(cls, id, version):
        result = cls.get_version_class().find_by(_document_id=id, _document_version=version, _status='entry')
        if len(result) > 1:
            raise IntegrityError("Two versions found.")
        if not result:
            return None
        result[0]._document_id = id
        return result[0]

    def get_version(self, version):
        return self.get_document_version(self.id, version)

    @classmethod
    def refresh(cls):
        cls.get_version_class().refresh()
        return super().refresh()


class StampedModel(TimeStampedInterface, SoftDeleteInterface):
    __slots__ = ('created_at', 'updated_at', 'deleted')

class VersionInterface():
    def to_version_entry(self, updated_at):
        self._status = "entry"
        self.updated_at = updated_at
        return self

    def get_id(self):
        return self._document_id

class VersionedModel(TimeStampedInterface, SoftDeleteInterface, VersionedInterface):
    __slots__ = ('created_at', 'updated_at', 'deleted', '_document_id')
    _meta = {
        '_doc_type': 'versioned_model',
        # not needed - if missing, treated as false
        # '_load_version': False,
        # '_check_version': False,
        '_post_save_refresh': True
    }

class AuditedModel(Model):
    pass
