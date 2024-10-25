import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from datetime import datetime


class DataDictionariesUSL(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'data_dictionaries_usl'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    name = columns.Text(required=True, index=True)
    version_number = columns.Integer(primary_key=True)  # dictionary version
    parent_dictionary_id = columns.UUID(required=False, index=True)  # Link to previous version
    is_published = columns.Boolean(default=False)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class DataDictionaryTermsUSL(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'data_dictionary_terms_usl'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    dictionary = columns.Text(required=True, index=True)
    dictionary_id = columns.Text(required=True, index=True)
    term = columns.Text(required=True, index=True)
    data_type = columns.Text(required=True)
    is_required = columns.Boolean(default=False)
    term_description = columns.Text(required=False)
    expected_values = columns.Text(required=False)
    is_active = columns.Boolean(required=True, default=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()

