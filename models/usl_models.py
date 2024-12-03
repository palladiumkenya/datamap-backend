import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from datetime import datetime


class DataDictionariesUSL(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'data_dictionaries_usl'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    name = columns.Text(required=True, index=True)
    version_number = columns.Integer(required=True, default=0)  # dictionary version
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


class DictionaryChangeLog(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'dictionary_change_log'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    dictionary_id = columns.UUID(required=True, index=True)
    term_id = columns.UUID(required=True)
    operation = columns.Text(required=True)  # ADD, EDIT, DELETE
    old_value = columns.Text(required=False)  # Store JSON string of old term
    new_value = columns.Text(required=False)  # Store JSON string of new term
    version_number = columns.Integer(required=True)
    changed_at = columns.DateTime(required=True, default=datetime.utcnow())

    def save(self):
        self.changed_at = datetime.utcnow()
        super().save()


class UniversalDictionaryTokens(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'universal_dictionary_tokens'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    universal_dictionary_token = columns.Text(required=True)
    secret = columns.Text(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class UniversalDictionaryFacilityPulls(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'universal_dictionary_facility_pulls'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    facility_mfl_code = columns.Text(required=True)
    date_last_updated = columns.DateTime(required=False)
    dictionary_versions = columns.Text(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()
