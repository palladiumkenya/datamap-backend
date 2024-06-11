import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from datetime import datetime


class AccessCredentials(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'access_credentials'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    conn_string = columns.Text(required=True)
    name = columns.Text(required=True)
    system = columns.Text(required=False)
    system_version = columns.Text(required=False)
    is_active = columns.Boolean(required=True, default=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class IndicatorVariables(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'indicator_variables'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    tablename = columns.Text(required=True,index=True)
    columnname = columns.Text(required=True,index=True)
    datatype = columns.Text(required=True,index=True)
    # indicator = columns.Text(required=True,index=True)
    base_repository = columns.Text(required=True,index=True)
    base_variable_mapped_to = columns.Text(required=True,index=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow(),index=True)
    updated_at = columns.DateTime(required=True, default=datetime.utcnow(),index=True)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class IndicatorQueries(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'indicator_queries'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    indicator = columns.Text(required=True)
    query = columns.Text(required=True)
    indicator_value = columns.Text(required=True, default="0")
    indicator_date = columns.DateTime(required=True, default=datetime.utcnow())

    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class DataDictionaries(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'data_dictionaries'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    datasource_id = columns.UUID(required=True)
    name = columns.Text(required=True)
    is_published = columns.Boolean(default=False)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class DataDictionaryTerms(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'data_dictionary_terms'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    dictionary_id = columns.UUID(required=True, index=True)
    term = columns.Text(required=True)
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
