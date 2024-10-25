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
    name = columns.Text(required=True, index=True)
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
    dictionary = columns.Text(required=True, index=True)
    dictionary_id = columns.UUID(required=True, index=True)
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


class SiteConfig(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'site_configuration'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    site_name = columns.Text(required=True, index=True)
    site_id = columns.UUID(required=True, index=True)
    primary_system = columns.Text(required=True)
    other_systems = columns.Text(required=False)

    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class USLConfig(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'usl_configuration'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    usl_host = columns.Text(required=True)
    usl_key = columns.Text(required=True)

    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class SchedulesConfig(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'schedules_configuration'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    schedule_name = columns.Text(required=True)
    cron_expression = columns.Text(required=True)
    last_run = columns.DateTime(required=False)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class SchedulesLog(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'schedules_log'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    schedule_uuid = columns.UUID(required=True)
    log_type = columns.Text(required=True)
    log_message = columns.Text(required=True)
    start_time = columns.DateTime(required=False)
    end_time = columns.DateTime(required=False)

    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()
