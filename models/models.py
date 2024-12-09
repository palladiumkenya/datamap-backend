import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from sqlalchemy.orm import relationship
from datetime import datetime


class AccessCredentials(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'access_credentials'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    conn_string = columns.Text(required=True)
    name = columns.Text(required=True)

    is_active = columns.Boolean(required=True, default=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()

    def update(self):
        self.updated_at = datetime.utcnow()
        super().save()


class MappedVariables(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'mapped_variables'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    tablename = columns.Text(required=True,index=True)
    columnname = columns.Text(required=True,index=True)
    datatype = columns.Text(required=True,index=True)
    join_by = columns.Text(required=True,index=True)
    base_repository = columns.Text(required=True,index=True)
    base_variable_mapped_to = columns.Text(required=True,index=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow(),index=True)
    updated_at = columns.DateTime(required=True, default=datetime.utcnow(),index=True)
    source_system_id = columns.UUID(default=uuid.uuid1)


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

class IndicatorHistory(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'indicator_history'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    indicator = columns.Text(required=True)
    indicator_value = columns.Text(required=True, default="0")
    indicator_date = columns.DateTime(required=True, default=datetime.utcnow())

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    usl_repository_name = columns.Text(required=True)
    source_system_id = columns.UUID(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    def save(self):
        self.created_at = datetime.utcnow()
        super().save()

class TransmissionHistory(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'transmission_history'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    usl_repository_name = columns.Text(required=True)
    facility = columns.Text(required=True)
    action = columns.Text(required=True)
    source_system_id = columns.UUID(required=True)
    source_system_name = columns.Text(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    started_at = columns.DateTime(required=True, default=datetime.utcnow())
    ended_at = columns.DateTime(required=False)
    manifest_id = columns.UUID(default=uuid.uuid1)

    def save(self):
        self.started_at = datetime.utcnow()
        super().save()


class USLDataErrorLogs(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'usl_data_error_logs'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    usl_repository_name = columns.Text(required=True)
    base_variable = columns.Text(required=True)
    issue = columns.Text(required=True)
    recommended_solution = columns.Text(required=True)
    source_system_id = columns.UUID(required=True)
    source_system_name = columns.Text(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    is_latest = columns.Boolean(default=False)

    def save(self):
        self.started_at = datetime.utcnow()
        super().save()


class DataDictionaries(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'data_dictionaries'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    datasource_id = columns.UUID(required=True)
    name = columns.Text(required=True, index=True)
    version_number = columns.Integer(required=True, default=0)
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
    site_code = columns.Text(required=True, index=True)
    site_id = columns.UUID(required=False, index=True)
    primary_system = columns.Text(required=True)
    other_systems = columns.Text(required=False)
    is_active = columns.Boolean(required=True, default=False)

    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class USLConfig(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'universal_dictionary_config'

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


class UniversalDictionaryConfig(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'universal_dictionary_config'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    universal_dictionary_url = columns.Text(required=True)
    universal_dictionary_jwt = columns.Text(required=True)
    universal_dictionary_update_frequency = columns.Text(required=False)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())
    deleted_at = columns.DateTime(required=False)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()

