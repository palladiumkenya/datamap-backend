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
    is_active = columns.Boolean(required=True, default=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class IndicatorVariables(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'indicator_variables'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    tablename = columns.Text(required=True)
    columnname = columns.Text(required=True)
    datatype = columns.Text(required=True)
    indicator = columns.Text(required=True)
    baseVariableMappedTo = columns.Text(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())

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