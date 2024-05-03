import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from datetime import datetime


class AccessCredentials(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'access_credentials'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    conn_string = columns.Text(required=True)
    is_active = columns.Boolean(required=True, default=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class User(Model):
    __keyspace__ = 'datamap'
    __table_name__ = 'users'

    id = columns.UUID(primary_key=True, default=uuid.uuid1)
    username = columns.Text(required=True, index=True)
    password = columns.Text(required=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())

    def save(self):
        super().save()
