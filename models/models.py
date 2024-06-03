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
    is_published = columns.Boolean(default=False)
    is_active = columns.Boolean(required=True, default=True)
    created_at = columns.DateTime(required=True, default=datetime.utcnow())
    updated_at = columns.DateTime(required=True, default=datetime.utcnow())

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()
