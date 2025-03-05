import logging
import re
import uuid

from cassandra.query import SimpleStatement

from database.database import _session as session


def sanitize_identifier(identifier: str) -> str:
    return re.sub(r'[^0-9a-zA-Z_]', '', identifier)


def create_table(data):
    sanitized_table_name = sanitize_identifier(data.name)
    columns = ', '.join([f'{sanitize_identifier(key)} TEXT' for key in data.data[0].keys()])
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {sanitized_table_name}_CSV_EXTRACT (
        generated_id_unique UUID PRIMARY KEY, -- primary key generated for this table
        {columns}
    )
    """)


def upload_data(data):
    try:
        create_table(data)
        for record in data.data:
            columns = ', '.join([sanitize_identifier(key) for key in record.keys()])
            placeholders = ', '.join(['%s'] * len(record))
            query = SimpleStatement(f"""
                INSERT INTO {sanitize_identifier(data.name)}_CSV_EXTRACT (generated_id_unique, {columns})
                VALUES (%s, {placeholders})
            """)
            values = (uuid.uuid4(), *record.values())
            session.execute(query, values)
    except Exception as e:
        logging.error("Error occurred in uploading data", exc_info=True)
