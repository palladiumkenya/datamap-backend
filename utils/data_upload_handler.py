import logging
import re
import uuid

from sqlalchemy import text

from database.database import execute_query


def sanitize_identifier(identifier: str) -> str:
    return re.sub(r'[^0-9a-zA-Z_]', '', identifier)


def create_table(data):
    sanitized_table_name = sanitize_identifier(data.name)
    columns = ', '.join([f'{sanitize_identifier(key)} TEXT' for key in data.data[0].keys()])
    execute_query(text(f"""
    CREATE TABLE IF NOT EXISTS {sanitized_table_name}_{data.upload.upper()}_EXTRACT (
        generated_id_unique UUID PRIMARY KEY, -- primary key generated for this table
        {columns}
    )
    """))


def upload_data(data):
    try:
        create_table(data)
        for record in data.data:
            columns = ', '.join([sanitize_identifier(key) for key in record.keys()])
            placeholders = ', '.join(['%s'] * len(record))
            values = (uuid.uuid4(), *(str(value) for value in record.values()))

            query = text(f"""
                INSERT INTO {sanitize_identifier(data.name)}_{data.upload.upper()}_EXTRACT (generated_id_unique, {columns})
                VALUES {values}
            """)
            execute_query(query)
    except Exception as e:
        logging.error("Error occurred in uploading data", exc_info=True)
