import uuid

from sqlalchemy import Column, Integer, text, String, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone


Base = declarative_base()
metadata = Base.metadata


class MappedVariables(Base):
    __tablename__ = 'mapped_variables'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    tablename = Column(String, nullable=False)
    columnname = Column(String, nullable=False)
    datatype = Column(String, nullable=False)
    join_by = Column(String, nullable=False)
    base_repository = Column(String, nullable=False)
    base_variable_mapped_to = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    source_system_id = Column(UUID(as_uuid=True))


class IndicatorQueries(Base):
    __tablename__ = 'indicator_queries'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    indicator = Column(String, nullable=False)
    query = Column(String, nullable=False)
    indicator_value = Column(String, nullable=False, default="0")
    indicator_date = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))

    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))


class IndicatorHistory(Base):
    __tablename__ = 'indicator_history'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    indicator = Column(String, nullable=False)
    indicator_value = Column(String, nullable=False, default="0")
    indicator_date = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))

    usl_repository_name = Column(String, nullable=False)
    source_system_id = Column(UUID(as_uuid=True))
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))


class TransmissionHistory(Base):
    __tablename__ = 'transmission_history'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    usl_repository_name = Column(String, nullable=False)
    facility = Column(String, nullable=False)
    action = Column(String, nullable=False)
    source_system_id = Column(UUID(as_uuid=True))
    source_system_name = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    started_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    ended_at = Column(DateTime)
    manifest_id = Column(UUID(as_uuid=True))


class USLDataErrorLogs(Base):
    __tablename__ = 'usl_data_error_logs'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    usl_repository_name = Column(String, nullable=False)
    base_variable = Column(String, nullable=False)
    issue = Column(String, nullable=False)
    recommended_solution = Column(String, nullable=False)
    source_system_id = Column(UUID(as_uuid=True))
    source_system_name = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    is_latest = Column(Boolean, default=False)


class DataDictionaries(Base):
    __tablename__ = 'data_dictionaries'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    datasource_id = Column(UUID(as_uuid=True))
    name = Column(String, nullable=False)
    version_number = Column(Integer, nullable=False, default=0)
    is_published = Column(Boolean, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)


class DataDictionaryTerms(Base):
    __tablename__ = 'data_dictionary_terms'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    dictionary = Column(String, nullable=False)
    dictionary_id = Column(UUID(as_uuid=True))
    term = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    is_required = Column(Boolean, default=False)
    term_description = Column(String, nullable=True)
    expected_values = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)


class SiteConfig(Base):
    __tablename__ = 'site_configuration'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    site_name = Column(String, nullable=False)
    site_code = Column(String, nullable=False)
    primary_system = Column(String, nullable=False)
    other_systems = Column(String, nullable=True)
    is_active = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)


class AccessCredentials(Base):
    __tablename__ = 'access_credentials'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    conn_string = Column(String, nullable=False)
    name = Column(String, nullable=False)
    system_id = Column(ForeignKey(SiteConfig.id), primary_key=True)
    conn_type = Column(VARCHAR(20), nullable=False, default='mysql')

    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)

    # relationships
    system = relationship(SiteConfig, foreign_keys='AccessCredentials.system_id')


class SchedulesConfig(Base):
    __tablename__ = 'schedules_configuration'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    schedule_name = Column(String, nullable=False)
    cron_expression = Column(String, nullable=False)
    last_run = Column(DateTime)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)


class SchedulesLog(Base):
    __tablename__ = 'schedules_log'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    schedule_uuid = Column(UUID(as_uuid=True))
    log_type = Column(String, nullable=False)
    log_message = Column(String, nullable=False)
    start_time = Column(DateTime)
    end_time = Column(DateTime)

    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)


class UniversalDictionaryConfig(Base):
    __tablename__ = 'universal_dictionary_config'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    universal_dictionary_url = Column(String, nullable=False)
    universal_dictionary_jwt = Column(String, nullable=False)
    universal_dictionary_update_frequency = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)


class DQAReport(Base):
    __tablename__ = 'dqa_report'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    base_table_name = Column(String, nullable=False)
    valid_rows = Column(Integer, nullable=False)
    invalid_rows = Column(Integer, nullable=False)
    null_rows = Column(Integer, nullable=False, default=0)
    dictionary_version = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))


class Transformations(Base):
    """Represents a transformation record in the datamap.

    This class is used to store information about changes made to a specific
    column in a base table, including the previous and new values, along with
    timestamps for creation and updates.

    Attributes:
        id (UUID): The unique identifier for the transformation record.
        base_table_name (Text): The name of the base table where the transformation occurs.
        base_table_column (Text): The name of the column in the base table that is transformed.
        previous_value (Text, optional): The value before the transformation.
        new_value (Text, optional): The value after the transformation.
        created_at (DateTime): The timestamp when the transformation record was created.
        updated_at (DateTime): The timestamp when the transformation record was last updated.
    
    Methods:
        save(): Updates the updated_at timestamp and saves the transformation record.
    """
    __tablename__ = 'transformations'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    base_table_name = Column(String, nullable=False)
    base_table_column = Column(String, nullable=False)
    previous_value = Column(String, nullable=True)
    new_value = Column(String, nullable=True)
    dictionary_version = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
