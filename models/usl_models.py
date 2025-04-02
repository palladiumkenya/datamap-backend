import uuid

from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime, Integer, Boolean
from sqlalchemy.dialects.postgresql import UUID, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class DataDictionariesUSL(Base):
    __tablename__ = 'data_dictionaries_usl'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    name = Column(String, nullable=False)
    version_number = Column(Integer, nullable=False, default=0)  # dictionary version
    is_published = Column(Boolean, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)

    def save(self):
        self.updated_at = datetime.now(timezone.utc)
        super().save()


class DataDictionaryTermsUSL(Base):
    __tablename__ = 'data_dictionary_terms_usl'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    dictionary = Column(String, nullable=False)
    dictionary_id = Column(String, nullable=False)
    term = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    is_required = Column(Boolean, default=False)
    term_description = Column(String, nullable=True)
    expected_values = Column(String, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)

    def save(self):
        self.updated_at = datetime.now(timezone.utc)
        super().save()


class DictionaryChangeLog(Base):
    __tablename__ = 'dictionary_change_log'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    dictionary_id = Column(UUID(as_uuid=True))
    term_id = Column(UUID(as_uuid=True))
    operation = Column(String, nullable=False)  # ADD, EDIT, DELETE
    old_value = Column(String, nullable=True)  # Store JSON string of old term
    new_value = Column(String, nullable=True)  # Store JSON string of new term
    version_number = Column(Integer, nullable=False)
    changed_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))

    def save(self):
        self.changed_at = datetime.now(timezone.utc)
        super().save()


class UniversalDictionaryTokens(Base):
    __tablename__ = 'universal_dictionary_tokens'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    universal_dictionary_token = Column(String, nullable=False)
    secret = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)

    def save(self):
        self.updated_at = datetime.now(timezone.utc)
        super().save()


class UniversalDictionaryFacilityPulls(Base):
    __tablename__ = 'universal_dictionary_facility_pulls'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    facility_mfl_code = Column(String, nullable=False)
    date_last_updated = Column(DateTime)
    dictionary_versions = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    deleted_at = Column(DateTime)

    def save(self):
        self.updated_at = datetime.now(timezone.utc)
        super().save()
