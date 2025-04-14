import re

from sqlalchemy import text

from database.database import execute_raw_data_query, execute_query
from models.models import DataDictionaries, DataDictionaryTerms, DQAReport


def dqa_check(baselookup: str, db):
    dictionary = db.query(DataDictionaries).filter(DataDictionaries.name == baselookup).first()
    terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary == baselookup).all()
    query = text(f"""
        SELECT * From {baselookup}
    """)
    data = execute_raw_data_query(query)
    count_data = len(data)
    total_failed = 0
    processed_records = []
    for row in data:
        failed_expected = []
        failed_null_check = False
        for term in terms:
            term_value = row[term.term.lower()]
            is_valid = re.match(term.expected_values, str(term_value), flags=re.IGNORECASE)
            if term.is_required and term_value is None:
                failed_null_check = True
            if not is_valid:
                failed_expected.append({
                    'term': term.term.lower(),
                    'expected': term.expected_values,
                    'actual': term_value
                })
        processed_records.append({'failed_dqa': failed_expected, 'row': row})
        if len(failed_expected) > 0:
            update_query = text(f"""
                UPDATE {baselookup} 
                SET data_valid = {False} and invalid_data_reasons = {failed_expected} 
                WHERE {baselookup}_id = {row[baselookup + '_id']}
            """)
            execute_query(update_query)
            total_failed += 1
        if failed_null_check:
            update_query = text(f"""
                UPDATE {baselookup} 
                SET data_required_check_fail = {True}
                WHERE {baselookup}_id = {row[baselookup + '_id']}
            """)
            execute_query(update_query)
    report = DQAReport(
        base_table_name=baselookup,
        valid_rows=count_data - total_failed,
        total_rows=count_data,
        invalid_rows=total_failed,
        dictionary_version=dictionary.version_number
    )
    db.add(report)
    db.commit()

    return {"data": processed_records, "count": count_data}
