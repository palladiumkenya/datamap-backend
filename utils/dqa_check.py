import re

from sqlalchemy import text

from database.database import execute_raw_data_query, execute_query
from models.models import DataDictionaries, DataDictionaryTerms, DQAReport


def dqa_check(baselookup: str, db):
    dictionary = db.query(DataDictionaries).filter(DataDictionaries.name == baselookup).first()
    terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary == baselookup).all()
    query = text("""
        SELECT * FROM :table_name
    """).bindparams(table_name=baselookup)
    data = execute_raw_data_query(query)
    count_data = len(data)
    total_failed = 0
    total_failed_null_check = 0
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
            update_query = text("""
                UPDATE :table_name 
                SET data_valid = :data_valid, invalid_data_reasons = :invalid_data_reasons 
                WHERE :table_name_id = :row_id
            """).bindparams(table_name=baselookup, data_valid=False, invalid_data_reasons=failed_expected, table_name_id=baselookup + '_id', row_id=row[baselookup + '_id'])
            execute_query(update_query)
            total_failed += 1
        if failed_null_check:
            update_query = text("""
                UPDATE :table_name 
                SET data_required_check_fail = :data_required_check_fail
                WHERE :table_name_id = :row_id
            """).bindparams(table_name=baselookup, data_required_check_fail=True, table_name_id=baselookup + '_id', row_id=row[baselookup + '_id'])
            execute_query(update_query)
            total_failed_null_check += 1
    report = DQAReport(
        base_table_name=baselookup,
        valid_rows=count_data - total_failed,
        total_rows=count_data,
        invalid_rows=total_failed,
        dictionary_version=dictionary.version_number,
        null_rows=total_failed_null_check
    )
    db.add(report)
    db.commit()

    return {"data": processed_records, "count": count_data}
