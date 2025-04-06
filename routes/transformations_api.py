import re

import exrex
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from database.database import execute_query, get_db, execute_data_query, execute_raw_data_query
from models.models import DataDictionaries, DataDictionaryTerms, DQAReport, Transformations
from serializers.transformations_serializer import transformation_list_serializer

router = APIRouter()


@router.get("/dqa/{baselookup}")
def transformation_api(baselookup: str, db: Session = Depends(get_db)):
    if not baselookup:
        raise HTTPException()
    dictionary = db.query(DataDictionaries).filter(DataDictionaries.name==baselookup).first()
    terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary==baselookup).all()
    query = text(f"""
        SELECT * From {baselookup}
    """)
    data = execute_raw_data_query(query)
    count_data = len(data)
    failed_expected = []
    for row in data:
        for term in terms:
            term_value = row[term.term.lower()]
            is_valid = re.match(term.expected_values, str(term_value), flags=re.IGNORECASE)
            if not is_valid:
                failed_expected.append({
                    'term': term.term.lower(),
                    'expected': term.expected_values,
                    'actual': term_value,
                    'example': exrex.getone(term.expected_values)
                })
    report = DQAReport(
        base_table_name=baselookup,
        valid_rows=count_data - len(failed_expected),
        invalid_rows=len(failed_expected),
        dictionary_version=dictionary.version_number
    )
    db.add(report)
    db.commit()

    return {"message": data, "count": count_data, "failed": failed_expected}


class TransformationData(BaseModel):
    invalid_value: str = Field(..., description="current value of the field")
    valid_value: str = Field(..., description="new value of the field")
    base_table: str = Field(..., description="table to implement change")
    column: str = Field(..., description="table to implement change")


@router.post("/transform")
def transform_api(transform: TransformationData, db: Session = Depends(get_db)):
    try:
        dictionary = db.query(DataDictionaries).filter(DataDictionaries.name==transform.base_table).first()
        dictionary_term = db.query(DataDictionaryTerms).filter(
            DataDictionaryTerms.dictionary_id==dictionary.id,
            DataDictionaryTerms.term==transform.column
        ).first()

        if dictionary_term:
            is_valid = re.match(dictionary_term.expected_values, transform.valid_value)
            if not is_valid:
                raise HTTPException()

        update_query = f"""
            UPDATE {transform.base_table} 
            SET {transform.column} = {transform.valid_value} 
            WHERE {transform.column} = {transform.invalid_value};
        """

        execute_query(update_query)
        return {"message": "success"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


# @router.get("/suggested-transformations")
# def transformation_suggestions_api():
#     return {"message": "Hello World"}
#
#
# class SuggestedTransformationData(BaseModel):
#     invalid_value: str = Field(..., description="current value of the field")
#     valid_value: str = Field(..., description="new value of the field")
#     base_table: str = Field(..., description="table to implement change")
#     column: str = Field(..., description="table to implement change")
#
#
# @router.post("/transformation/report")
# def transformation_api_report(data: SuggestedTransformationData):
#     dictionary = DataDictionaries.objects().filter(name=data.base_table).first()
#     dictionary_term = DataDictionaryTerms.objects().filter(
#         dictionary_id=dictionary.id,
#         term=data.column
#     ).first()
#
#     return {"message": "Hello World"}


@router.get("/transformation/report")
def transformation_api_report(db: Session = Depends(get_db)):
    try:
        transformation_data = db.query(Transformations).all()
        data = transformation_list_serializer(transformation_data)
        return {
            'status': 'success',
            'data': data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
