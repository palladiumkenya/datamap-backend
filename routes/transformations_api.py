import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from database.database import execute_query
from models.models import DataDictionaries, DataDictionaryTerms, DQAReport, Transformations
from serializers.transformations_serializer import transformation_list_serializer

router = APIRouter()


@router.get("/dqa/{baselookup}")
def transformation_api(baselookup: str):
    if not baselookup:
        raise HTTPException()
    dictionary = DataDictionaries.objects().filter(name=baselookup).first()
    terms = DataDictionaryTerms.objects().filter(dictionary=baselookup).allow_filtering().all()
    query = f"""
        SELECT * From {baselookup}
    """
    data = execute_query(query)
    count_data = len(data)
    failed_expected = []
    for row in data:
        for term in terms:
            is_valid = re.match(term.expected_values, str(row[term.term.lower()])) # flags=re.IGNORECASE
            if not is_valid:
                failed_expected.append({
                    'term': term.term.lower(),
                    'expected': term.expected_values
                })
    report = DQAReport(
        base_tale_name=baselookup,
        valid_rows=count_data - len(failed_expected),
        invalid_rows=len(failed_expected),
        dictionary_version=dictionary.version_number
    )
    report.save()

    return {"message": data, "count": count_data}


class TransformationData(BaseModel):
    invalid_value: str = Field(..., description="current value of the field")
    valid_value: str = Field(..., description="new value of the field")
    base_table: str = Field(..., description="table to implement change")
    column: str = Field(..., description="table to implement change")


@router.post("/transform")
def transform_api(transform: TransformationData):
    try:
        dictionary = DataDictionaries.objects().filter(name=transform.base_table).first()
        dictionary_term = DataDictionaryTerms.objects().filter(
            dictionary_id=dictionary.id,
            term=transform.column
        ).first()

        if dictionary_term:
            is_valid = re.match(dictionary_term.expected_values, transform.valid_value)
            if not is_valid:
                raise HTTPException()
        id_queries = f"""
            SELECT 
                {transform.base_table}_id as id 
            FROM {transform.base_table} 
            WHERE {transform.column} = {transform.invalid_value} ALLOW FILTERING;
        """
        ids = eval(id_queries)
        print(ids)
        ids = [x.id for x in ids]
        update_query = f"""
            UPDATE {transform.base_table} 
            SET {transform.column} = {transform.valid_value} 
            WHERE {transform.base_table}_id IN {ids}
        """

        execute_query(update_query)
        return

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
def transformation_api_report():
    try:
        transformation_data = Transformations.objects().all()
        data = transformation_list_serializer(transformation_data)
        return {
            'status': 'success',
            'data': data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
