from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import func
# from lineagex.lineagex import lineagex
from sqlalchemy.orm import Session, aliased

from database.database import get_db
from models.models import DQAReport, Transformations
from serializers.transformations_serializer import transformation_list_serializer
from utils.dqa_check import dqa_check

router = APIRouter()


@router.get("/dqa/{baselookup}")
def transformation_api(baselookup: str, db: Session = Depends(get_db)):
    if not baselookup:
        raise HTTPException(status_code=400, detail="Baselookup not provided")
    return dqa_check(baselookup, db)


# Transformations removed for this version
# class TransformationData(BaseModel):
#     invalid_value: str = Field(..., description="current value of the field")
#     valid_value: str = Field(..., description="new value of the field")
#     base_table: str = Field(..., description="table to implement change")
#     column: str = Field(..., description="table to implement change")
#
#
# @router.post("/transform")
# def transform_api(transform: TransformationData, db: Session = Depends(get_db)):
#     try:
#         dictionary = db.query(DataDictionaries).filter(DataDictionaries.name==transform.base_table).first()
#         dictionary_term = db.query(DataDictionaryTerms).filter(
#             DataDictionaryTerms.dictionary_id==dictionary.id,
#             DataDictionaryTerms.term==transform.column
#         ).first()
#
#         if dictionary_term:
#             is_valid = re.match(dictionary_term.expected_values, transform.valid_value)
#             if not is_valid:
#                 raise HTTPException()
#
#         update_query = f"""
#             UPDATE {transform.base_table}
#             SET {transform.column} = {transform.valid_value}
#             WHERE {transform.column} = {transform.invalid_value};
#         """
#
#         execute_query(update_query)
#         return {"message": "success"}
#
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e)) from e
#

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
        report = db.query(DQAReport).order_by(DQAReport.created_at.desc()).all()

        latest_per_base_subquery = db.query(
            DQAReport.base_table_name,
            func.max(DQAReport.created_at).label('latest_date')
        ).group_by(DQAReport.base_table_name).subquery()
        dqa_alias = aliased(DQAReport)
        latest_per_base_report = db.query(dqa_alias).join(
            latest_per_base_subquery,
            (dqa_alias.base_table_name == latest_per_base_subquery.c.base_table_name) & (dqa_alias.created_at == latest_per_base_subquery.c.latest_date)
        ).all()

        transformation_data = db.query(Transformations).order_by(Transformations.created_at.desc()).all()
        data = transformation_list_serializer(transformation_data)
        return {
            'status': 'success',
            'transformations': data,
            'dqa_report': report,
            'latest_per_base_report': latest_per_base_report
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
